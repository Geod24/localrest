/**
 * This is a low-level messaging API upon which more structured or restrictive
 * APIs may be built.  The general idea is that every messageable entity is
 * represented by a common handle type called a Tid, which allows messages to
 * be sent to logical threads that are executing in both the current process
 * and in external processes using the same interface.  This is an important
 * aspect of scalability because it allows the components of a program to be
 * spread across available resources with few to no changes to the actual
 * implementation.
 *
 * A logical thread is an execution context that has its own stack and which
 * runs asynchronously to other logical threads.  These may be preemptively
 * scheduled kernel threads, fibers (cooperative user-space threads), or some
 * other concept with similar behavior.
 *
 * The type of concurrency used when logical threads are created is determined
 * by the Scheduler selected at initialization time.  The default behavior is
 * currently to create a new kernel thread per call to spawn, but other
 * schedulers are available that multiplex fibers across the main thread or
 * use some combination of the two approaches.
 *
 * Copyright: Copyright Sean Kelly 2009 - 2014.
 * License:   <a href="http://www.boost.org/LICENSE_1_0.txt">Boost License 1.0</a>.
 * Authors:   Sean Kelly, Alex Rønne Petersen, Martin Nowak
 * Source:    $(PHOBOSSRC std/concurrency.d)
 */
/*          Copyright Sean Kelly 2009 - 2014.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          http://www.boost.org/LICENSE_1_0.txt)
 */
module geod24.concurrency;

import std.container;
import std.datetime;
import std.range.primitives;
import std.range.interfaces : InputRange;
import std.traits;
public import std.variant;

import core.atomic;
import core.sync.condition;
import core.sync.mutex;
import core.thread;

private
{
    bool hasLocalAliasing(Types...)()
    {
        // Works around "statement is not reachable"
        bool doesIt = false;
        static foreach (T; Types)
        {
            static if (is(T == Tid))
            { /* Allowed */ }
            else static if (is(T == struct))
                doesIt |= hasLocalAliasing!(typeof(T.tupleof));
            else
                doesIt |= std.traits.hasUnsharedAliasing!(T);
        }
        return doesIt;
    }

    @safe unittest
    {
        static struct Container { Tid t; }
        static assert(!hasLocalAliasing!(Tid, Container, int));
    }
}

enum MsgType
{
    standard,
    priority,
    linkDead,
    shutdown,
}

struct Message
{
    MsgType type;
    Variant data;

    this(T...)(MsgType t, T vals) if (T.length > 0)
    {
        static if (T.length == 1)
        {
            type = t;
            data = vals[0];
        }
        else
        {
            import std.typecons : Tuple;

            type = t;
            data = Tuple!(T)(vals);
        }
    }

    @property auto convertsTo(T...)()
    {
        static if (T.length == 1)
        {
            return is(T[0] == Variant) || data.convertsTo!(T);
        }
        else
        {
            import std.typecons : Tuple;
            return data.convertsTo!(Tuple!(T));
        }
    }

    @property auto get(T...)()
    {
        static if (T.length == 1)
        {
            static if (is(T[0] == Variant))
                return data;
            else
                return data.get!(T);
        }
        else
        {
            import std.typecons : Tuple;
            return data.get!(Tuple!(T));
        }
    }

    auto map(Op)(Op op)
    {
        alias Args = Parameters!(Op);

        static if (Args.length == 1)
        {
            static if (is(Args[0] == Variant))
                return op(data);
            else
                return op(data.get!(Args));
        }
        else
        {
            import std.typecons : Tuple;
            return op(data.get!(Tuple!(Args)).expand);
        }
    }
}

void checkops(T...)(T ops)
{
    foreach (i, t1; T)
    {
        static assert(isFunctionPointer!t1 || isDelegate!t1);
        alias a1 = Parameters!(t1);
        alias r1 = ReturnType!(t1);

        static if (i < T.length - 1 && is(r1 == void))
        {
            static assert(a1.length != 1 || !is(a1[0] == Variant),
                            "function with arguments " ~ a1.stringof ~
                            " occludes successive function");

            foreach (t2; T[i + 1 .. $])
            {
                static assert(isFunctionPointer!t2 || isDelegate!t2);
                alias a2 = Parameters!(t2);

                static assert(!is(a1 == a2),
                    "function with arguments " ~ a1.stringof ~ " occludes successive function");
            }
        }
    }
}

@property ref ThreadInfo thisInfo() nothrow
{
    return ThreadInfo.thisInfo;
}
static ~this()
{
    thisInfo.cleanup();
}

// Exceptions

/**
 * Thrown on calls to `receiveOnly` if a message other than the type
 * the receiving thread expected is sent.
 */
class MessageMismatch : Exception
{
    ///
    this(string msg = "Unexpected message type") @safe pure nothrow @nogc
    {
        super(msg);
    }
}

/**
 * Thrown on calls to `receive` if the thread that spawned the receiving
 * thread has terminated and no more messages exist.
 */
class OwnerTerminated : Exception
{
    ///
    this(Tid t, string msg = "Owner terminated") @safe pure nothrow @nogc
    {
        super(msg);
        tid = t;
    }

    Tid tid;
}

/**
 * Thrown if a linked thread has terminated.
 */
class LinkTerminated : Exception
{
    ///
    this(Tid t, string msg = "Link terminated") @safe pure nothrow @nogc
    {
        super(msg);
        tid = t;
    }

    Tid tid;
}

/// Ask the node to shut down
class Shutdown : Exception
{
    ///
    this(Tid t, string msg = "Shut down") @safe pure nothrow @nogc
    {
        super(msg);
        tid = t;
    }

    Tid tid;
}

/// Data sent by the caller
struct Request
{
    /// Tid of the sender thread (cannot be JSON serialized)
    Tid sender;

    /// Method to call
    string method;

    /// Arguments to the method, JSON formatted
    string args;

    ///
    SysTime request_time;

    ///
    Duration timeout;
}

/// Status of a request
enum Status
{
    /// Request failed
    Failed,

    /// Request timed-out
    Timeout,

    /// Request succeeded
    Success
}

/// Data sent by the callee back to the caller
struct Response
{
    /// Final status of a request (failed, timeout, success, etc)
    Status status;

    /// If `status == Status.Success`, the JSON-serialized return value.
    /// Otherwise, it contains `Exception.toString()`.
    string data;
}

/**
 * Thrown on mailbox crowding if the mailbox is configured with
 * `OnCrowding.throwException`.
 */
class MailboxFull : Exception
{
    ///
    this(Tid t, string msg = "Mailbox full") @safe pure nothrow @nogc
    {
        super(msg);
        tid = t;
    }

    Tid tid;
}

/**
 * Thrown when a Tid is missing, e.g. when `ownerTid` doesn't
 * find an owner thread.
 */
class TidMissingException : Exception
{
    import std.exception : basicExceptionCtors;
    ///
    mixin basicExceptionCtors;
}


// Thread ID


/**
 * An opaque type used to represent a logical thread.
 */
struct Tid
{
private:
    this(MessageBox m) @safe pure nothrow @nogc
    {
        mbox = m;
    }

    MessageBox mbox;

public:

    /**
     * Generate a convenient string for identifying this Tid.  This i
     
     
     s only
     * useful to see if Tid's that are currently executing are the same or
     * different, e.g. for logging and debugging.  It is potentially possible
     * that a Tid executed in the future will have the same toString() output
     * as another Tid that has already terminated.
     */
    void toString(scope void delegate(const(char)[]) sink)
    {
        import std.format : formattedWrite;
        formattedWrite(sink, "Tid(%x)", cast(void*) mbox);
    }

}

@system unittest
{
    // text!Tid is @system
    import std.conv : text;
    Tid tid;
    assert(text(tid) == "Tid(0)");
    auto tid2 = thisTid;
    assert(text(tid2) != "Tid(0)");
    auto tid3 = tid2;
    assert(text(tid2) == text(tid3));
}

/**
 * Returns: The $(LREF Tid) of the caller's thread.
 */
@property Tid thisTid() @safe
{
    // TODO: remove when concurrency is safe
    static auto trus() @trusted
    {
        if (thisInfo.ident != Tid.init)
            return thisInfo.ident;
        thisInfo.ident = Tid(new MessageBox);
        return thisInfo.ident;
    }

    return trus();
}

/**
 * Return the Tid of the thread which spawned the caller's thread.
 *
 * Throws: A `TidMissingException` exception if
 * there is no owner thread.
 */
@property Tid ownerTid()
{
    import std.exception : enforce;

    enforce!TidMissingException(thisInfo.owner.mbox !is null, "Error: Thread has no owner thread.");
    return thisInfo.owner;
}

// Thread Creation
private template isSpawnable(F, T...)
{
    template isParamsImplicitlyConvertible(F1, F2, int i = 0)
    {
        alias param1 = Parameters!F1;
        alias param2 = Parameters!F2;
        static if (param1.length != param2.length)
            enum isParamsImplicitlyConvertible = false;
        else static if (param1.length == i)
            enum isParamsImplicitlyConvertible = true;
        else static if (isImplicitlyConvertible!(param2[i], param1[i]))
            enum isParamsImplicitlyConvertible = isParamsImplicitlyConvertible!(F1,
                    F2, i + 1);
        else
            enum isParamsImplicitlyConvertible = false;
    }

    enum isSpawnable = isCallable!F && is(ReturnType!F == void)
            && isParamsImplicitlyConvertible!(F, void function(T))
            && (isFunctionPointer!F || !hasUnsharedAliasing!F);
}

/**
 * Starts fn(args) in a new logical thread.
 *
 * Executes the supplied function in a new logical thread represented by
 * `Tid`.  The calling thread is designated as the owner of the new thread.
 * When the owner thread terminates an `OwnerTerminated` message will be
 * sent to the new thread, causing an `OwnerTerminated` exception to be
 * thrown on `receive()`.
 *
 * Params:
 *  fn   = The function to execute.
 *  args = Arguments to the function.
 *
 * Returns:
 *  A Tid representing the new logical thread.
 *
 * Notes:
 *  `args` must not have unshared aliasing.  In other words, all arguments
 *  to `fn` must either be `shared` or `immutable` or have no
 *  pointer indirection.  This is necessary for enforcing isolation among
 *  threads.
 */
Tid spawn(F, T...)(F fn, T args)
if (isSpawnable!(F, T))
{
    static assert(!hasLocalAliasing!(T), "Aliases to mutable thread-local data not allowed.");
    return _spawn(true, fn, args);
}

///
@system unittest
{
    static void f(string msg)
    {
        assert(msg == "Hello World");
    }

    auto tid = spawn(&f, "Hello World");
}

/// Fails: char[] has mutable aliasing.
@system unittest
{
    string msg = "Hello, World!";

    static void f1(string msg) {}
    static assert(!__traits(compiles, spawn(&f1, msg.dup)));
    static assert( __traits(compiles, spawn(&f1, msg.idup)));

    static void f2(char[] msg) {}
    static assert(!__traits(compiles, spawn(&f2, msg.dup)));
    static assert(!__traits(compiles, spawn(&f2, msg.idup)));
}

/**
 * Starts fn(args) in a logical thread and will receive a LinkTerminated
 * message when the operation terminates.
 *
 * Executes the supplied function in a new logical thread represented by
 * Tid.  This new thread is linked to the calling thread so that if either
 * it or the calling thread terminates a LinkTerminated message will be sent
 * to the other, causing a LinkTerminated exception to be thrown on receive().
 * The owner relationship from spawn() is preserved as well, so if the link
 * between threads is broken, owner termination will still result in an
 * OwnerTerminated exception to be thrown on receive().
 *
 * Params:
 *  fn   = The function to execute.
 *  args = Arguments to the function.
 *
 * Returns:
 *  A Tid representing the new thread.
 */
Tid spawnLinked(F, T...)(F fn, T args)
if (isSpawnable!(F, T))
{
    static assert(!hasLocalAliasing!(T), "Aliases to mutable thread-local data not allowed.");
    return _spawn(true, fn, args);
}

/*
 *
 */
private Tid _spawn(F, T...)(bool linked, F fn, T args)
if (isSpawnable!(F, T))
{
    // TODO: MessageList and &exec should be shared.
    auto spawnTid = Tid(new MessageBox);
    auto ownerTid = thisTid;

    void exec()
    {
        thisInfo.ident = spawnTid;
        thisInfo.owner = ownerTid;
        fn(args);
    }

    // TODO: MessageList and &exec should be shared.
    auto t = new Thread(&exec);
    t.start();

    thisInfo.links[spawnTid] = linked;
    return spawnTid;
}

@system unittest
{
    void function() fn1;
    void function(int) fn2;
    static assert(__traits(compiles, spawn(fn1)));
    static assert(__traits(compiles, spawn(fn2, 2)));
    static assert(!__traits(compiles, spawn(fn1, 1)));
    static assert(!__traits(compiles, spawn(fn2)));

    void delegate(int) shared dg1;
    shared(void delegate(int)) dg2;
    shared(void delegate(long) shared) dg3;
    shared(void delegate(real, int, long) shared) dg4;
    void delegate(int) immutable dg5;
    void delegate(int) dg6;
    static assert(__traits(compiles, spawn(dg1, 1)));
    static assert(__traits(compiles, spawn(dg2, 2)));
    static assert(__traits(compiles, spawn(dg3, 3)));
    static assert(__traits(compiles, spawn(dg4, 4, 4, 4)));
    static assert(__traits(compiles, spawn(dg5, 5)));
    static assert(!__traits(compiles, spawn(dg6, 6)));

    auto callable1  = new class{ void opCall(int) shared {} };
    auto callable2  = cast(shared) new class{ void opCall(int) shared {} };
    auto callable3  = new class{ void opCall(int) immutable {} };
    auto callable4  = cast(immutable) new class{ void opCall(int) immutable {} };
    auto callable5  = new class{ void opCall(int) {} };
    auto callable6  = cast(shared) new class{ void opCall(int) immutable {} };
    auto callable7  = cast(immutable) new class{ void opCall(int) shared {} };
    auto callable8  = cast(shared) new class{ void opCall(int) const shared {} };
    auto callable9  = cast(const shared) new class{ void opCall(int) shared {} };
    auto callable10 = cast(const shared) new class{ void opCall(int) const shared {} };
    auto callable11 = cast(immutable) new class{ void opCall(int) const shared {} };
    static assert(!__traits(compiles, spawn(callable1,  1)));
    static assert( __traits(compiles, spawn(callable2,  2)));
    static assert(!__traits(compiles, spawn(callable3,  3)));
    static assert( __traits(compiles, spawn(callable4,  4)));
    static assert(!__traits(compiles, spawn(callable5,  5)));
    static assert(!__traits(compiles, spawn(callable6,  6)));
    static assert(!__traits(compiles, spawn(callable7,  7)));
    static assert( __traits(compiles, spawn(callable8,  8)));
    static assert(!__traits(compiles, spawn(callable9,  9)));
    static assert( __traits(compiles, spawn(callable10, 10)));
    static assert( __traits(compiles, spawn(callable11, 11)));
}

/**
 * Places the values as a message at the back of tid's message queue.
 *
 * Sends the supplied value to the thread represented by tid.  As with
 * $(REF spawn, std,concurrency), `T` must not have unshared aliasing.
 */
void send(T...)(Tid tid, T vals)
{
    static assert(!hasLocalAliasing!(T), "Aliases to mutable thread-local data not allowed.");
    _send(tid, vals);
}

/*
 * ditto
 */
private void _send(T...)(Tid tid, T vals)
{
    _send(MsgType.standard, tid, vals);
}

/*
 * Implementation of send.  This allows parameter checking to be different for
 * both Tid.send() and .send().
 */
private void _send(T...)(MsgType type, Tid tid, T vals)
{
    auto msg = Message(type, vals);
    tid.mbox.request(msg);
}

///
public Response query(Tid tid, ref Request data)
{
    data.request_time = Clock.currTime();
    auto req = Message(MsgType.standard, data);
    auto res = request(tid, req);
    return *res.data.peek!(Response);
}

///
public Message request(Tid tid, ref Message msg)
{
    return tid.mbox.request(msg);
}

public alias ProcessDlg = scope Message delegate (ref Message msg);
public void process (Tid tid, ProcessDlg dg)
{
    tid.mbox.process(dg);
}

///
void shutdown (Tid tid) @trusted
{
    _send(tid, new Shutdown(tid));
}

private
{
    __gshared Tid[string] tidByName;
    __gshared string[][Tid] namesByTid;
}

private @property Mutex registryLock()
{
    __gshared Mutex impl;
    initOnce!impl(new Mutex);
    return impl;
}

private void unregisterMe()
{
    auto me = thisInfo.ident;
    if (thisInfo.ident != Tid.init)
    {
        synchronized (registryLock)
        {
            if (auto allNames = me in namesByTid)
            {
                foreach (name; *allNames)
                    tidByName.remove(name);
                namesByTid.remove(me);
            }
        }
    }
}

/**
 * Associates name with tid.
 *
 * Associates name with tid in a process-local map.  When the thread
 * represented by tid terminates, any names associated with it will be
 * automatically unregistered.
 *
 * Params:
 *  name = The name to associate with tid.
 *  tid  = The tid register by name.
 *
 * Returns:
 *  true if the name is available and tid is not known to represent a
 *  defunct thread.
 */
bool register(string name, Tid tid)
{
    synchronized (registryLock)
    {
        if (name in tidByName)
            return false;
        if (tid.mbox.isClosed)
            return false;
        namesByTid[tid] ~= name;
        tidByName[name] = tid;
        return true;
    }
}

/**
 * Removes the registered name associated with a tid.
 *
 * Params:
 *  name = The name to unregister.
 *
 * Returns:
 *  true if the name is registered, false if not.
 */
bool unregister(string name)
{
    import std.algorithm.mutation : remove, SwapStrategy;
    import std.algorithm.searching : countUntil;

    synchronized (registryLock)
    {
        if (auto tid = name in tidByName)
        {
            auto allNames = *tid in namesByTid;
            auto pos = countUntil(*allNames, name);
            remove!(SwapStrategy.unstable)(*allNames, pos);
            tidByName.remove(name);
            return true;
        }
        return false;
    }
}

/**
 * Gets the Tid associated with name.
 *
 * Params:
 *  name = The name to locate within the registry.
 *
 * Returns:
 *  The associated Tid or Tid.init if name is not registered.
 */
Tid locate(string name)
{
    synchronized (registryLock)
    {
        if (auto tid = name in tidByName)
            return *tid;
        return Tid.init;
    }
}

/**
 * Encapsulates all implementation-level data needed for scheduling.
 *
 * When defining a Scheduler, an instance of this struct must be associated
 * with each logical thread.  It contains all implementation-level information
 * needed by the internal API.
 */
struct ThreadInfo
{
    Tid ident;
    bool[Tid] links;
    Tid owner;

    /**
     * Gets a thread-local instance of ThreadInfo.
     *
     * Gets a thread-local instance of ThreadInfo, which should be used as the
     * default instance when info is requested for a thread not created by the
     * Scheduler.
     */
    static @property ref thisInfo() nothrow
    {
        static ThreadInfo val;
        return val;
    }

    /**
     * Cleans up this ThreadInfo.
     *
     * This must be called when a scheduled thread terminates.  It tears down
     * the messaging system for the thread and notifies interested parties of
     * the thread's termination.
     */
    void cleanup()
    {
        if (ident.mbox !is null)
            ident.mbox.close();
        foreach (tid; links.keys)
            _send(MsgType.linkDead, tid, ident);
        if (owner != Tid.init)
            _send(MsgType.linkDead, owner, ident);
        unregisterMe(); // clean up registry entries
    }
}

// Generator

/**
 * If the caller is a Fiber and is not a Generator, this function will call
 * scheduler.yield() or Fiber.yield(), as appropriate.
 */
void yield() nothrow
{
    auto fiber = Fiber.getThis();
    if (!(cast(IsGenerator) fiber))
    {
        if (fiber)
            return Fiber.yield();
    }
}

/// Used to determine whether a Generator is running.
private interface IsGenerator {}


/**
 * A Generator is a Fiber that periodically returns values of type T to the
 * caller via yield.  This is represented as an InputRange.
 */
class Generator(T) :
    Fiber, IsGenerator, InputRange!T
{
    /**
     * Initializes a generator object which is associated with a static
     * D function.  The function will be called once to prepare the range
     * for iteration.
     *
     * Params:
     *  fn = The fiber function.
     *
     * In:
     *  fn must not be null.
     */
    this(void function() fn)
    {
        super(fn);
        call();
    }

    /**
     * Initializes a generator object which is associated with a static
     * D function.  The function will be called once to prepare the range
     * for iteration.
     *
     * Params:
     *  fn = The fiber function.
     *  sz = The stack size for this fiber.
     *
     * In:
     *  fn must not be null.
     */
    this(void function() fn, size_t sz)
    {
        super(fn, sz);
        call();
    }

    /**
     * Initializes a generator object which is associated with a static
     * D function.  The function will be called once to prepare the range
     * for iteration.
     *
     * Params:
     *  fn = The fiber function.
     *  sz = The stack size for this fiber.
     *  guardPageSize = size of the guard page to trap fiber's stack
     *                  overflows. Refer to $(REF Fiber, core,thread)'s
     *                  documentation for more details.
     *
     * In:
     *  fn must not be null.
     */
    this(void function() fn, size_t sz, size_t guardPageSize)
    {
        super(fn, sz, guardPageSize);
        call();
    }

    /**
     * Initializes a generator object which is associated with a dynamic
     * D function.  The function will be called once to prepare the range
     * for iteration.
     *
     * Params:
     *  dg = The fiber function.
     *
     * In:
     *  dg must not be null.
     */
    this(void delegate() dg)
    {
        super(dg);
        call();
    }

    /**
     * Initializes a generator object which is associated with a dynamic
     * D function.  The function will be called once to prepare the range
     * for iteration.
     *
     * Params:
     *  dg = The fiber function.
     *  sz = The stack size for this fiber.
     *
     * In:
     *  dg must not be null.
     */
    this(void delegate() dg, size_t sz)
    {
        super(dg, sz);
        call();
    }

    /**
     * Initializes a generator object which is associated with a dynamic
     * D function.  The function will be called once to prepare the range
     * for iteration.
     *
     * Params:
     *  dg = The fiber function.
     *  sz = The stack size for this fiber.
     *  guardPageSize = size of the guard page to trap fiber's stack
     *                  overflows. Refer to $(REF Fiber, core,thread)'s
     *                  documentation for more details.
     *
     * In:
     *  dg must not be null.
     */
    this(void delegate() dg, size_t sz, size_t guardPageSize)
    {
        super(dg, sz, guardPageSize);
        call();
    }

    /**
     * Returns true if the generator is empty.
     */
    final bool empty() @property
    {
        return m_value is null || state == State.TERM;
    }

    /**
     * Obtains the next value from the underlying function.
     */
    final void popFront()
    {
        call();
    }

    /**
     * Returns the most recently generated value by shallow copy.
     */
    final T front() @property
    {
        return *m_value;
    }

    /**
     * Returns the most recently generated value without executing a
     * copy contructor. Will not compile for element types defining a
     * postblit, because Generator does not return by reference.
     */
    final T moveFront()
    {
        static if (!hasElaborateCopyConstructor!T)
        {
            return front;
        }
        else
        {
            static assert(0,
                    "Fiber front is always rvalue and thus cannot be moved since it defines a postblit.");
        }
    }

    final int opApply(scope int delegate(T) loopBody)
    {
        int broken;
        for (; !empty; popFront())
        {
            broken = loopBody(front);
            if (broken) break;
        }
        return broken;
    }

    final int opApply(scope int delegate(size_t, T) loopBody)
    {
        int broken;
        for (size_t i; !empty; ++i, popFront())
        {
            broken = loopBody(i, front);
            if (broken) break;
        }
        return broken;
    }
private:
    T* m_value;
}

/**
 * Yields a value of type T to the caller of the currently executing
 * generator.
 *
 * Params:
 *  value = The value to yield.
 */
void yield(T)(ref T value)
{
    Generator!T cur = cast(Generator!T) Fiber.getThis();
    if (cur !is null && cur.state == Fiber.State.EXEC)
    {
        cur.m_value = &value;
        return Fiber.yield();
    }
    throw new Exception("yield(T) called with no active generator for the supplied type");
}

/// ditto
void yield(T)(T value)
{
    yield(value);
}

///
@system unittest
{
    import std.range;

    InputRange!int myIota = iota(10).inputRangeObject;

    myIota.popFront();
    myIota.popFront();
    assert(myIota.moveFront == 2);
    assert(myIota.front == 2);
    myIota.popFront();
    assert(myIota.front == 3);

    //can be assigned to std.range.interfaces.InputRange directly
    myIota = new Generator!int(
    {
        foreach (i; 0 .. 10) yield(i);
    });

    myIota.popFront();
    myIota.popFront();
    assert(myIota.moveFront == 2);
    assert(myIota.front == 2);
    myIota.popFront();
    assert(myIota.front == 3);

    size_t[2] counter = [0, 0];
    foreach (i, unused; myIota) counter[] += [1, i];

    assert(myIota.empty);
    assert(counter == [7, 21]);
}

private
{
    /*
     * A MessageBox is a message queue for one thread.  Other threads may send
     * messages to this owner by calling put(), and the owner receives them by
     * calling get().  The put() call is therefore effectively shared and the
     * get() call is effectively local.  setMaxMsgs may be used by any thread
     * to limit the size of the message queue.
     */
    class MessageBox
    {
        this() @trusted nothrow /* TODO: make @safe after relevant druntime PR gets merged */
        {
            mutex = new Mutex;
            closed = false;
        }

        final Message request(ref Message req_msg)
        {
            this.mutex.lock();

            if (this.closed)
            {
                this.mutex.unlock();
                return Message(MsgType.standard, Response(Status.Failed, ""));
            }

            Message res_msg;
            Fiber fiber = Fiber.getThis();
            if (fiber !is null)
            {
                SudoFiber new_sf;
                new_sf.fiber = fiber;
                new_sf.req_msg = &req_msg;
                new_sf.res_msg = &res_msg;

                this.queue.insertBack(new_sf);
                this.mutex.unlock();
                Fiber.yield();
            }
            else
            {
                shared(bool) is_waiting = true;
                void stopWait() {
                    is_waiting = false;
                }
                SudoFiber new_sf;
                new_sf.fiber = null;
                new_sf.req_msg = &req_msg;
                new_sf.res_msg = &res_msg;
                new_sf.swdg = &stopWait;

                this.queue.insertBack(new_sf);
                this.mutex.unlock();
                while (is_waiting)
                    Thread.sleep(dur!("msecs")(1));
            }

            return res_msg;
        }

        final bool process(ProcessDlg dg)
        {
            bool onStandardReq(Message* req_msg, Message* res_msg)
            {
                if  (
                        (req_msg.convertsTo!(OwnerTerminated)) ||
                        (req_msg.convertsTo!(Shutdown))
                    )
                {
                    Message shutdown = Message(MsgType.shutdown, "");
                    dg(shutdown);
                }
                else
                {
                    *res_msg = dg(*req_msg);
                }

                return true;
            }

            bool onStandardMsg(Message* msg)
            {
                dg(*msg);

                return true;
            }

            bool onLinkDeadMsg(Message* msg)
            {
                assert(msg.convertsTo!(Tid));
                auto tid = msg.get!(Tid);

                if (bool* pDepends = tid in thisInfo.links)
                {
                    auto depends = *pDepends;
                    thisInfo.links.remove(tid);

                    if (depends && tid != thisInfo.owner)
                    {
                        auto e = new LinkTerminated(tid);
                        auto m = Message(MsgType.standard, e);
                        if (onStandardMsg(&m))
                            return true;
                        throw e;
                    }
                }

                if (tid == thisInfo.owner)
                {
                    thisInfo.owner = Tid.init;
                    auto e = new OwnerTerminated(tid);
                    auto m = Message(MsgType.standard, e);
                    if (onStandardMsg(&m))
                        return true;
                    throw e;
                }

                return false;
            }

            bool onControlMsg(Message* msg)
            {
                switch (msg.type)
                {
                    case MsgType.linkDead:
                        return onLinkDeadMsg(msg);
                    default:
                        return false;
                }
            }

            this.mutex.lock();
            scope (exit) this.mutex.unlock();

            if (this.closed)
            {
                return false;
            }

            if (this.queue[].walkLength > 0)
            {
                SudoFiber sf = this.queue.front;

                this.queue.removeFront();

                if (isControlMsg(sf.req_msg))
                    onControlMsg(sf.req_msg);
                else
                    onStandardReq(sf.req_msg, sf.res_msg);

                if (sf.fiber !is null)
                    sf.fiber.call();
                else if (sf.swdg !is null)
                    sf.swdg();

                return true;
            }

            return false;
        }

        ///
        final @property bool isClosed() @safe @nogc pure
        {
            synchronized (mutex)
            {
                return closed;
            }
        }

        final void close()
        {
            static void onLinkDeadMsg(Message* msg)
            {
                assert(msg.convertsTo!(Tid));
                auto tid = msg.get!(Tid);

                thisInfo.links.remove(tid);
                if (tid == thisInfo.owner)
                    thisInfo.owner = Tid.init;
            }

            SudoFiber sf;
            bool res;

            this.mutex.lock();
            scope (exit) this.mutex.unlock();

            this.closed = true;

            while (true)
            {
                if (this.queue[].walkLength == 0)
                    break;

                sf = this.queue.front;

                if (sf.req_msg.type == MsgType.linkDead)
                    onLinkDeadMsg(sf.req_msg);

                this.queue.removeFront();

                if (sf.fiber !is null)
                    sf.fiber.call();
                else if (sf.swdg !is null)
                    sf.swdg();
            }
        }

    private:

        private bool isControlMsg(Message* msg) @safe @nogc pure nothrow
        {
            return msg.type != MsgType.standard;
        }

        private bool isLinkDeadMsg(Message* msg) @safe @nogc pure nothrow
        {
            return msg.type == MsgType.linkDead;
        }

        /// closed
        bool closed;

        /// lock
        Mutex mutex;

        /// collection of equest waiters
        DList!(SudoFiber) queue;
    }
}

private alias StopWaitDg = void delegate ();

///
private struct SudoFiber
{
    public Fiber fiber;
    public Message* req_msg;
    public Message* res_msg;
    public StopWaitDg swdg;
}

private @property shared(Mutex) initOnceLock()
{
    static shared Mutex lock;
    if (auto mtx = atomicLoad!(MemoryOrder.acq)(lock))
        return mtx;
    auto mtx = new shared Mutex;
    if (cas(&lock, cast(shared) null, mtx))
        return mtx;
    return atomicLoad!(MemoryOrder.acq)(lock);
}

/**
 * Initializes $(D_PARAM var) with the lazy $(D_PARAM init) value in a
 * thread-safe manner.
 *
 * The implementation guarantees that all threads simultaneously calling
 * initOnce with the same $(D_PARAM var) argument block until $(D_PARAM var) is
 * fully initialized. All side-effects of $(D_PARAM init) are globally visible
 * afterwards.
 *
 * Params:
 *   var = The variable to initialize
 *   init = The lazy initializer value
 *
 * Returns:
 *   A reference to the initialized variable
 */
auto ref initOnce(alias var)(lazy typeof(var) init)
{
    return initOnce!var(init, initOnceLock);
}

/**
 * Same as above, but takes a separate mutex instead of sharing one among
 * all initOnce instances.
 *
 * This should be used to avoid dead-locks when the $(D_PARAM init)
 * expression waits for the result of another thread that might also
 * call initOnce. Use with care.
 *
 * Params:
 *   var = The variable to initialize
 *   init = The lazy initializer value
 *   mutex = A mutex to prevent race conditions
 *
 * Returns:
 *   A reference to the initialized variable
 */
auto ref initOnce(alias var)(lazy typeof(var) init, shared Mutex mutex)
{
    // check that var is global, can't take address of a TLS variable
    static assert(is(typeof({ __gshared p = &var; })),
        "var must be 'static shared' or '__gshared'.");
    import core.atomic : atomicLoad, MemoryOrder, atomicStore;

    static shared bool flag;
    if (!atomicLoad!(MemoryOrder.acq)(flag))
    {
        synchronized (mutex)
        {
            if (!atomicLoad!(MemoryOrder.raw)(flag))
            {
                var = init;
                atomicStore!(MemoryOrder.rel)(flag, true);
            }
        }
    }
    return var;
}

/// ditto
auto ref initOnce(alias var)(lazy typeof(var) init, Mutex mutex)
{
    return initOnce!var(init, cast(shared) mutex);
}

@system unittest
{
    import std.conv;

    auto child = spawn({
        bool terminated = false;
        auto sleep_inteval = dur!("msecs")(1);
        while (!terminated)
        {
            thisTid.process((ref Message msg) {
                Message res_msg;
                if (msg.type == MsgType.shutdown)
                {
                    terminated = true;
                    return Message(MsgType.shutdown, Response(Status.Success));
                }

                if (msg.convertsTo!(Request))
                {
                    auto req = msg.get!(Request);
                    if (req.method == "pow")
                    {
                        immutable int value = to!int(req.args);
                        return Message(MsgType.standard, Response(Status.Success, to!string(value * value)));
                    }
                }
                return Message(MsgType.standard, Response(Status.Failed));
            });
            Thread.sleep(sleep_inteval);
        }
    });

    auto req = Request(thisTid(), "pow", "2");
    auto res = child.query(req);
    assert(res.data == "4");

    child.shutdown();
    thisInfo.cleanup();
}
