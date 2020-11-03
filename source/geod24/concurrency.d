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
 * Note:
 * Copied (almost verbatim) from Phobos at commit 3bfccf4f1 (2019-11-27)
 * Changes are this notice, and the module rename, from `std.concurrency`
 * to `geod24.concurrency`.
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

public import std.variant;

import core.atomic;
import core.sync.condition;
import core.sync.mutex;
import core.sync.semaphore;
import core.thread;
import core.time : MonoTime;
import std.range.primitives;
import std.traits;
import std.algorithm;
import std.typecons;
import std.container : DList;
import std.exception : assumeWontThrow;

import geod24.RingBuffer;

///
@system unittest
{
    __gshared string received;
    static void spawnedFunc(Tid self, Tid ownerTid)
    {
        import std.conv : text;
        // Receive a message from the owner thread.
        self.receive((int i){
            received = text("Received the number ", i);

            // Send a message back to the owner thread
            // indicating success.
            send(ownerTid, true);
        });
    }

    // Start spawnedFunc in a new thread.
    auto childTid = spawn(&spawnedFunc, thisTid);
    auto self = thisTid();

    // Send the number 42 to this new thread.
    send(childTid, 42);

    // Receive the result code.
    auto wasSuccessful = self.receiveOnly!(bool);
    assert(wasSuccessful);
    assert(received == "Received the number 42");
}

private
{
    bool hasLocalAliasing(Types...)()
    {
        import std.typecons : Rebindable;

        // Works around "statement is not reachable"
        bool doesIt = false;
        static foreach (T; Types)
        {
            static if (is(T == Tid))
            { /* Allowed */ }
            else static if (is(T : Rebindable!R, R))
                doesIt |= hasLocalAliasing!R;
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

    @safe unittest
    {
        /* Issue 20097 */
        import std.datetime.systime : SysTime;
        static struct Container { SysTime time; }
        static assert(!hasLocalAliasing!(SysTime, Container));
    }

    struct Message
    {
        Variant data;

        this(T...)(T vals) if (T.length > 0)
        {
            static if (T.length == 1)
            {
                data = vals[0];
            }
            else
            {
                import std.typecons : Tuple;

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
        import std.format : format;

        foreach (i, t1; T)
        {
            static assert(isFunctionPointer!t1 || isDelegate!t1,
                    format!"T %d is not a function pointer or delegates"(i));
            alias a1 = Parameters!(t1);
            alias r1 = ReturnType!(t1);

            static if (i < T.length - 1 && is(r1 == void))
            {
                static assert(a1.length != 1 || !is(a1[0] == Variant),
                              "function with arguments " ~ a1.stringof ~
                              " occludes successive function");

                foreach (t2; T[i + 1 .. $])
                {
                    alias a2 = Parameters!(t2);

                    static assert(!is(a1 == a2),
                        "function with arguments " ~ a1.stringof ~ " occludes successive function");
                }
            }
        }
    }

    @property ref ThreadInfo thisInfo() nothrow
    {
        auto t = cast(InfoThread)Thread.getThis();

        if (t !is null)
            return t.info;

        return ThreadInfo.thisInfo;
    }
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
 * Thrown when a Tid is missing, e.g. when `ownerTid` doesn't
 * find an owner thread.
 */
class TidMissingException : Exception
{
    import std.exception : basicExceptionCtors;
    ///
    mixin basicExceptionCtors;
}

class FiberBlockedException : Exception
{
    this(string msg = "Fiber is blocked") @safe pure nothrow @nogc
    {
        super(msg);
    }
}

// Thread ID


/**
 * An opaque type used to represent a logical thread.
 */
struct Tid
{
package:
    this(MessageBox m) @safe pure nothrow @nogc
    {
        mbox = m;
    }

    MessageBox mbox;

public:

    /**
     * Generate a convenient string for identifying this Tid.  This is only
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
            && isParamsImplicitlyConvertible!(F, void function(Tid, T))
            && (isFunctionPointer!F || !hasUnsharedAliasing!F);
}

/**
 * Starts fn(args) in a new logical thread.
 *
 * Executes the supplied function in a new logical thread represented by
 * `Tid`.
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
    return _spawn(fn, args);
}

///
@system unittest
{
    static void f(Tid self, string msg)
    {
        assert(msg == "Hello World");
    }

    auto tid = spawn(&f, "Hello World");
}

/// Fails: char[] has mutable aliasing.
@system unittest
{
    string msg = "Hello, World!";

    static void f1(Tid self, string msg) {}
    static assert(!__traits(compiles, spawn(&f1, msg.dup)));
    static assert( __traits(compiles, spawn(&f1, msg.idup)));

    static void f2(Tid self, char[] msg) {}
    static assert(!__traits(compiles, spawn(&f2, msg.dup)));
    static assert(!__traits(compiles, spawn(&f2, msg.idup)));
}

/// New thread with anonymous function
@system unittest
{
    auto self = thisTid();
    spawn((Tid self, Tid caller) {
        caller.send("This is so great!");
    }, self);
    assert(self.receiveOnly!string == "This is so great!");
}

/*
 *
 */
private Tid _spawn(F, T...)(F fn, T args)
if (isSpawnable!(F, T))
{
    // TODO: MessageList and &exec should be shared.
    auto spawnTid = Tid(new MessageBox);

    void exec()
    {
        thisInfo.ident = spawnTid;
        fn(spawnTid, args);
    }

    // TODO: MessageList and &exec should be shared.
    auto t = new InfoThread(&exec);
    t.start();
    return spawnTid;
}

@system unittest
{
    void function(Tid) fn1;
    void function(Tid, int) fn2;
    static assert(__traits(compiles, spawn(fn1)));
    static assert(__traits(compiles, spawn(fn2, 2)));
    static assert(!__traits(compiles, spawn(fn1, 1)));
    static assert(!__traits(compiles, spawn(fn2)));

    void delegate(Tid, int) shared dg1;
    shared(void delegate(Tid, int)) dg2;
    shared(void delegate(Tid, long) shared) dg3;
    shared(void delegate(Tid, real, int, long) shared) dg4;
    void delegate(Tid, int) immutable dg5;
    void delegate(Tid, int) dg6;
    static assert(__traits(compiles, spawn(dg1, 1)));
    static assert(__traits(compiles, spawn(dg2, 2)));
    static assert(__traits(compiles, spawn(dg3, 3)));
    static assert(__traits(compiles, spawn(dg4, 4, 4, 4)));
    static assert(__traits(compiles, spawn(dg5, 5)));
    static assert(!__traits(compiles, spawn(dg6, 6)));

    auto callable1  = new class{ void opCall(Tid, int) shared {} };
    auto callable2  = cast(shared) new class{ void opCall(Tid, int) shared {} };
    auto callable3  = new class{ void opCall(Tid, int) immutable {} };
    auto callable4  = cast(immutable) new class{ void opCall(Tid, int) immutable {} };
    auto callable5  = new class{ void opCall(Tid, int) {} };
    auto callable6  = cast(shared) new class{ void opCall(Tid, int) immutable {} };
    auto callable7  = cast(immutable) new class{ void opCall(Tid, int) shared {} };
    auto callable8  = cast(shared) new class{ void opCall(Tid, int) const shared {} };
    auto callable9  = cast(const shared) new class{ void opCall(Tid, int) shared {} };
    auto callable10 = cast(const shared) new class{ void opCall(Tid, int) const shared {} };
    auto callable11 = cast(immutable) new class{ void opCall(Tid, int) const shared {} };
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
    auto msg = Message(vals);
    tid.mbox.put(msg) || assert(0, "MessageBox is closed");
}

/// Ditto, but do not assert in case of failure
bool trySend(T...)(Tid tid, T vals)
{
    static assert(!hasLocalAliasing!(T), "Aliases to mutable thread-local data not allowed.");
    auto msg = Message(vals);
    return tid.mbox.put(msg);
}

/**
 * Receives a message from another thread.
 *
 * Receive a message from another thread, or block if no messages of the
 * specified types are available.  This function works by pattern matching
 * a message against a set of delegates and executing the first match found.
 *
 * If a delegate that accepts a $(REF Variant, std,variant) is included as
 * the last argument to `receive`, it will match any message that was not
 * matched by an earlier delegate.  If more than one argument is sent,
 * the `Variant` will contain a $(REF Tuple, std,typecons) of all values
 * sent.
 */
void receive(T...)(Tid self, T ops )
in
{
    assert(self.mbox !is null,
           "Cannot receive a message until a thread was spawned "
           ~ "or thisTid was passed to a running thread.");
}
do
{
    checkops( ops );
    self.mbox.getUntimed(ops);
}

///
@system unittest
{
    import std.variant : Variant;

    auto process = (Tid self, Tid caller)
    {
        self.receive(
            (int i) { caller.send(1); },
            (double f) { caller.send(2); },
            (Variant v) { caller.send(3); }
        );
    };

    auto self = thisTid();
    {
        auto tid = spawn(process, self);
        send(tid, 42);
        assert(self.receiveOnly!int == 1);
    }

    {
        auto tid = spawn(process, self);
        send(tid, 3.14);
        assert(self.receiveOnly!int == 2);
    }

    {
        auto tid = spawn(process, self);
        send(tid, "something else");
        assert(self.receiveOnly!int == 3);
    }
}

@safe unittest
{
    static assert( __traits( compiles,
                      {
                          receive(Tid.init, (Variant x) {} );
                          receive(Tid.init, (int x) {}, (Variant x) {} );
                      } ) );

    static assert( !__traits( compiles,
                       {
                           receive(Tid.init, (Variant x) {}, (int x) {} );
                       } ) );

    static assert( !__traits( compiles,
                       {
                           receive(Tid.init, (int x) {}, (int x) {} );
                       } ) );
}

// Make sure receive() works with free functions as well.
version (unittest)
{
    private void receiveFunction(int x) {}
}
@safe unittest
{
    static assert( __traits( compiles,
                      {
                          receive(Tid.init, &receiveFunction );
                          receive(Tid.init, &receiveFunction, (Variant x) {} );
                      } ) );
}


private template receiveOnlyRet(T...)
{
    static if ( T.length == 1 )
    {
        alias receiveOnlyRet = T[0];
    }
    else
    {
        import std.typecons : Tuple;
        alias receiveOnlyRet = Tuple!(T);
    }
}

/**
 * Receives only messages with arguments of types `T`.
 *
 * Throws:  `MessageMismatch` if a message of types other than `T`
 *          is received.
 *
 * Returns: The received message.  If `T.length` is greater than one,
 *          the message will be packed into a $(REF Tuple, std,typecons).
 */
receiveOnlyRet!(T) receiveOnly(T...)(Tid self)
in
{
    assert(self.mbox !is null,
        "Cannot receive a message until a thread was spawned or thisTid was passed to a running thread.");
}
do
{
    import std.format : format;
    import std.typecons : Tuple;

    Tuple!(T) ret;

    self.mbox.getUntimed((T val) {
        static if (T.length)
            ret.field = val;
    },
    (Variant val) {
        static if (T.length > 1)
            string exp = T.stringof;
        else
            string exp = T[0].stringof;

        throw new MessageMismatch(
            format("Unexpected message type: expected '%s', got '%s'", exp, val.type.toString()));
    });
    static if (T.length == 1)
        return ret[0];
    else
        return ret;
}

///
@system unittest
{
    auto tid = spawn(
    (Tid self) {
        assert(self.receiveOnly!int == 42);
    });
    send(tid, 42);
}

///
@system unittest
{
    auto tid = spawn(
    (Tid self) {
        assert(self.receiveOnly!string == "text");
    });
    send(tid, "text");
}

///
@system unittest
{
    struct Record { string name; int age; }

    auto tid = spawn(
    (Tid self) {
        auto msg = self.receiveOnly!(double, Record);
        assert(msg[0] == 0.5);
        assert(msg[1].name == "Alice");
        assert(msg[1].age == 31);
    });

    send(tid, 0.5, Record("Alice", 31));
}

@system unittest
{
    static void t1(Tid self, Tid mainTid)
    {
        try
        {
            self.receiveOnly!string();
            mainTid.send("");
        }
        catch (Throwable th)
        {
            mainTid.send(th.msg);
        }
    }

    auto self = thisTid();
    auto tid = spawn(&t1, self);
    tid.send(1);
    string result = self.receiveOnly!string();
    assert(result == "Unexpected message type: expected 'string', got 'int'");
}

/**
 * Tries to receive but will give up if no matches arrive within duration.
 * Won't wait at all if provided $(REF Duration, core,time) is negative.
 *
 * Same as `receive` except that rather than wait forever for a message,
 * it waits until either it receives a message or the given
 * $(REF Duration, core,time) has passed. It returns `true` if it received a
 * message and `false` if it timed out waiting for one.
 */
bool receiveTimeout(T...)(Tid self, Duration duration, T ops)
in
{
    assert(self.mbox !is null,
        "Cannot receive a message until a thread was spawned or thisTid was passed to a running thread.");
}
do
{
    checkops(ops);
    return self.mbox.get(duration, ops);
}

@safe unittest
{
    static assert(__traits(compiles, {
        receiveTimeout(Tid.init, msecs(0), (Variant x) {});
        receiveTimeout(Tid.init, msecs(0), (int x) {}, (Variant x) {});
    }));

    static assert(!__traits(compiles, {
        receiveTimeout(Tid.init, msecs(0), (Variant x) {}, (int x) {});
    }));

    static assert(!__traits(compiles, {
        receiveTimeout(Tid.init, msecs(0), (int x) {}, (int x) {});
    }));

    static assert(__traits(compiles, {
        receiveTimeout(Tid.init, msecs(10), (int x) {}, (Variant x) {});
    }));
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
    }
}


/***************************************************************************

    Thread with ThreadInfo,
    This is implemented to avoid using global variables.

***************************************************************************/

public class InfoThread : Thread
{
    public ThreadInfo info;

    /***************************************************************************

        Initializes a thread object which is associated with a static

        Params:
            fn = The thread function.
            sz = The stack size for this thread.

    ***************************************************************************/

    this (void function() fn, size_t sz = 0) @safe pure nothrow @nogc
    {
        super(fn, sz);
    }


    /***************************************************************************

        Initializes a thread object which is associated with a dynamic

        Params:
            dg = The thread function.
            sz = The stack size for this thread.

    ***************************************************************************/

    this (void delegate() dg, size_t sz = 0) @safe pure nothrow @nogc
    {
        super(dg, sz);
    }
}


private FiberScheduler thisSchedulerStorage;

/***************************************************************************

    Get current running FiberScheduler

    Returns:
        Returns a reference to the current scheduler or null no
        scheduler is running

    TODO: Support for nested schedulers

***************************************************************************/

public FiberScheduler thisScheduler () nothrow
{
    return thisSchedulerStorage;
}

/***************************************************************************

    Set current running FiberScheduler

    Params:
        value = Reference to the current FiberScheduler

***************************************************************************/

public void thisScheduler (FiberScheduler value) nothrow
{
    thisSchedulerStorage = value;
}

/**
 * An example Scheduler using Fibers.
 *
 * This is an example scheduler that creates a new Fiber per call to spawn
 * and multiplexes the execution of all fibers within the main thread.
 */
class FiberScheduler
{
    /// Default ctor
    this () nothrow
    {
        this.sem = assumeWontThrow(new Semaphore());
        this.blocked_ex = new FiberBlockedException();
    }

    /**
     * This creates a new Fiber for the supplied op and then starts the
     * dispatcher.
     */
    void start(void delegate() op)
    {
        create(op, true);
        // Make sure the just-created fiber is run first
        dispatch();
    }

    /**
     * This created a new Fiber for the supplied op and adds it to the
     * dispatch list.
     */
    void spawn(void delegate() op) nothrow
    {
        create(op);
        FiberScheduler.yield();
    }

    /**************************************************************************

        Schedule a task to be run next time the scheduler yields

        Behave similarly to `spawn`, but instead of running the task
        immediately, it simply adds it to the queue and continue executing
        the current task.

        Params:
            op = Operation to run

    **************************************************************************/

    void schedule(void delegate() op) nothrow
    {
        this.create(op);
    }

    /**
     * If the caller is a scheduled Fiber, this yields execution to another
     * scheduled Fiber.
     */
    static void yield() nothrow
    {
        // NOTE: It's possible that we should test whether the calling Fiber
        //       is an InfoFiber before yielding, but I think it's reasonable
        //       that any fiber should yield here.
        if (Fiber.getThis())
            Fiber.yield();
    }

    /**
     * If the caller is a scheduled Fiber, this yields execution to another
     * scheduled Fiber.
     */
    static void yieldAndThrow(Throwable t) nothrow
    {
        // NOTE: It's possible that we should test whether the calling Fiber
        //       is an InfoFiber before yielding, but I think it's reasonable
        //       that any fiber should yield here.
        if (Fiber.getThis())
            Fiber.yieldAndThrow(t);
    }

    /**
     * Returns an appropriate ThreadInfo instance.
     *
     * Returns a ThreadInfo instance specific to the calling Fiber if the
     * Fiber was created by this dispatcher, otherwise it returns
     * ThreadInfo.thisInfo.
     */
    @property ref ThreadInfo thisInfo() nothrow
    {
        auto f = cast(InfoFiber) Fiber.getThis();

        if (f !is null)
            return f.info;

        auto t = cast(InfoThread)Thread.getThis();

        if (t !is null)
            return t.info;

        return ThreadInfo.thisInfo;
    }

    /// Resource type that will be tracked by FiberScheduler
    interface Resource
    {
        ///
        void release () nothrow;
    }

    /***********************************************************************

        Add Resource to the Resource list of runnning Fiber

        Param:
            r = Resource instace

    ***********************************************************************/

    void addResource (Resource r, InfoFiber info_fiber = cast(InfoFiber) Fiber.getThis()) nothrow
    {
        assert(info_fiber, "Called from outside of an InfoFiber");
        info_fiber.resources.insert(r);
    }

    /***********************************************************************

        Remove Resource from the Resource list of runnning Fiber

        Param:
            r = Resource instance

        Returns:
            Success/failure

    ***********************************************************************/

    bool removeResource (Resource r, InfoFiber info_fiber = cast(InfoFiber) Fiber.getThis()) nothrow
    {
        assert(info_fiber, "Called from outside of an InfoFiber");
        // TODO: For some cases, search is not neccesary. We can just pop the last element
        return assumeWontThrow(info_fiber.resources.linearRemoveElement(r));
    }

protected:
    /**
     * Creates a new Fiber which calls the given delegate.
     *
     * Params:
     *   op = The delegate the fiber should call
     *   insert_front = Fiber will be added to the front
     *                  of the ready queue to be run first
     */
    void create (void delegate() op, bool insert_front = false) nothrow
    {
        void wrap()
        {
            scope (exit)
            {
                thisInfo.cleanup();
            }
            op();
        }

        if (insert_front)
            this.readyq.insertFront(new InfoFiber(&wrap));
        else
            this.readyq.insertBack(new InfoFiber(&wrap));
    }

    /**
     * Fiber which embeds a ThreadInfo
     */
    static class InfoFiber : Fiber
    {
        ThreadInfo info;

        /// Semaphore reference that this Fiber is blocked on
        FiberBlocker blocker;

        /// List of Resources held by this Fiber
        DList!Resource resources;

        this (void delegate() op, size_t sz = 512 * 1024) nothrow
        {
            super(op, sz);
        }
    }

    final public class FiberBlocker
    {

        /***********************************************************************

            Associate `FiberBlocker` with the running `Fiber`

            `FiberScheduler` will check to see if the `Fiber` is blocking on a
            `FiberBlocker` to avoid rescheduling it unnecessarily

        ***********************************************************************/

        private void registerToInfoFiber (InfoFiber info_fiber = cast(InfoFiber) Fiber.getThis()) nothrow
        {
            assert(info_fiber !is null, "This Fiber does not belong to FiberScheduler");
            assert(info_fiber.blocker is null, "This Fiber already has a registered FiberBlocker");
            info_fiber.blocker = this;

        }

        /***********************************************************************

            Wait on the blocker with optional timeout

            Params:
                period = Timeout period

        ***********************************************************************/

        bool wait (Duration period = Duration.init) nothrow
        {
            if (period != Duration.init)
                this.limit = MonoTime.currTime + period;

            if (this.shouldBlock())
            {
                this.registerToInfoFiber();
                FiberScheduler.yieldAndThrow(this.outer.blocked_ex);
            }

            this.limit = MonoTime.init;
            this.notified = false;
            return !this.hasTimedOut();
        }

        /***********************************************************************

            Unblock the Fiber waiting on this blocker

        ***********************************************************************/

        void notify () nothrow
        {
            this.stopTimer();
            this.notified = true;
            assumeWontThrow(this.outer.sem.notify());
        }

        /***********************************************************************

            Query if `FiberBlocker` should still block

            FiberBlocker will block the Fiber until it is notified or
            the specified timeout is reached.

        ***********************************************************************/

        bool shouldBlock () nothrow
        {
            bool timed_out = (this.limit != MonoTime.init
                                && MonoTime.currTime >= this.limit);

            if (timed_out)
                cas(&this.timer_state, TimerState.Running, TimerState.TimedOut);

            return atomicLoad(this.timer_state) != TimerState.TimedOut && !this.notified;
        }

        /***********************************************************************

            Try freezing the internal timer

        ***********************************************************************/

        bool stopTimer () nothrow
        {
            return cas(&this.timer_state, TimerState.Running, TimerState.Stopped);
        }

        /***********************************************************************

            Query if the internal timer has timed out

        ***********************************************************************/

        bool hasTimedOut () nothrow
        {
            return atomicLoad(this.timer_state) == TimerState.TimedOut;
        }

        MonoTime getTimeout () nothrow
        {
            return limit;
        }

    private:
        enum TimerState
        {
            Running,
            TimedOut,
            Stopped
        }

        /// Time limit that will eventually unblock the caller if a timeout is specified
        MonoTime limit = MonoTime.init;

        /// State of the blocker
        bool notified;

        /// State of the internal timer
        shared(TimerState) timer_state;
    }

private:

    /***********************************************************************

        Start the scheduling loop

    ***********************************************************************/

    void dispatch ()
    {
        thisScheduler(this);
        scope (exit) thisScheduler(null);

        MonoTime earliest_timeout;

        while (!this.readyq.empty() || !this.wait_list.empty())
        {
            while (!readyq.empty())
            {
                InfoFiber cur_fiber = this.readyq.front();
                this.readyq.removeFront();

                assert(cur_fiber.state != Fiber.State.TERM);

                auto t = cur_fiber.call(Fiber.Rethrow.no);

                // Fibers that block on a FiberBlocker throw an
                // exception for scheduler to catch
                if (t is this.blocked_ex)
                {
                    auto cur_timeout = cur_fiber.blocker.getTimeout();

                    // Keep track of the earliest timeout in the system
                    if (cur_timeout != MonoTime.init
                            && (earliest_timeout == MonoTime.init || cur_timeout < earliest_timeout))
                    {
                        earliest_timeout = cur_timeout;
                    }

                    this.wait_list.insert(cur_fiber);
                    continue;
                }
                else if (t)
                {
                    // We are exiting the dispatch loop prematurely, all resources
                    // held by Fibers should be released.
                    this.releaseResources(cur_fiber);
                    throw t;
                }

                if (cur_fiber.state != Fiber.State.TERM)
                {
                    this.readyq.insert(cur_fiber);
                }

                // See if there are Fibers to be woken up if we reach a timeout
                // or the scheduler semaphore was notified
                if (MonoTime.currTime >= earliest_timeout || this.sem.tryWait())
                    earliest_timeout = wakeFibers();
            }

            if (!this.wait_list.empty())
            {
                Duration time_to_timeout = earliest_timeout - MonoTime.currTime;

                // Sleep until a timeout or an event
                if (earliest_timeout == MonoTime.init)
                    this.sem.wait();
                else if (time_to_timeout > 0.seconds)
                    this.sem.wait(time_to_timeout);
            }

            // OS Thread woke up populate ready queue
            earliest_timeout = wakeFibers();
        }
    }

    /***********************************************************************

        Move unblocked Fibers to ready queue

        Return:
            Returns the earliest timeout left in the waiting list

    ***********************************************************************/

    MonoTime wakeFibers()
    {
        import std.range;
        MonoTime earliest_timeout;

        auto wait_range = this.wait_list[];
        while (!wait_range.empty)
        {
            auto fiber = wait_range.front;

            // Remove the unblocked Fiber from wait list and
            // append it to the end ofready queue
            if (!fiber.blocker.shouldBlock())
            {
                this.wait_list.popFirstOf(wait_range);
                fiber.blocker = null;
                this.readyq.insert(fiber);
            }
            else
            {
                auto timeout = fiber.blocker.getTimeout();
                if (timeout != MonoTime.init
                        && (earliest_timeout == MonoTime.init || timeout < earliest_timeout))
                {
                    earliest_timeout = timeout;
                }
                wait_range.popFront();
            }
        }

        return earliest_timeout;
    }

    /***********************************************************************

        Release all resources currently held by all Fibers owned by this
        scheduler

        Param:
            cur_fiber = Running Fiber

    ***********************************************************************/

    void releaseResources (InfoFiber cur_fiber)
    {
        foreach (ref resource; cur_fiber.resources)
            resource.release();
        foreach (ref fiber; this.readyq)
            foreach (ref resource; fiber.resources)
                resource.release();
        foreach (ref fiber; this.wait_list)
            foreach (ref resource; fiber.resources)
                resource.release();
    }

private:

    /// OS semaphore for scheduler to sleep on
    Semaphore sem;

    /// A FIFO Queue of Fibers ready to run
    DList!InfoFiber readyq;

    /// List of Fibers waiting for an event
    DList!InfoFiber wait_list;

    /// Cached instance of FiberBlockedException
    FiberBlockedException blocked_ex;
}

/// Ensure argument to `start` is run first
unittest
{
    {
        scope sched = new FiberScheduler();
        bool startHasRun;
        sched.spawn(() => assert(startHasRun));
        sched.start(() { startHasRun = true; });
    }
    {
        scope sched = new FiberScheduler();
        bool startHasRun;
        sched.schedule(() => assert(startHasRun));
        sched.start(() { startHasRun = true; });
    }
}

/*
 * A MessageBox is a message queue for one thread.  Other threads may send
 * messages to this owner by calling put(), and the owner receives them by
 * calling get().  The put() call is therefore effectively shared and the
 * get() call is effectively local.  setMaxMsgs may be used by any thread
 * to limit the size of the message queue.
 */
package class MessageBox
{
    this() @safe nothrow
    {
        m_lock = new Mutex;
        m_closed = false;

        m_putMsg = new Condition(m_lock);
    }

    ///
    final @property bool isClosed() @safe @nogc pure
    {
        synchronized (m_lock)
        {
            return m_closed;
        }
    }

    /*
     * If maxMsgs is not set, the message is added to the queue and the
     * owner is notified. If the queue is full, onCrowdingDoThis is called.
     * If the routine returns true, this call will block until
     * the owner has made space available in the queue.  If it returns
     * false, this call will abort.
     *
     * Params:
     *  msg = The message to put in the queue.
     *
     * Returns:
     *  `false` if the message box is closed, `true` otherwise
     */
    final bool put (ref Message msg)
    {
        synchronized (m_lock)
        {
            if (m_closed)
                return false;
            m_sharedBox.put(msg);
            m_putMsg.notify();
        }
        return true;
    }

    /*
     * Matches ops against each message in turn until a match is found.
     *
     * Params:
     *  ops = The operations to match.  Each may return a bool to indicate
     *        whether a message with a matching type is truly a match.
     *
     * Returns:
     *  true if a message was retrieved and false if not (such as if a
     *  timeout occurred).
     */
    bool getUntimed(Ops...)(scope Ops ops)
    {
        return this.get(Duration.init, ops);
    }

    bool get(Ops...)(Duration period, scope Ops ops)
    {
        immutable timedWait = period !is Duration.init;
        MonoTime limit = timedWait ? MonoTime.currTime + period : MonoTime.init;

        bool onStandardMsg(ref Message msg)
        {
            foreach (i, t; Ops)
            {
                alias Args = Parameters!(t);
                auto op = ops[i];

                if (msg.convertsTo!(Args))
                {
                    static if (is(ReturnType!(t) == bool))
                    {
                        return msg.map(op);
                    }
                    else
                    {
                        msg.map(op);
                        return true;
                    }
                }
            }
            return false;
        }

        bool scan(ref ListT list)
        {
            for (auto range = list[]; !range.empty;)
            {
                // Only the message handler will throw, so if this occurs
                // we can be certain that the message was handled.
                scope (failure)
                    list.removeAt(range);

                if (onStandardMsg(range.front))
                {
                    list.removeAt(range);
                    return true;
                }
                range.popFront();
                continue;
            }
            return false;
        }

        while (true)
        {
            ListT arrived;

            if (scan(m_localBox))
            {
                return true;
            }
            FiberScheduler.yield();
            synchronized (m_lock)
            {
                while (m_sharedBox.empty)
                {
                    if (timedWait)
                    {
                        if (period <= Duration.zero || !m_putMsg.wait(period))
                            return false;
                    }
                    else
                    {
                        m_putMsg.wait();
                    }
                }
                arrived.put(m_sharedBox);
            }
            scope (exit) m_localBox.put(arrived);
            if (scan(arrived))
                return true;

            if (timedWait)
                period = limit - MonoTime.currTime;
        }
    }

    /*
     * Called on thread termination.
     *
     * This routine clears out message queues and sets a flag to reject
     * any future messages.
     */
    final void close()
    {
        synchronized (m_lock)
            m_closed = true;
        m_localBox.clear();
    }

private:

    alias ListT = List!(Message);

    ListT m_localBox;

    Mutex m_lock;
    Condition m_putMsg;
    ListT m_sharedBox;
    bool m_closed;
}


///
package struct List (T)
{
    struct Range
    {
        import std.exception : enforce;

        @property bool empty() const
        {
            return !m_prev.next;
        }

        @property ref T front()
        {
            enforce(m_prev.next, "invalid list node");
            return m_prev.next.val;
        }

        @property void front(T val)
        {
            enforce(m_prev.next, "invalid list node");
            m_prev.next.val = val;
        }

        void popFront()
        {
            enforce(m_prev.next, "invalid list node");
            m_prev = m_prev.next;
        }

        private this(Node* p)
        {
            m_prev = p;
        }

        private Node* m_prev;
    }

    void put(T val)
    {
        put(newNode(val));
    }

    void put(ref List!(T) rhs)
    {
        if (!rhs.empty)
        {
            put(rhs.m_first);
            while (m_last.next !is null)
            {
                m_last = m_last.next;
                m_count++;
            }
            rhs.m_first = null;
            rhs.m_last = null;
            rhs.m_count = 0;
        }
    }

    Range opSlice()
    {
        return Range(cast(Node*)&m_first);
    }

    void removeAt(Range r)
    {
        import std.exception : enforce;

        assert(m_count, "Can not remove from empty Range");
        Node* n = r.m_prev;
        enforce(n && n.next, "attempting to remove invalid list node");

        if (m_last is m_first)
            m_last = null;
        else if (m_last is n.next)
            m_last = n; // nocoverage
        Node* to_free = n.next;
        n.next = n.next.next;
        freeNode(to_free);
        m_count--;
    }

    @property size_t length()
    {
        return m_count;
    }

    void clear()
    {
        m_first = m_last = null;
        m_count = 0;
    }

    @property bool empty()
    {
        return m_first is null;
    }

private:
    struct Node
    {
        Node* next;
        T val;

        this(T v)
        {
            val = v;
        }
    }

    static shared struct SpinLock
    {
        void lock() { while (!cas(&locked, false, true)) { Thread.yield(); } }
        void unlock() { atomicStore!(MemoryOrder.rel)(locked, false); }
        bool locked;
    }

    static shared SpinLock sm_lock;
    static shared Node* sm_head;

    Node* newNode(T v)
    {
        Node* n;
        {
            sm_lock.lock();
            scope (exit) sm_lock.unlock();

            if (sm_head)
            {
                n = cast(Node*) sm_head;
                sm_head = sm_head.next;
            }
        }
        if (n)
        {
            import std.conv : emplace;
            emplace!Node(n, v);
        }
        else
        {
            n = new Node(v);
        }
        return n;
    }

    void freeNode(Node* n)
    {
        // destroy val to free any owned GC memory
        destroy(n.val);

        sm_lock.lock();
        scope (exit) sm_lock.unlock();

        auto sn = cast(shared(Node)*) n;
        sn.next = sm_head;
        sm_head = sn;
    }

    void put(Node* n)
    {
        m_count++;
        if (!empty)
        {
            m_last.next = n;
            m_last = n;
            return;
        }
        m_first = n;
        m_last = n;
    }

    Node* m_first;
    Node* m_last;
    size_t m_count;
}

// test ability to send shared arrays
@system unittest
{
    static shared int[] x = new shared(int)[1];
    auto tid = spawn((Tid self, Tid caller) {
        auto arr = self.receiveOnly!(shared(int)[]);
        arr[0] = 5;
        caller.send(true);
    }, thisTid);
    tid.send(x);
    auto self = thisTid();
    self.receiveOnly!(bool);
    assert(x[0] == 5);
}

/***********************************************************************

    A common interface for objects that can be used in `select()`

***********************************************************************/

public interface Selectable
{
    /***********************************************************************

        Try to read/write to the `Selectable` without blocking. If the
        operation would block, queue and link it with the `sel_state`

        Params:
            ptr = pointer to the data for the select operation
            sel_state = SelectState instace of the select call being executed
            sel_id = id of the select call being executed

    ***********************************************************************/

    void selectWrite (void* ptr, SelectState sel_state, int sel_id);

    /// Ditto
    void selectRead (void* ptr, SelectState sel_state, int sel_id);
}

/***********************************************************************

    An aggregate to hold neccessary information for a select operation

***********************************************************************/

public struct SelectEntry
{
    /// Reference to a Selectable object
    Selectable selectable;

    /// Pointer to the select data
    void* select_data;

    /***********************************************************************

        Default ctor

        Params:
            selectable = A selectable interface reference
            select_data = pointer to the data for the select operation

    ***********************************************************************/

    this (Selectable selectable, void* select_data) @safe pure nothrow @nogc
    {
        this.selectable = selectable;
        this.select_data = select_data;
    }
}

/// Consists of the id and result of the select operation that was completed
public alias SelectReturn = Tuple!(bool, "success", int, "id");

/***********************************************************************

    Block on multiple `Channel`s

    Only one operation is completed per `select()` call

    Params:
        read_list = List of `Channel`s to select for read operation
        write_list = List of `Channel`s to select for write operation

    Return:
        Returns success/failure status of the operation and the index
        of the `Channel` that the operation was carried on. The index is
        the position of the SelectEntry in `read_list ~ write_list`, ie
        concatenated lists.

***********************************************************************/

public SelectReturn select (ref SelectEntry[] read_list, ref SelectEntry[] write_list, Duration timeout = Duration.init)
{
    import std.random : randomShuffle;

    auto ss = new SelectState();
    int sel_id = 0;
    thisScheduler().addResource(ss);
    scope (exit) thisScheduler().removeResource(ss);

    read_list = read_list.randomShuffle();
    write_list = write_list.randomShuffle();

    foreach(ref entry; read_list)
    {
        entry.selectable.selectRead(entry.select_data, ss, sel_id++);
    }

    foreach(ref entry; write_list)
    {
        entry.selectable.selectWrite(entry.select_data, ss, sel_id++);
    }

    if (!ss.blocker.wait(timeout))
        return SelectReturn(false, -1); // Timed out

    return SelectReturn(ss.success, ss.id);
}

/***********************************************************************

    Holds the state of a group of `selectRead`/`selectWrite` calls.
    Synchronizes peers that will consume those calls, so that only one
    `selectRead`/`selectWrite` call is completed.

***********************************************************************/

final private class SelectState : FiberScheduler.Resource
{
    /// Shared blocker object for multiple ChannelQueueEntry objects
    FiberScheduler.FiberBlocker blocker;

    /***********************************************************************

        Default constructor

    ***********************************************************************/

    this () nothrow
    {
        this.blocker = thisScheduler().new FiberBlocker();
    }

    /***********************************************************************

        Tries to atomically consume a `SelectState` and sets `id` and `success`
        fields

        Param:
            id = ID of the `selectRead`/`selectWrite` call that is consuming
                 this `SelectState`
            success_in = Success/Failure of the select call

        Return:
            Returns true if `SelectState` was not already consumed, false otherwise

    ***********************************************************************/

    bool tryConsume (int id, bool success_in = true) nothrow
    {
        if (cas(&this.consumed, false, true))
        {
            this.id = id;
            this.success = success_in;
            return true;
        }
        return false;
    }

    /***********************************************************************

        Returns if `SelectState` is already consumed or not

    ***********************************************************************/

    bool isConsumed () nothrow
    {
        return atomicLoad(this.consumed);
    }

    /***********************************************************************

        Consume SelectState so that it is neutralized

    ***********************************************************************/

    void release () nothrow
    {
        this.tryConsume(-1, false);
    }

    /// ID of the select call that consumed this `SelectState`
    int id;

    /// Success/failure state of the select call with ID of `id`
    bool success;

private:
    /// Indicates if this `SelectState` is consumed or not
    shared(bool) consumed;
}

/***********************************************************************

    A golang style channel implementation with buffered and unbuffered
    operation modes

    Intended to be used between Fibers

    Param:
        T = Type of the messages carried accross the `Channel`. Currently
            all reference and values types are supported.

***********************************************************************/

final public class Channel (T) : Selectable
{
    /***********************************************************************

        Constructs a Channel

        Param:
            max_size = Maximum amount of T a Channel can buffer
                       (0 -> Unbuffered operation,
                        Positive integer -> Buffered operation)

    ***********************************************************************/

    this (ulong max_size = 0) nothrow
    {
        this.max_size = max_size;
        this.lock = new FiberMutex;
        if (max_size)
            this.buffer = new RingBuffer!T(max_size);
    }

    /***********************************************************************

        Write a message to the `Channel` with an optional timeout

        Unbuffered mode:

            If a reader is already blocked on the `Channel`, writer copies the
            message to reader's buffer and wakes up the reader by yielding

            If no reader is ready in the wait queue, writer appends itself
            to write wait queue and blocks

        Buffered mode:

            If a reader is already blocked on the `Channel`, writer copies the
            message to reader's buffer and wakes up the reader and returns
            immediately.

            If the buffer is not full writer puts the message in the `Channel`
            buffer and returns immediately

            If buffer is full writer appends itself to write wait queue and blocks

        If `Channel` is closed, it returns immediately with a failure,
        regardless of the operation mode

        Param:
            val = Message to write to `Channel`
            duration = Timeout duration

        Returns:
            Success/Failure - Fails when `Channel` is closed or timeout is reached.

    ***********************************************************************/

    bool write () (auto ref T val, Duration duration = Duration.init) nothrow
    {
        this.lock.lock_nothrow();

        bool success = tryWrite(val);

        if (!success && !this.closed)
        {
            ChannelQueueEntry q_ent = this.enqueueEntry(this.writeq, &val);
            thisScheduler().addResource(q_ent);
            scope (exit) thisScheduler().removeResource(q_ent);

            this.lock.unlock_nothrow();
            return q_ent.blocker.wait(duration) && q_ent.success;
        }

        this.lock.unlock_nothrow();
        return success;
    }

    /***********************************************************************

        Try to write a message to `Channel` without blocking

        tryWrite writes a message if it is possible to do so without blocking.

        If the tryWrite is being executed in a select call, it tries to consume the
        `caller_sel` with the given `caller_sel_id`. It only proceeds if it can
        successfully consume the `caller_sel`

        Param:
            val = Message to write to `Channel`
            caller_sel = SelectState instace of the select call being executed
            caller_sel_id = id of the select call being executed

        Returns:
            Success/Failure

    ***********************************************************************/

    private bool tryWrite () (auto ref T val, SelectState caller_sel = null, int caller_sel_id = 0) nothrow
    {
        if (this.closed)
        {
            if (caller_sel)
                caller_sel.tryConsume(caller_sel_id, false);
            return false;
        }

        if (ChannelQueueEntry readq_ent = this.dequeueEntry(this.readq, caller_sel, caller_sel_id))
        {
            *readq_ent.pVal = val;
            readq_ent.blocker.notify();
            return true;
        }

        if (this.max_size > 0 // this.max_size > 0 = buffered
                    && this.buffer.length < this.max_size
                    && (!caller_sel || caller_sel.tryConsume(caller_sel_id)))
        {
            this.buffer.insert(val);
            return true;
        }

        return false;
    }

    /***********************************************************************

        Try to write a message to Channel without blocking and if it fails,
        create a write queue entry using the given `sel_state` and `sel_id`

        Param:
            ptr = Message to write to channel
            sel_state = SelectState instace of the select call being executed
            sel_id = id of the select call being executed

    ***********************************************************************/

    void selectWrite (void* ptr, SelectState sel_state, int sel_id) nothrow
    {
        assert(ptr !is null);
        assert(sel_state !is null);
        T* val = cast(T*) ptr;

        this.lock.lock_nothrow();

        bool success = tryWrite(*val, sel_state, sel_id);

        if (!sel_state.isConsumed())
            this.enqueueEntry(this.writeq, val, sel_state, sel_id);

        if (success || this.closed || sel_state.id == -1)
        {
            this.lock.unlock_nothrow();
            sel_state.blocker.notify();
        }
        else
            this.lock.unlock_nothrow();
    }

    /***********************************************************************

        Read a message from the Channel with an optional timeout

        Unbuffered mode:

            If a writer is already blocked on the Channel, reader copies the
            value to `output` and wakes up the writer by yielding

            If no writer is ready in the wait queue, reader appends itself
            to read wait queue and blocks

            If channel is closed, it returns immediatly with a failure

        Buffered mode:

            If there are existing messages in the buffer, reader pops one off
            the buffer and returns immediatly with success, regardless of the
            Channel being closed or not

            If there are no messages in the buffer it behaves exactly like the
            unbuffered operation

        Param:
            output = Reference to output variable
            duration = Timeout duration

        Returns:
            Success/Failure - Fails when channel is closed and there are
            no existing messages to be read. Fails when timeout is reached.

    ***********************************************************************/

    bool read (ref T output, Duration duration = Duration.init) nothrow
    {
        this.lock.lock_nothrow();

        bool success = tryRead(output);

        if (!success && !this.closed)
        {
            ChannelQueueEntry q_ent = this.enqueueEntry(this.readq, &output);
            thisScheduler().addResource(q_ent);
            scope (exit) thisScheduler().removeResource(q_ent);

            this.lock.unlock_nothrow();
            return q_ent.blocker.wait(duration) && q_ent.success;
        }

        this.lock.unlock_nothrow();
        return success;
    }

    /***********************************************************************

        Try to read a message from Channel without blocking

        tryRead reads a message if it is possible to do so without blocking.

        If the tryRead is being executed in a select call, it tries to consume the
        `caller_sel` with the given `caller_sel_id`. It only proceeds if it can
        successfully consume the `caller_sel`

        Param:
            output = Field to write the message to
            caller_sel = SelectState instace of the select call being executed
            caller_sel_id = id of the select call being executed\

        Returns:
            Success/Failure

    ***********************************************************************/

    private bool tryRead (ref T output, SelectState caller_sel = null, int caller_sel_id = 0) nothrow
    {
        ChannelQueueEntry write_ent = this.dequeueEntry(this.writeq, caller_sel, caller_sel_id);

        if (this.max_size > 0 && !this.buffer.empty())
        {
            // if dequeueEntry fails, we will try to consume caller_sel again.
            if (!caller_sel || write_ent || caller_sel.tryConsume(caller_sel_id))
            {
                output = this.buffer.front();
                this.buffer.popFront();

                if (write_ent)
                {
                    this.buffer.insert(*write_ent.pVal);
                }
            }
            else
            {
                return false;
            }
        }
        // if dequeueEntry returns a valid entry, it always successfully consumes the related select states.
        // the race between 2 select calls is resolved in dequeueEntry.
        else if (write_ent)
        {
            output = *write_ent.pVal;
        }
        else
        {
            if (this.closed && caller_sel)
                caller_sel.tryConsume(caller_sel_id, false);
            return false;
        }

        if (write_ent)
            write_ent.blocker.notify();
        return true;
    }

    /***********************************************************************

        Try to read a message from Channel without blocking and if it fails,
        create a read queue entry using the given `sel_state` and `sel_id`

        Param:
            ptr = Buffer to write the message to
            sel_state = SelectState instace of the select call being executed
            sel_id = id of the select call being executed

    ***********************************************************************/

    void selectRead (void* ptr, SelectState sel_state, int sel_id) nothrow
    {
        assert(ptr !is null);
        assert(sel_state !is null);
        T* val = cast(T*) ptr;

        this.lock.lock_nothrow();

        bool success = tryRead(*val, sel_state, sel_id);

        if (!sel_state.isConsumed())
            this.enqueueEntry(this.readq, val, sel_state, sel_id);

        if (success || this.closed || sel_state.id == -1)
        {
            this.lock.unlock_nothrow();
            sel_state.blocker.notify();
        }
        else
            this.lock.unlock_nothrow();
    }

    /***********************************************************************

        Close the channel

        Closes the channel by marking it closed and flushing all the wait
        queues

    ***********************************************************************/

    void close () nothrow
    {
        this.lock.lock_nothrow();
        if (!this.closed)
        {
            this.closed = true;

            // Wake blocked Fibers up, report the failure
            foreach (ref entry; this.readq)
            {
                entry.terminate();
            }
            foreach (ref entry; this.writeq)
            {
                entry.terminate();
            }

            this.readq.clear();
            this.writeq.clear();
        }
        this.lock.unlock_nothrow();
    }

    /***********************************************************************

        Return the length of the internal buffer

    ***********************************************************************/

    size_t length () nothrow
    {
        this.lock.lock_nothrow();
        scope (exit) this.lock.unlock_nothrow();
        return this.buffer.length;
    }

    /***********************************************************************

        Return the closed status of the `Channel`

    ***********************************************************************/

    bool isClosed () nothrow
    {
        this.lock.lock_nothrow();
        scope (exit) this.lock.unlock_nothrow();
        return this.closed;
    }

    /***********************************************************************

        An aggrate of neccessary information to block a Fiber and record
        their request

    ***********************************************************************/

    private class ChannelQueueEntry : FiberScheduler.Resource
    {
        /// FiberBlocker blocking the `Fiber`
        FiberScheduler.FiberBlocker blocker;

        /// Pointer to the variable that we will read to/from
        T* pVal;

        /// Result of the blocking read/write call
        bool success = true;

        /// State of the select call that this entry was created for
        SelectState select_state;

        /// Id of the select call that this entry was created for
        int sel_id;

        this (T* pVal, SelectState select_state = null, int sel_id = 0) nothrow
        {
            this.pVal = pVal;
            this.select_state = select_state;
            this.sel_id = sel_id;

            if (this.select_state)
                this.blocker = this.select_state.blocker;
            else
                this.blocker = thisScheduler().new FiberBlocker();
        }

        /***********************************************************************

            Terminate a `ChannelQueueEntry` by waking up the blocked Fiber
            and reporting the failure

            This is called on all the `ChannelQueueEntry` instances still in
            the wait queues when Channel is closed

        ***********************************************************************/

        void terminate () nothrow
        {
            this.success = false;
            if (!this.select_state || this.select_state.tryConsume(this.sel_id, this.success))
                this.blocker.notify();
        }


        /***********************************************************************

            Terminate ChannelQueueEntry so that it is neutralized

        ***********************************************************************/

        void release() nothrow
        {
            if (this.blocker.stopTimer())
                this.pVal = null; // Sanitize pVal so that we can catch illegal accesses
        }
    }

    /***********************************************************************

        Create and enqueue a `ChannelQueueEntry` to the given entryq

        Param:
            entryq = Queue to append the new ChannelQueueEntry
            pVal =  Pointer to the message buffer
            sel_state = SelectState object to associate with the
                        newly created ChannelQueueEntry
            sel_id = id of the select call creating the new ChannelQueueEntry

        Return:
            newly created ChannelQueueEntry

    ***********************************************************************/

    private ChannelQueueEntry enqueueEntry (ref DList!ChannelQueueEntry entryq, T* pVal,
                                            SelectState sel_state = null, int sel_id = 0) nothrow
    {
        assert(pVal !is null);

        ChannelQueueEntry q_ent = new ChannelQueueEntry(pVal, sel_state, sel_id);
        entryq.insert(q_ent);

        return q_ent;
    }

    /***********************************************************************

        Dequeue a `ChannelQueueEntry` from the given `entryq`

        Walks the `entryq` until it finds a suitable entry or the queue
        empties. If `dequeueEntry` is called from a select, it tries to
        consume the `caller_sel` if the `peer_sel` is not currently consumed.
        If it fails to consume the `caller_sel`, returns with a failure.

        If selected queue entry is part of a select operation, it is also
        consumed. If it consumes `caller_sel` but `peer_sel` was already
        consumed whole select operation would fail and caller would need to try
        again. This should be a rare case, where the `peer_sel` gets consumed by
        someone else between the first if check which verifies that it is not
        consumed and the point we actually try to consume it.

        Param:
            entryq = Queue to append the new ChannelQueueEntry
            caller_sel = SelectState instace of the select call being executed
            caller_sel_id = id of the select call being executed

        Return:
            a valid ChannelQueueEntry or null

    ***********************************************************************/

    private ChannelQueueEntry dequeueEntry (ref DList!ChannelQueueEntry entryq,
                                            SelectState caller_sel = null, int caller_sel_id = 0) nothrow
    {
        while (!entryq.empty())
        {
            ChannelQueueEntry qent = entryq.front();
            auto peer_sel = qent.select_state;

            if ((!peer_sel || !peer_sel.isConsumed()) && qent.blocker.shouldBlock())
            {
                // If we are in a select call, try to consume the caller select
                // if we can't consume the caller select, no need to continue
                if (caller_sel && !caller_sel.tryConsume(caller_sel_id))
                {
                    return null;
                }

                // At this point, caller select is consumed.
                // Try to consume the peer select if it exists
                // If peer_sel was consumed by someone else, tough luck
                // In that case, whole select will fail since we consumed the caller_sel
                if ((!peer_sel || peer_sel.tryConsume(qent.sel_id)) && qent.blocker.stopTimer())
                {
                    entryq.removeFront();
                    return qent;
                }
                else if (caller_sel)
                {
                    // Mark caller_sel failed
                    caller_sel.id = -1;
                    caller_sel.success = false;
                    return null;
                }
            }

            entryq.removeFront();
        }

        return null;
    }

private:
    /// Internal data storage
    RingBuffer!T buffer;

    /// Closed flag
    bool closed;

    /// Per channel lock
    FiberMutex lock;

    /// List of fibers blocked on read()
    DList!ChannelQueueEntry readq;

    /// List of fibers blocked on write()
    DList!ChannelQueueEntry writeq;

public:
    /// Maximum amount of T a Channel can buffer
    immutable ulong max_size;
}

// Test non blocking operation
@system unittest
{
    string str = "DEADBEEF";
    string rcv_str;
    FiberScheduler scheduler = new FiberScheduler;
    auto chn = new Channel!string(2);

    scheduler.start({
        chn.write(str ~ " 1");
        assert(chn.length() == 1);
        chn.write(str ~ " 2");
        assert(chn.length() == 2);

        assert(chn.read(rcv_str));
        assert(rcv_str == str ~ " 1");
        assert(chn.length() == 1);

        chn.write(str ~ " 3");
        assert(chn.length() == 2);

        assert(chn.read(rcv_str));
        assert(rcv_str == str ~ " 2");
        assert(chn.length() == 1);

        assert(chn.read(rcv_str));
        assert(rcv_str == str ~ " 3");
        assert(chn.length() == 0);
    });
}

// Test unbuffered blocking operation with multiple receivers
// Receiver should read every message in the order they were sent
@system unittest
{
    FiberScheduler scheduler = new FiberScheduler;
    auto chn = new Channel!int();
    int n = 1000;
    long sum;

    scheduler.spawn(
        () {
            int val, prev;
            bool ret = chn.read(prev);
            sum += prev;
            assert(ret);

            while (chn.read(val))
            {
                sum += val;
                assert(ret);
                assert(prev < val);
                prev = val;
            }
        }
    );

    scheduler.spawn(
        () {
            int val, prev;
            bool ret = chn.read(prev);
            sum += prev;
            assert(ret);

            while (chn.read(val))
            {
                sum += val;
                assert(ret);
                assert(prev < val);
                prev = val;
            }
        }
    );

    scheduler.start(
        () {
            for (int i = 0; i <= n; i++)
            {
                assert(chn.write(i));
            }
            chn.close();

            assert(!chn.write(0));
        }
    );

    // Sum of [0..1000]
    assert(sum == n*(n+1)/2);
}

// Test that writer is not blocked until buffer is full and a read unblocks the writer
// Reader should be able to read remaining messages after chn is closed
@system unittest
{
    FiberScheduler scheduler = new FiberScheduler;
    auto chn = new Channel!int(5);

    scheduler.spawn(
        () {
            int val;
            assert(chn.max_size == chn.length);
            assert(chn.read(val));
            chn.close();
            // Read remaining messages after channel is closed
            for (int i = 0; i < chn.max_size; i++)
            {
                assert(chn.read(val));
            }
            // No more messages to read on closed chn
            assert(!chn.read(val));
        }
    );

    scheduler.start(
        () {
            for (int i = 0; i < chn.max_size; i++)
            {
                assert(chn.write(i));
            }
            assert(chn.max_size == chn.length);
            // Wait for read.
            assert(chn.write(42));
            // Reader already closed the channel
            assert(!chn.write(0));
        }
    );
}

@system unittest
{
    struct HyperLoopMessage
    {
        int id;
        MonoTime time;
    }

    FiberScheduler scheduler = new FiberScheduler;
    auto chn1 = new Channel!HyperLoopMessage();
    auto chn2 = new Channel!HyperLoopMessage();
    auto chn3 = new Channel!HyperLoopMessage();

    scheduler.spawn(
        () {
            HyperLoopMessage msg;

            for (int i = 0; i < 1000; ++i)
            {
                assert(chn2.read(msg));
                assert(msg.id % 3 == 1);
                msg.id++;
                msg.time = MonoTime.currTime;
                assert(chn3.write(msg));
            }
        }
    );

    scheduler.spawn(
        () {
            HyperLoopMessage msg;

            for (int i = 0; i < 1000; ++i)
            {
                assert(chn1.read(msg));
                assert(msg.id % 3 == 0);
                msg.id++;
                msg.time = MonoTime.currTime;
                assert(chn2.write(msg));
            }
        }
    );

    scheduler.start(
        () {
            HyperLoopMessage msg = {
                id : 0,
                time : MonoTime.currTime
            };

            for (int i = 0; i < 1000; ++i)
            {
                assert(chn1.write(msg));
                assert(chn3.read(msg));
                assert(msg.id % 3 == 2);
                msg.id++;
                msg.time = MonoTime.currTime;
            }
        }
    );
}

// Multiple writer threads writing to a buffered channel
// Reader should receive all messages
@system unittest
{
    FiberScheduler scheduler = new FiberScheduler;
    immutable int n = 5000;
    auto chn1 = new Channel!int(n/10);

    shared int sharedVal = 0;
    shared int writer_sum = 0;

    auto t1 = new InfoThread({
        FiberScheduler scheduler = new FiberScheduler();
        scheduler.start(
            () {
                int val = atomicOp!"+="(sharedVal, 1);
                while (chn1.write(val))
                {
                    atomicOp!"+="(writer_sum, val);
                    val = atomicOp!"+="(sharedVal, 1);
                }
            }
        );
    });
    t1.start();

    auto t2 = new InfoThread({
        FiberScheduler scheduler = new FiberScheduler();
        scheduler.start(
            () {
                int val = atomicOp!"+="(sharedVal, 1);
                while (chn1.write(val))
                {
                    atomicOp!"+="(writer_sum, val);
                    val = atomicOp!"+="(sharedVal, 1);
                }
            }
        );
    });
    t2.start();

    scheduler.start(
        () {
            int reader_sum, readVal, count;

            while(chn1.read(readVal))
            {
                reader_sum += readVal;
                if (count++ == n) chn1.close();
            }

            thread_joinAll();
            assert(reader_sum == writer_sum);
        }
    );
}

@system unittest
{
    FiberScheduler scheduler = new FiberScheduler;
    auto chn1 = new Channel!int();
    auto chn2 = new Channel!int(1);
    auto chn3 = new Channel!int();

    scheduler.spawn(
        () {
            chn1.write(42);
            chn1.close();
            chn3.write(37);
        }
    );

    scheduler.spawn(
        () {
            chn2.write(44);
            chn2.close();
        }
    );

    scheduler.start(
        () {
            bool[3] chn_closed;
            int[3] read_val;
            for (int i = 0; i < 5; ++i)
            {
                SelectEntry[] read_list;
                SelectEntry[] write_list;

                if (!chn_closed[0])
                    read_list ~= SelectEntry(chn1, &read_val[0]);
                if (!chn_closed[1])
                    read_list ~= SelectEntry(chn2, &read_val[1]);
                read_list ~= SelectEntry(chn3, &read_val[2]);

                auto select_return = select(read_list, write_list);

                if (!select_return.success)
                {
                    if (!chn_closed[0])  chn_closed[0] = true;
                    else if (!chn_closed[1])  chn_closed[1] = true;
                    else chn_closed[2] = true;
                }
            }
            assert(read_val[0] == 42 && read_val[1] == 44 && read_val[2] == 37);
            assert(chn_closed[0] && chn_closed[1] && !chn_closed[2]);
        }
    );
}

@system unittest
{
    FiberScheduler scheduler = new FiberScheduler;

    auto chn1 = new Channel!int(20);
    auto chn2 = new Channel!int();
    auto chn3 = new Channel!int(20);
    auto chn4 = new Channel!int();

    void thread_func (T) (ref T write_chn, ref T read_chn, int _tid)
    {
        FiberScheduler scheduler = new FiberScheduler;
        int read_val, write_val;
        int prev_read = -1;
        int n = 10000;

        scheduler.start(
            () {
                while(read_val < n || write_val <= n)
                {
                    int a;
                    SelectEntry[] read_list;
                    SelectEntry[] write_list;

                    if (write_val <= n)
                        write_list ~= SelectEntry(write_chn, &write_val);

                    if (read_val < n)
                        read_list ~= SelectEntry(read_chn, &read_val);

                    auto select_return = select(read_list, write_list);

                    if (select_return.success)
                    {
                        if (read_list.length > 0 && select_return.id == 0)
                        {
                            assert(prev_read + 1 == read_val);
                            prev_read = read_val;
                        }
                        else
                        {
                            write_val++;
                        }
                    }
                }
            }
        );
    }

    auto t1 = new InfoThread({
        thread_func(chn1, chn2, 0);
    });
    t1.start();

    auto t2 = new InfoThread({
        thread_func(chn2, chn3, 1);
    });
    t2.start();

    auto t3 = new InfoThread({
        thread_func(chn3, chn4, 2);
    });
    t3.start();

    thread_func(chn4, chn1, 3);

    thread_joinAll();
}

@system unittest
{
    FiberScheduler scheduler = new FiberScheduler;
    auto chn1 = new Channel!int();
    auto chn2 = new Channel!int();

    scheduler.spawn(
        () {
            FiberScheduler.yield();
            chn1.close();
        }
    );

    scheduler.spawn(
        () {
            FiberScheduler.yield();
            chn2.close();
        }
    );

    scheduler.spawn(
        () {
            for (int i = 0; i < 2; ++i)
            {
                int write_val = 42;
                SelectEntry[] read_list;
                SelectEntry[] write_list;
                write_list ~= SelectEntry(chn1, &write_val);
                auto select_return = select(read_list, write_list);

                assert(select_return.id == 0);
                assert(!select_return.success);
            }
        }
    );

    scheduler.start(
        () {
            for (int i = 0; i < 2; ++i)
            {
                int read_val;
                SelectEntry[] read_list;
                SelectEntry[] write_list;
                read_list ~= SelectEntry(chn2, &read_val);
                auto select_return = select(read_list, write_list);

                assert(select_return.id == 0);
                assert(!select_return.success);
            }
        }
    );
}

@system unittest
{
    import core.sync.semaphore;
    FiberScheduler scheduler = new FiberScheduler;
    auto chn1 = new Channel!int();

    auto start = MonoTime.currTime;
    Semaphore writer_sem = new Semaphore();
    Semaphore reader_sem = new Semaphore();

    auto t1 = new Thread({
        FiberScheduler scheduler = new FiberScheduler;
        scheduler.start(
            () {
                writer_sem.wait();
                assert(chn1.write(42));
            }
        );
    });
    t1.start();

    auto t2 = new Thread({
        FiberScheduler scheduler = new FiberScheduler;
        scheduler.start(
            () {
                int read_val;
                reader_sem.wait();
                assert(chn1.read(read_val));
                assert(read_val == 43);
            }
        );
    });
    t2.start();

    scheduler.start(
        () {
            int read_val;

            scope (failure) {
                reader_sem.notify();
                writer_sem.notify();
                chn1.close();
            }

            assert(!chn1.read(read_val, 1000.msecs));
            assert(MonoTime.currTime - start >= 1000.msecs);

            writer_sem.notify();
            assert(chn1.read(read_val, 1000.msecs));
            assert(read_val == 42);

            start = MonoTime.currTime;

            assert(!chn1.write(read_val + 1, 1000.msecs));
            assert(MonoTime.currTime - start >= 1000.msecs);

            reader_sem.notify();
            assert(chn1.write(read_val + 1, 1000.msecs));
        }
    );

    thread_joinAll();
}

/// A simple spinlock
struct SpinLock
{
    /// Spin until lock is free
    void lock() nothrow
    {
        while (!cas(&locked, false, true)) { }
    }

    /// Atomically unlock
    void unlock() nothrow
    {
        atomicStore!(MemoryOrder.rel)(locked, false);
    }

    /// Lock state
    private shared(bool) locked;
}

/// A Fiber level Semaphore
class FiberSemaphore
{

    /***********************************************************************

        Ctor

        Param:
            count = Initial count of FiberSemaphore

    ************************************************************************/

    this (size_t count = 0) nothrow
    {
        this.count = count;
    }

    /***********************************************************************

        Wait for FiberSemaphore count to be greater than 0

    ************************************************************************/

    void wait () nothrow
    {
        this.slock.lock();

        if (this.count > 0)
        {
            this.count--;
            this.slock.unlock();
            return;
        }
        auto entry = new SemaphoreQueueEntry();
        thisScheduler().addResource(entry);
        scope (exit) thisScheduler().removeResource(entry);
        this.queue.insert(entry);

        this.slock.unlock();
        entry.blocker.wait();
    }

    /***********************************************************************

        Increment the FiberSemaphore count

    ************************************************************************/

    void notify () nothrow
    {
        this.slock.lock();
        if (auto entry = this.dequeueEntry())
            entry.blocker.notify();
        else
            this.count++;
        this.slock.unlock();
    }

    ///
    private class SemaphoreQueueEntry : FiberScheduler.Resource
    {

        ///
        this () nothrow
        {
            assert(thisScheduler(), "Can not block with no FiberScheduler running!");
            this.blocker = thisScheduler(). new FiberBlocker();
        }

        /***********************************************************************

            Terminate SemaphoreQueueEntry so that it is neutralized

        ************************************************************************/

        void release () nothrow
        {
            if (!blocker.stopTimer())
                this.outer.notify();
        }

        /// FiberBlocker blocking the `Fiber`
        FiberScheduler.FiberBlocker blocker;
    }

    /***********************************************************************

        Dequeue a `SemaphoreQueueEntry` from the waiting queue

        Return:
            a valid SemaphoreQueueEntry or null

    ***********************************************************************/

    private SemaphoreQueueEntry dequeueEntry () nothrow
    {
        while (!this.queue.empty())
        {
            auto entry = this.queue.front();
            this.queue.removeFront();
            if (entry.blocker.shouldBlock() && entry.blocker.stopTimer())
            {
                return entry;
            }
        }
        return null;
    }

    ///
    private SpinLock slock;

    /// Waiting queue for Fibers
    private DList!SemaphoreQueueEntry queue;

    /// Current semaphore count
    private size_t count;
}

/// A Fiber level Mutex, essentially a binary FiberSemaphore
class FiberMutex : FiberSemaphore
{
    this () nothrow
    {
        super(1);
    }

    ///
    alias lock = wait;

    ///
    alias unlock = notify;

    ///
    alias lock_nothrow = lock;

    ///
    alias unlock_nothrow = unlock;
}

// Test releasing a queue entry
@system unittest
{
    FiberMutex mtx = new FiberMutex();
    int sharedVal;

    auto t1 = new Thread({
        FiberScheduler scheduler = new FiberScheduler;
        scheduler.start(
            () {
                mtx.lock();
                Thread.sleep(400.msecs);
                sharedVal += 1;
                mtx.unlock();
            }
        );
    });
    t1.start();

    auto t2 = new Thread({
        FiberScheduler scheduler = new FiberScheduler;
        Thread.sleep(100.msecs);

        scheduler.spawn(
            () {
                Thread.sleep(200.msecs);
                throw new Exception("");
            }
        );

        try
        {
            scheduler.start(
                () {
                    mtx.lock();
                    sharedVal += 1;
                    mtx.unlock();
                }
            );
        } catch (Exception e) { }
    });
    t2.start();

    auto t3 = new Thread({
        FiberScheduler scheduler = new FiberScheduler;
        scheduler.start(
            () {
                Thread.sleep(200.msecs);
                mtx.lock();
                assert(sharedVal == 1);
                sharedVal += 1;
                mtx.unlock();
            }
        );
    });
    t3.start();

    thread_joinAll();
}
