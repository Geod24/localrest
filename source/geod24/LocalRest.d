/*******************************************************************************

    Provides utilities to mock a network in unittests

    This module is based on the idea that D `interface`s can be used
    to represent a server's API, and that D `class` inheriting this `interface`
    are used to define the server's business code,
    abstracting away the communication layer.

    For example, a server that exposes an API to concatenate two strings would
    define the following code:
    ---
    interface API { public string concat (string a, string b); }
    class Server : API
    {
        public override string concat (string a, string b)
        {
            return a ~ b;
        }
    }
    ---

    Then one can use "generators" to define how multiple process communicate
    together. One such generator, that pioneered this design is `vibe.web.rest`,
    which allows to expose such servers as REST APIs.

    `localrest` is another generator, which uses message passing and threads
    to create a local "network".
    The motivation was to create a testing library that could be used to
    model a network at a much cheaper cost than spawning processes
    (and containers) would be, when doing integration tests.

    Control_Interface:
    When instantiating a `RemoteAPI`, one has the ability to call foreign
    implementations through auto-generated `override`s of the `interface`.
    In addition to that, as this library is intended for testing,
    a few extra functionalities are offered under a control interface,
    accessible under the `ctrl` namespace in the instance.
    The control interface allows to make the node unresponsive to one or all
    methods, for some defined time or until unblocked.
    See `sleep`, `filter`, and `clearFilter` for more details.

    Event_Loop:
    Server process usually needs to perform some action in an asynchronous way.
    Additionally, some actions needs to be completed at a semi-regular interval,
    for example based on a timer.
    For those use cases, a node should call `runTask` or `sleep`, respectively.
    Note that this has the same name (and purpose) as Vibe.d's core primitives.
    Users should only ever call Vibe's `runTask` / `sleep` with `vibe.web.rest`,
    or only call LocalRest's `runTask` / `sleep` with `RemoteAPI`.

    Implementation:
    In order for tests to simulate an asynchronous system accurately,
    multiple nodes need to be able to run concurrently and asynchronously.

    There are two common solutions to this, to use either fibers or threads.
    Fibers have the advantage of being simpler to implement and predictable.
    Threads have the advantage of more accurately describing an asynchronous
    system and thus have the ability to uncover more issues.

    When spawning a node, a thread is spawned, a node is instantiated with
    the provided arguments, and an event loop waits for messages sent
    to the Tid. Messages consist of the sender's Tid, the mangled name
    of the function to call (to support overloading) and the arguments,
    serialized as a JSON string.

    Note:
    While this module's original motivation was to test REST nodes,
    the only dependency to Vibe.d is actually to it's JSON module,
    as Vibe.d is the only available JSON module known to the author
    to provide an interface to deserialize composite types.

    Author:         Mathias 'Geod24' Lang
    License:        MIT (See LICENSE.txt)
    Copyright:      Copyright (c) 2018-2019 Mathias Lang. All rights reserved.

*******************************************************************************/

module geod24.LocalRest;

import vibe.data.json;

static import C = geod24.concurrency;
import std.meta : AliasSeq;
import std.traits : Parameters, ReturnType;

import core.thread;
import core.time;


/// Data sent by the caller
private struct Command
{
    /// Tid of the sender thread (cannot be JSON serialized)
    C.Tid sender;
    /// In order to support re-entrancy, every request contains an id
    /// which should be copied in the `Response`
    /// Initialized to `size_t.max` so not setting it crashes the program
    size_t id = size_t.max;
    /// Method to call
    string method;
    /// Arguments to the method, JSON formatted
    string args;
}

/// Ask the node to exhibit a certain behavior for a given time
private struct TimeCommand
{
    /// For how long our remote node apply this behavior
    Duration dur;
    /// Whether or not affected messages should be dropped
    bool drop = false;
}

/// Ask the node to shut down
private struct ShutdownCommand
{
}

/// Filter out requests before they reach a node
private struct FilterAPI
{
    /// the mangled symbol name of the function to filter
    string func_mangleof;

    /// used for debugging
    string pretty_func;
}

/// Status of a request
private enum Status
{
    /// Request failed
    Failed,

    /// Request timed-out
    Timeout,

    /// Request succeeded
    Success
}

/// Data sent by the callee back to the caller
private struct Response
{
    /// Final status of a request (failed, timeout, success, etc)
    Status status;
    /// In order to support re-entrancy, every request contains an id
    /// which should be copied in the `Response` so the scheduler can
    /// properly dispatch this event
    /// Initialized to `size_t.max` so not setting it crashes the program
    size_t id;
    /// If `status == Status.Success`, the JSON-serialized return value.
    /// Otherwise, it contains `Exception.toString()`.
    string data;
}

/// Simple wrapper to deal with tuples
/// Vibe.d might emit a pragma(msg) when T.length == 0
private struct ArgWrapper (T...)
{
    static if (T.length == 0)
        size_t dummy;
    T args;
}

/**
 * Copied from geod24.concurrency.FiberScheduler, increased the stack size to 16MB.
 */
class BaseFiberScheduler : C.Scheduler
{
    static class InfoFiber : Fiber
    {
        C.ThreadInfo info;

        this(void delegate() op) nothrow
        {
            super(op, 16 * 1024 * 1024);  // 16Mb
        }
    }


    /**
     * This creates a new Fiber for the supplied op and then starts the
     * dispatcher.
     */
    void start(void delegate() op)
    {
        create(op);
        dispatch();
    }

    /**
     * This created a new Fiber for the supplied op and adds it to the
     * dispatch list.
     */
    void spawn(void delegate() op) nothrow
    {
        create(op);
        yield();
    }

    /**
     * If the caller is a scheduled Fiber, this yields execution to another
     * scheduled Fiber.
     */
    void yield() nothrow
    {
        // NOTE: It's possible that we should test whether the calling Fiber
        //       is an InfoFiber before yielding, but I think it's reasonable
        //       that any (non-Generator) fiber should yield here.
        if (Fiber.getThis())
            Fiber.yield();
    }

    /**
     * Returns an appropriate ThreadInfo instance.
     *
     * Returns a ThreadInfo instance specific to the calling Fiber if the
     * Fiber was created by this dispatcher, otherwise it returns
     * ThreadInfo.thisInfo.
     */
    @property ref C.ThreadInfo thisInfo() nothrow
    {
        auto f = cast(InfoFiber) Fiber.getThis();

        if (f !is null)
            return f.info;
        return C.ThreadInfo.thisInfo;
    }

    /**
     * Returns a Condition analog that yields when wait or notify is called.
     */
    C.Condition newCondition(C.Mutex m) nothrow
    {
        return new FiberCondition(m);
    }

private:

    class FiberCondition : C.Condition
    {
        this(C.Mutex m) nothrow
        {
            super(m);
            notified = false;
        }

        override void wait() nothrow
        {
            scope (exit) notified = false;

            while (!notified)
                switchContext();
        }

        override bool wait(Duration period) nothrow
        {
            import core.time : MonoTime;

            scope (exit) notified = false;

            for (auto limit = MonoTime.currTime + period;
                 !notified && !period.isNegative;
                 period = limit - MonoTime.currTime)
            {
                yield();
            }
            return notified;
        }

        override void notify() nothrow
        {
            notified = true;
            switchContext();
        }

        override void notifyAll() nothrow
        {
            notified = true;
            switchContext();
        }

    private:
        void switchContext() nothrow
        {
            mutex_nothrow.unlock_nothrow();
            scope (exit) mutex_nothrow.lock_nothrow();
            yield();
        }

        private bool notified;
    }

private:
    void dispatch()
    {
        import std.algorithm.mutation : remove;

        while (m_fibers.length > 0)
        {
            auto t = m_fibers[m_pos].call(Fiber.Rethrow.no);
            if (t !is null && !(cast(C.OwnerTerminated) t))
            {
                throw t;
            }
            if (m_fibers[m_pos].state == Fiber.State.TERM)
            {
                if (m_pos >= (m_fibers = remove(m_fibers, m_pos)).length)
                    m_pos = 0;
            }
            else if (m_pos++ >= m_fibers.length - 1)
            {
                m_pos = 0;
            }
        }
    }

    void create(void delegate() op) nothrow
    {
        void wrap()
        {
            scope (exit)
            {
                thisInfo.cleanup();
            }
            op();
        }

        m_fibers ~= new InfoFiber(&wrap);
    }

private:
    Fiber[] m_fibers;
    size_t m_pos;
}

/// Our own little scheduler
private final class LocalScheduler : BaseFiberScheduler
{
    import core.sync.condition;
    import core.sync.mutex;

    /// Just a FiberCondition with a state
    private struct Waiting { FiberCondition c; bool busy; }

    /// The 'Response' we are currently processing, if any
    private Response pending;

    /// Request IDs waiting for a response
    private Waiting[ulong] waiting;

    /// Should never be called from outside
    public override Condition newCondition(Mutex m = null) nothrow
    {
        assert(0);
    }

    /// Get the next available request ID
    public size_t getNextResponseId ()
    {
        static size_t last_idx;
        return last_idx++;
    }

    public Response waitResponse (size_t id, Duration duration) nothrow
    {
        if (id !in this.waiting)
            this.waiting[id] = Waiting(new FiberCondition, false);

        Waiting* ptr = &this.waiting[id];
        if (ptr.busy)
            assert(0, "Trying to override a pending request");

        // We yield and wait for an answer
        ptr.busy = true;

        if (duration == Duration.init)
            ptr.c.wait();
        else if (!ptr.c.wait(duration))
            this.pending = Response(Status.Timeout, this.pending.id);

        ptr.busy = false;
        // After control returns to us, `pending` has been filled
        scope(exit) this.pending = Response.init;
        return this.pending;
    }

    /// Called when a waiting condition was handled and can be safely removed
    public void remove (size_t id)
    {
        this.waiting.remove(id);
    }

    /// Override `FiberScheduler.FiberCondition` to avoid mutexes
    /// and usage of global state
    private class FiberCondition : Condition
    {
        this() nothrow
        {
            super(null);
            notified = false;
        }

        override void wait() nothrow
        {
            scope (exit) notified = false;
            while (!notified)
                this.outer.yield();
        }

        override bool wait(Duration period) nothrow
        {
            scope (exit) notified = false;

            for (auto limit = MonoTime.currTime + period;
                 !notified && !period.isNegative;
                 period = limit - MonoTime.currTime)
            {
                this.outer.yield();
            }
            return notified;
        }

        override void notify() nothrow
        {
            notified = true;
            this.outer.yield();
        }

        override void notifyAll() nothrow
        {
            notified = true;
            this.outer.yield();
        }

        private bool notified;
    }
}


/// We need a scheduler to simulate an event loop and to be re-entrant
/// However, the one in `geod24.concurrency` is process-global (`__gshared`)
private LocalScheduler scheduler;

/// Whether this is the main thread
private bool is_main_thread;


/*******************************************************************************

    Provide eventloop-like functionalities

    Since nodes instantiated via this modules are Vibe.d server,
    they expect the ability to run an asynchronous task ,
    usually provided by `vibe.core.core : runTask`.

    In order for them to properly work, we need to integrate them to our event
    loop by providing the ability to spawn a task, and wait on some condition,
    optionally with a timeout.

    The following functions do that.
    Note that those facilities are not available from the main thread,
    while is supposed to do tests and doesn't have a scheduler.

*******************************************************************************/

public void runTask (scope void delegate() dg)
{
    assert(scheduler !is null, "Cannot call this function from the main thread");
    scheduler.spawn(dg);
}

/// Ditto
public void sleep (Duration timeout)
{
    assert(scheduler !is null, "Cannot call this function from the main thread");
    scope cond = scheduler.new FiberCondition();
    cond.wait(timeout);
}


/*******************************************************************************

    A reference to an alread-instantiated node

    This class serves the same purpose as a `RestInterfaceClient`:
    it is a client for an already instantiated rest `API` interface.

    In order to instantiate a new server (in a remote thread), use the static
    `spawn` function.

    Params:
      API = The interface defining the API to implement

*******************************************************************************/

public final class RemoteAPI (API) : API
{
    /***************************************************************************

        Instantiate a node and start it

        This is usually called from the main thread, which will start all the
        nodes and then start to process request.
        In order to have a connected network, no nodes in any thread should have
        a different reference to the same node.
        In practice, this means there should only be one `Tid` per "address".

        Note:
          When the `RemoteAPI` returned by this function is finalized,
          the child thread will be shut down.
          This ownership mechanism should be replaced with reference counting
          in a later version.

        Params:
          Impl = Type of the implementation to instantiate
          args = Arguments to the object's constructor
          timeout = (optional) timeout to use with requests

        Returns:
          A `RemoteAPI` owning the node reference

    ***************************************************************************/

    public static RemoteAPI!(API) spawn (Impl) (CtorParams!Impl args,
        Duration timeout = Duration.init)
    {
        auto childTid = C.spawn(&spawned!(Impl), args);
        return new RemoteAPI(childTid, true, timeout);
    }

    /// Helper template to get the constructor's parameters
    private static template CtorParams (Impl)
    {
        static if (is(typeof(Impl.__ctor)))
            private alias CtorParams = Parameters!(Impl.__ctor);
        else
            private alias CtorParams = AliasSeq!();
    }

    /***************************************************************************

        Handler function

        Performs the dispatch from `cmd` to the proper `node` function,
        provided the function is not filtered.

        Params:
            cmd    = the command to run (contains the method name and the arguments)
            node   = the node to invoke the method on
            filter = used for filtering API calls (returns default response)

    ***************************************************************************/

    private static void handleCommand (Command cmd, API node, FilterAPI filter)
    {
        import std.format;

        switch (cmd.method)
        {
            static foreach (member; __traits(allMembers, API))
            static foreach (ovrld; __traits(getOverloads, API, member))
            {
                mixin(
                q{
                    case `%2$s`:
                    try
                    {
                        if (cmd.method == filter.func_mangleof)
                        {
                            // we have to send back a message
                            import std.format;
                            C.send(cmd.sender, Response(Status.Failed, cmd.id,
                                format("Filtered method '%%s'", filter.pretty_func)));
                            return;
                        }

                        auto args = cmd.args.deserializeJson!(ArgWrapper!(Parameters!ovrld));

                        static if (!is(ReturnType!ovrld == void))
                        {
                            C.send(cmd.sender,
                                Response(
                                    Status.Success,
                                    cmd.id,
                                    node.%1$s(args.args).serializeToJsonString()));
                        }
                        else
                        {
                            node.%1$s(args.args);
                            C.send(cmd.sender, Response(Status.Success, cmd.id));
                        }
                    }
                    catch (Throwable t)
                    {
                        // Our sender expects a response
                        C.send(cmd.sender, Response(Status.Failed, cmd.id, t.toString()));
                    }

                    return;
                }.format(member, ovrld.mangleof));
            }
        default:
            assert(0, "Unmatched method name: " ~ cmd.method);
        }
    }

    /***************************************************************************

        Main dispatch function

       This function receive string-serialized messages from the calling thread,
       which is a struct with the sender's Tid, the method's mangleof,
       and the method's arguments as a tuple, serialized to a JSON string.

       `geod24.concurrency.receive` is not `@safe`, so neither is this.

       Params:
           Implementation = Type of the implementation to instantiate
           args = Arguments to `Implementation`'s constructor

    ***************************************************************************/

    private static void spawned (Implementation) (CtorParams!Implementation cargs)
    {
        import std.datetime.systime : Clock, SysTime;
        import std.algorithm : each;
        import std.range;

        scope node = new Implementation(cargs);
        scheduler = new LocalScheduler;
        scope exc = new Exception("You should never see this exception - please report a bug");

        // very simple & limited variant, to keep it performant.
        // should be replaced by a real Variant later
        static struct Variant
        {
            this (Response res) { this.res = res; this.tag = 0; }
            this (Command cmd) { this.cmd = cmd; this.tag = 1; }

            union
            {
                Response res;
                Command cmd;
            }

            ubyte tag;
        }

        // used for controling filtering / sleep
        struct Control
        {
            FilterAPI filter;    // filter specific messages
            SysTime sleep_until; // sleep until this time
            bool drop;           // drop messages if sleeping
        }

        Control control;

        bool isSleeping()
        {
            return control.sleep_until != SysTime.init
                && Clock.currTime < control.sleep_until;
        }

        void handle (T)(T arg)
        {
            static if (is(T == Command))
            {
                scheduler.spawn(() => handleCommand(arg, node, control.filter));
            }
            else static if (is(T == Response))
            {
                scheduler.pending = arg;
                scheduler.waiting[arg.id].c.notify();
                scheduler.remove(arg.id);
            }
            else static assert(0, "Unhandled type: " ~ T.stringof);
        }

        // we need to keep track of messages which were ignored when
        // node.sleep() was used, and then handle each message in sequence.
        Variant[] await_msgs;

        try scheduler.start(() {
                bool terminated = false;
                scheduler.spawn({
                    while (!terminated)
                    {
                        // now handle any leftover messages after any sleep() call
                        if (!isSleeping())
                        {
                            await_msgs.each!(msg => msg.tag == 0 ? handle(msg.res) : handle(msg.cmd));
                            await_msgs.length = 0;
                            assumeSafeAppend(await_msgs);
                        }
                        scheduler.yield();
                    }
                });
                while (!terminated)
                {
                    C.receiveTimeout(10.msecs,
                        (C.OwnerTerminated e) { terminated = true; },
                        (ShutdownCommand e) {
                            terminated = true;
                        },
                        (TimeCommand s)      {
                            control.sleep_until = Clock.currTime + s.dur;
                            control.drop = s.drop;
                        },
                        (FilterAPI filter_api) {
                            control.filter = filter_api;
                        },
                        (Response res) {
                            if (!isSleeping())
                                handle(res);
                            else if (!control.drop)
                                await_msgs ~= Variant(res);
                        },
                        (Command cmd)
                        {
                            if (!isSleeping())
                                handle(cmd);
                            else if (!control.drop)
                                await_msgs ~= Variant(cmd);
                        });

                        scheduler.yield();
                }
                // Make sure the scheduler is not waiting for polling tasks
                throw exc;
            });
        catch (Exception e)
            if (e !is exc)
                throw e;
    }

    /// Where to send message to
    private C.Tid childTid;

    /// Whether or not the destructor should destroy the thread
    private bool owner;

    /// Timeout to use when issuing requests
    private const Duration timeout;

    // Vibe.d mandates that method must be @safe
    @safe:

    /***************************************************************************

        Create an instante of a client

        This connects to an already instantiated node.
        In order to instantiate a node, see the static `spawn` function.

        Params:
          tid = `geod24.concurrency.Tid` of the node.
                This can usually be obtained by `geod24.concurrency.locate`.
          timeout = any timeout to use

    ***************************************************************************/

    public this (C.Tid tid, Duration timeout = Duration.init) @nogc pure nothrow
    {
        this(tid, false, timeout);
    }

    /// Private overload used by `spawn`
    private this (C.Tid tid, bool isOwner, Duration timeout) @nogc pure nothrow
    {
        this.childTid = tid;
        this.owner = isOwner;
        this.timeout = timeout;
        this.childTid.setTimeout(timeout);
    }

    /***************************************************************************

        Introduce a namespace to avoid name clashes

        The only way we have a name conflict is if someone exposes `ctrl`,
        in which case they will be served an error along the following line:
        LocalRest.d(...): Error: function `RemoteAPI!(...).ctrl` conflicts
        with mixin RemoteAPI!(...).ControlInterface!() at LocalRest.d(...)

    ***************************************************************************/

    public mixin ControlInterface!() ctrl;

    /// Ditto
    private mixin template ControlInterface ()
    {
        /***********************************************************************

            Returns the `Tid` this `RemoteAPI` wraps

            This can be useful for calling `geod24.concurrency.register` or similar.
            Note that the `Tid` should not be used directly, as our event loop,
            would error out on an unknown message.

        ***********************************************************************/

        public C.Tid tid () @nogc pure nothrow
        {
            return this.childTid;
        }

        /***********************************************************************

            Send an async message to the thread to immediately shut down.

        ***********************************************************************/

        public void shutdown () @trusted
        {
            C.send(this.childTid, ShutdownCommand());
            this.childTid.shutdown = true;
        }

        /***********************************************************************

            Make the remote node sleep for `Duration`

            The remote node will call `Thread.sleep`, becoming completely
            unresponsive, potentially having multiple tasks hanging.
            This is useful to simulate a delay or a network outage.

            Params:
              delay = Duration the node will sleep for
              dropMessages = Whether to process the pending requests when the
                             node come back online (the default), or to drop
                             pending traffic

        ***********************************************************************/

        public void sleep (Duration d, bool dropMessages = false) @trusted
        {
            C.send(this.childTid, TimeCommand(d, dropMessages));
        }

        /***********************************************************************

            Filter any requests issued to the provided method.

            Calling the API endpoint will throw an exception,
            therefore the request will fail.

            Use via:

            ----
            interface API { void call(); }
            class C : API { void call() { } }
            auto obj = new RemoteAPI!API(...);
            obj.filter!(API.call);
            ----

            To match a specific overload of a method, specify the
            parameters to match against in the call. For example:

            ----
            interface API { void call(int); void call(int, float); }
            class C : API { void call(int) {} void call(int, float) {} }
            auto obj = new RemoteAPI!API(...);
            obj.filter!(API.call, int, float);  // only filters the second overload
            ----

            Params:
              method = the API method for which to filter out requests
              OverloadParams = (optional) the parameters to match against
                  to select an overload. Note that if the method has no other
                  overloads, then even if that method takes parameters and
                  OverloadParams is empty, it will match that method
                  out of convenience.

        ***********************************************************************/

        public void filter (alias method, OverloadParams...) () @trusted
        {
            import std.format;
            import std.traits;
            enum method_name = __traits(identifier, method);

            // return the mangled name of the matching overload
            template getBestMatch (T...)
            {
                static if (is(Parameters!(T[0]) == OverloadParams))
                {
                    enum getBestMatch = T[0].mangleof;
                }
                else static if (T.length > 0)
                {
                    enum getBestMatch = getBestMatch!(T[1 .. $]);
                }
                else
                {
                    static assert(0,
                        format("Couldn't select best overload of '%s' for " ~
                        "parameter types: %s",
                        method_name, OverloadParams.stringof));
                }
            }

            // ensure it's used with API.method, *not* RemoteAPI.method which
            // is an override of API.method. Otherwise mangling won't match!
            // special-case: no other overloads, and parameter list is empty:
            // just select that one API method
            alias Overloads = __traits(getOverloads, API, method_name);
            static if (Overloads.length == 1 && OverloadParams.length == 0)
            {
                immutable pretty = method_name ~ Parameters!(Overloads[0]).stringof;
                enum mangled = Overloads[0].mangleof;
            }
            else
            {
                immutable pretty = format("%s%s", method_name, OverloadParams.stringof);
                enum mangled = getBestMatch!Overloads;
            }

            C.send(this.childTid, FilterAPI(mangled, pretty));
        }


        /***********************************************************************

            Clear out any filtering set by a call to filter()

        ***********************************************************************/

        public void clearFilter () @trusted
        {
            C.send(this.childTid, FilterAPI(""));
        }
    }

    /***************************************************************************

        Generate the API `override` which forward to the actual object

    ***************************************************************************/

    static foreach (member; __traits(allMembers, API))
        static foreach (ovrld; __traits(getOverloads, API, member))
        {
            mixin(q{
                override ReturnType!(ovrld) } ~ member ~ q{ (Parameters!ovrld params)
                {
                    // we are in the main thread
                    if (scheduler is null)
                    {
                        scheduler = new LocalScheduler;
                        is_main_thread = true;
                    }

                    if (this.childTid.shutdown)
                        throw new Exception(serializeToJsonString("Request timed-out"));

                    // `geod24.concurrency.send/receive[Only]` is not `@safe` but
                    // this overload needs to be
                    auto res = () @trusted {
                        auto serialized = ArgWrapper!(Parameters!ovrld)(params)
                            .serializeToJsonString();

                        auto command = Command(C.thisTid(), scheduler.getNextResponseId(), ovrld.mangleof, serialized);
                        C.send(this.childTid, command);

                        // for the main thread, we run the "event loop" until
                        // the request we're interested in receives a response.
                        if (is_main_thread)
                        {
                            bool terminated = false;
                            runTask(() {
                                while (!terminated)
                                {
                                    C.receiveTimeout(10.msecs,
                                        (Response res) {
                                            scheduler.pending = res;
                                            scheduler.waiting[res.id].c.notify();
                                        });

                                    scheduler.yield();
                                }
                            });

                            Response res;
                            scheduler.start(() {
                                res = scheduler.waitResponse(command.id, this.timeout);
                                terminated = true;
                            });
                            return res;
                        }
                        else
                        {
                            return scheduler.waitResponse(command.id, this.timeout);
                        }
                    }();

                    if (res.status == Status.Failed)
                        throw new Exception(res.data);

                    if (res.status == Status.Timeout)
                        throw new Exception(serializeToJsonString("Request timed-out"));

                    static if (!is(ReturnType!(ovrld) == void))
                        return res.data.deserializeJson!(typeof(return));
                }
                });
        }
}
