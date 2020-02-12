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

/// Our own little scheduler
private final class LocalScheduler : C.FiberScheduler
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
        return new RemoteAPI(childTid, timeout);
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

    private static void spawned (Implementation) (C.Tid self, CtorParams!Implementation cargs)
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
                while (1)
                {
                    C.receiveTimeout(C.thisTid(), 10.msecs,
                        (C.OwnerTerminated e) { throw exc; },
                        (ShutdownCommand e) { throw exc; },
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

                    // now handle any leftover messages after any sleep() call
                    if (!isSleeping())
                    {
                        await_msgs.each!(msg => msg.tag == 0 ? handle(msg.res) : handle(msg.cmd));
                        await_msgs.length = 0;
                        assumeSafeAppend(await_msgs);
                    }
                }
            });
        catch (Exception e)
            if (e !is exc)
                throw e;
    }

    /// Where to send message to
    private C.Tid childTid;

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
        this.childTid = tid;
        this.timeout = timeout;
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
                                    C.receiveTimeout(C.thisTid(), 10.msecs,
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

/// Simple usage example
unittest
{
    static interface API
    {
        @safe:
        public @property ulong pubkey ();
        public Json getValue (ulong idx);
        public Json getQuorumSet ();
        public string recv (Json data);
    }

    static class MockAPI : API
    {
        @safe:
        public override @property ulong pubkey ()
        { return 42; }
        public override Json getValue (ulong idx)
        { assert(0); }
        public override Json getQuorumSet ()
        { assert(0); }
        public override string recv (Json data)
        { assert(0); }
    }

    scope test = RemoteAPI!API.spawn!MockAPI();
    assert(test.pubkey() == 42);
    test.ctrl.shutdown();
}

/// In a real world usage, users will most likely need to use the registry
unittest
{
    import std.conv;
    static import geod24.concurrency;
    import geod24.Registry;

    __gshared Registry registry;
    registry.initialize();

    static interface API
    {
        @safe:
        public @property ulong pubkey ();
        public Json getValue (ulong idx);
        public string recv (Json data);
        public string recv (ulong index, Json data);

        public string last ();
    }

    static class Node : API
    {
        @safe:
        public this (bool isByzantine) { this.isByzantine = isByzantine; }
        public override @property ulong pubkey ()
        { lastCall = `pubkey`; return this.isByzantine ? 0 : 42; }
        public override Json getValue (ulong idx)
        { lastCall = `getValue`; return Json.init; }
        public override string recv (Json data)
        { lastCall = `recv@1`; return null; }
        public override string recv (ulong index, Json data)
        { lastCall = `recv@2`; return null; }

        public override string last () { return this.lastCall; }

        private bool isByzantine;
        private string lastCall;
    }

    static RemoteAPI!API factory (string type, ulong hash)
    {
        const name = hash.to!string;
        auto tid = registry.locate(name);
        if (tid != tid.init)
            return new RemoteAPI!API(tid);

        switch (type)
        {
        case "normal":
            auto ret =  RemoteAPI!API.spawn!Node(false);
            registry.register(name, ret.tid());
            return ret;
        case "byzantine":
            auto ret =  RemoteAPI!API.spawn!Node(true);
            registry.register(name, ret.tid());
            return ret;
        default:
            assert(0, type);
        }
    }

    auto node1 = factory("normal", 1);
    auto node2 = factory("byzantine", 2);

    static void testFunc(geod24.concurrency.Tid self, geod24.concurrency.Tid parent)
    {
        auto node1 = factory("this does not matter", 1);
        auto node2 = factory("neither does this", 2);
        assert(node1.pubkey() == 42);
        assert(node1.last() == "pubkey");
        assert(node2.pubkey() == 0);
        assert(node2.last() == "pubkey");

        node1.recv(42, Json.init);
        assert(node1.last() == "recv@2");
        node1.recv(Json.init);
        assert(node1.last() == "recv@1");
        assert(node2.last() == "pubkey");
        node1.ctrl.shutdown();
        node2.ctrl.shutdown();
        geod24.concurrency.send(parent, 42);
    }

    auto self = geod24.concurrency.thisTid();
    auto testerFiber = geod24.concurrency.spawn(&testFunc, self);
    // Make sure our main thread terminates after everyone else
    geod24.concurrency.receiveOnly!int(self);
}

/// This network have different types of nodes in it
unittest
{
    import geod24.concurrency;

    static interface API
    {
        @safe:
        public @property ulong requests ();
        public @property ulong value ();
    }

    static class MasterNode : API
    {
        @safe:
        public override @property ulong requests()
        {
            return this.requests_;
        }

        public override @property ulong value()
        {
            this.requests_++;
            return 42; // Of course
        }

        private ulong requests_;
    }

    static class SlaveNode : API
    {
        @safe:
        this(Tid masterTid)
        {
            this.master = new RemoteAPI!API(masterTid);
        }

        public override @property ulong requests()
        {
            return this.requests_;
        }

        public override @property ulong value()
        {
            this.requests_++;
            return master.value();
        }

        private API master;
        private ulong requests_;
    }

    RemoteAPI!API[4] nodes;
    auto master = RemoteAPI!API.spawn!MasterNode();
    nodes[0] = master;
    nodes[1] = RemoteAPI!API.spawn!SlaveNode(master.tid());
    nodes[2] = RemoteAPI!API.spawn!SlaveNode(master.tid());
    nodes[3] = RemoteAPI!API.spawn!SlaveNode(master.tid());

    foreach (n; nodes)
    {
        assert(n.requests() == 0);
        assert(n.value() == 42);
    }

    assert(nodes[0].requests() == 4);

    foreach (n; nodes[1 .. $])
    {
        assert(n.value() == 42);
        assert(n.requests() == 2);
    }

    assert(nodes[0].requests() == 7);
    import std.algorithm;
    nodes.each!(node => node.ctrl.shutdown());
}

/// Support for circular nodes call
unittest
{
    static import geod24.concurrency;
    import std.format;

    __gshared C.Tid[string] tbn;

    static interface API
    {
        @safe:
        public ulong call (ulong count, ulong val);
        public void setNext (string name);
    }

    static class Node : API
    {
        @safe:
        public override ulong call (ulong count, ulong val)
        {
            if (!count)
                return val;
            return this.next.call(count - 1, val + count);
        }

        public override void setNext (string name) @trusted
        {
            this.next = new RemoteAPI!API(tbn[name]);
        }

        private API next;
    }

    RemoteAPI!(API)[3] nodes = [
        RemoteAPI!API.spawn!Node(),
        RemoteAPI!API.spawn!Node(),
        RemoteAPI!API.spawn!Node(),
    ];

    foreach (idx, ref api; nodes)
        tbn[format("node%d", idx)] = api.tid();
    nodes[0].setNext("node1");
    nodes[1].setNext("node2");
    nodes[2].setNext("node0");

    // 7 level of re-entrancy
    assert(210 == nodes[0].call(20, 0));

    import std.algorithm;
    nodes.each!(node => node.ctrl.shutdown());
}


/// Nodes can start tasks
unittest
{
    static import core.thread;
    import core.time;

    static interface API
    {
        public void start ();
        public ulong getCounter ();
    }

    static class Node : API
    {
        public override void start ()
        {
            runTask(&this.task);
        }

        public override ulong getCounter ()
        {
            scope (exit) this.counter = 0;
            return this.counter;
        }

        private void task ()
        {
            while (true)
            {
                this.counter++;
                sleep(50.msecs);
            }
        }

        private ulong counter;
    }

    import std.format;
    auto node = RemoteAPI!API.spawn!Node();
    assert(node.getCounter() == 0);
    node.start();
    assert(node.getCounter() == 1);
    assert(node.getCounter() == 0);
    core.thread.Thread.sleep(1.seconds);
    // It should be 19 but some machines are very slow
    // (e.g. Travis Mac testers) so be safe
    assert(node.getCounter() >= 9);
    assert(node.getCounter() == 0);
    node.ctrl.shutdown();
}

// Sane name insurance policy
unittest
{
    import geod24.concurrency : Tid;

    static interface API
    {
        public ulong tid ();
    }

    static class Node : API
    {
        public override ulong tid () { return 42; }
    }

    auto node = RemoteAPI!API.spawn!Node();
    assert(node.tid == 42);
    assert(node.ctrl.tid != Tid.init);

    static interface DoesntWork
    {
        public string ctrl ();
    }
    static assert(!is(typeof(RemoteAPI!DoesntWork)));
    node.ctrl.shutdown();
}

// Simulate temporary outage
unittest
{
    __gshared C.Tid n1tid;

    static interface API
    {
        public ulong call ();
        public void asyncCall ();
    }
    static class Node : API
    {
        public this()
        {
            if (n1tid != C.Tid.init)
                this.remote = new RemoteAPI!API(n1tid);
        }

        public override ulong call () { return ++this.count; }
        public override void  asyncCall () { runTask(() => cast(void)this.remote.call); }
        size_t count;
        RemoteAPI!API remote;
    }

    auto n1 = RemoteAPI!API.spawn!Node();
    n1tid = n1.tid();
    auto n2 = RemoteAPI!API.spawn!Node();

    /// Make sure calls are *relatively* efficient
    auto current1 = MonoTime.currTime();
    assert(1 == n1.call());
    assert(1 == n2.call());
    auto current2 = MonoTime.currTime();
    assert(current2 - current1 < 200.msecs);

    // Make one of the node sleep
    n1.sleep(1.seconds);
    // Make sure our main thread is not suspended,
    // nor is the second node
    assert(2 == n2.call());
    auto current3 = MonoTime.currTime();
    assert(current3 - current2 < 400.msecs);

    // Wait for n1 to unblock
    assert(2 == n1.call());
    // Check current time >= 1 second
    auto current4 = MonoTime.currTime();
    assert(current4 - current2 >= 1.seconds);

    // Now drop many messages
    n1.sleep(1.seconds, true);
    for (size_t i = 0; i < 500; i++)
        n2.asyncCall();
    // Make sure we don't end up blocked forever
    Thread.sleep(1.seconds);
    assert(3 == n1.call());

    // Debug output, uncomment if needed
    version (none)
    {
        import std.stdio;
        writeln("Two non-blocking calls: ", current2 - current1);
        writeln("Sleep + non-blocking call: ", current3 - current2);
        writeln("Delta since sleep: ", current4 - current2);
    }

    n1.ctrl.shutdown();
    n2.ctrl.shutdown();
}

// Filter commands
unittest
{
    __gshared C.Tid node_tid;

    static interface API
    {
        size_t fooCount();
        size_t fooIntCount();
        size_t barCount ();
        void foo ();
        void foo (int);
        void bar (int);  // not in any overload set
        void callBar (int);
        void callFoo ();
        void callFoo (int);
    }

    static class Node : API
    {
        size_t foo_count;
        size_t foo_int_count;
        size_t bar_count;
        RemoteAPI!API remote;

        public this()
        {
            this.remote = new RemoteAPI!API(node_tid);
        }

        override size_t fooCount() { return this.foo_count; }
        override size_t fooIntCount() { return this.foo_int_count; }
        override size_t barCount() { return this.bar_count; }
        override void foo () { ++this.foo_count; }
        override void foo (int) { ++this.foo_int_count; }
        override void bar (int) { ++this.bar_count; }  // not in any overload set
        // This one is part of the overload set of the node, but not of the API
        // It can't be accessed via API and can't be filtered out
        void bar(string) { assert(0); }

        override void callFoo()
        {
            try
            {
                this.remote.foo();
            }
            catch (Exception ex)
            {
                assert(ex.msg == "Filtered method 'foo()'");
            }
        }

        override void callFoo(int arg)
        {
            try
            {
                this.remote.foo(arg);
            }
            catch (Exception ex)
            {
                assert(ex.msg == "Filtered method 'foo(int)'");
            }
        }

        override void callBar(int arg)
        {
            try
            {
                this.remote.bar(arg);
            }
            catch (Exception ex)
            {
                assert(ex.msg == "Filtered method 'bar(int)'");
            }
        }
    }

    auto filtered = RemoteAPI!API.spawn!Node();
    node_tid = filtered.tid();

    // caller will call filtered
    auto caller = RemoteAPI!API.spawn!Node();
    caller.callFoo();
    assert(filtered.fooCount() == 1);

    // both of these work
    static assert(is(typeof(filtered.filter!(API.foo))));
    static assert(is(typeof(filtered.filter!(filtered.foo))));

    // only method in the overload set that takes a parameter,
    // should still match a call to filter with no parameters
    static assert(is(typeof(filtered.filter!(filtered.bar))));

    // wrong parameters => fail to compile
    static assert(!is(typeof(filtered.filter!(filtered.bar, float))));
    // Only `API` overload sets are considered
    static assert(!is(typeof(filtered.filter!(filtered.bar, string))));

    filtered.filter!(API.foo);

    caller.callFoo();
    assert(filtered.fooCount() == 1);  // it was not called!

    filtered.clearFilter();  // clear the filter
    caller.callFoo();
    assert(filtered.fooCount() == 2);  // it was called!

    // verify foo(int) works first
    caller.callFoo(1);
    assert(filtered.fooCount() == 2);
    assert(filtered.fooIntCount() == 1);  // first time called

    // now filter only the int overload
    filtered.filter!(API.foo, int);

    // make sure the parameterless overload is still not filtered
    caller.callFoo();
    assert(filtered.fooCount() == 3);  // updated

    caller.callFoo(1);
    assert(filtered.fooIntCount() == 1);  // call filtered!

    // not filtered yet
    caller.callBar(1);
    assert(filtered.barCount() == 1);

    filtered.filter!(filtered.bar);
    caller.callBar(1);
    assert(filtered.barCount() == 1);  // filtered!

    // last blocking calls, to ensure the previous calls complete
    filtered.clearFilter();
    caller.foo();
    caller.bar(1);

    filtered.ctrl.shutdown();
    caller.ctrl.shutdown();
}

// request timeouts (from main thread)
unittest
{
    import core.thread;
    import std.exception;

    static interface API
    {
        size_t sleepFor (long dur);
    }

    static class Node : API
    {
        override size_t sleepFor (long dur)
        {
            Thread.sleep(msecs(dur));
            return 42;
        }
    }

    // node with no timeout
    auto node = RemoteAPI!API.spawn!Node();
    assert(node.sleepFor(80) == 42);  // no timeout

    // node with a configured timeout
    auto to_node = RemoteAPI!API.spawn!Node(500.msecs);

    /// none of these should time out
    assert(to_node.sleepFor(10) == 42);
    assert(to_node.sleepFor(20) == 42);
    assert(to_node.sleepFor(30) == 42);
    assert(to_node.sleepFor(40) == 42);

    assertThrown!Exception(to_node.sleepFor(2000));
    Thread.sleep(2.seconds);  // need to wait for sleep() call to finish before calling .shutdown()
    to_node.ctrl.shutdown();
    node.ctrl.shutdown();
}

// test-case for responses to re-used requests (from main thread)
unittest
{
    import core.thread;
    import std.exception;

    static interface API
    {
        float getFloat();
        size_t sleepFor (long dur);
    }

    static class Node : API
    {
        override float getFloat() { return 69.69; }
        override size_t sleepFor (long dur)
        {
            Thread.sleep(msecs(dur));
            return 42;
        }
    }

    // node with no timeout
    auto node = RemoteAPI!API.spawn!Node();
    assert(node.sleepFor(80) == 42);  // no timeout

    // node with a configured timeout
    auto to_node = RemoteAPI!API.spawn!Node(500.msecs);

    /// none of these should time out
    assert(to_node.sleepFor(10) == 42);
    assert(to_node.sleepFor(20) == 42);
    assert(to_node.sleepFor(30) == 42);
    assert(to_node.sleepFor(40) == 42);

    assertThrown!Exception(to_node.sleepFor(2000));
    Thread.sleep(2.seconds);  // need to wait for sleep() call to finish before calling .shutdown()
    import std.stdio;
    assert(cast(int)to_node.getFloat() == 69);

    to_node.ctrl.shutdown();
    node.ctrl.shutdown();
}

// request timeouts (foreign node to another node)
unittest
{
    static import geod24.concurrency;
    import std.exception;

    __gshared C.Tid node_tid;

    static interface API
    {
        void check ();
        int ping ();
    }

    static class Node : API
    {
        override int ping () { return 42; }

        override void check ()
        {
            auto node = new RemoteAPI!API(node_tid, 500.msecs);

            // no time-out
            node.ctrl.sleep(10.msecs);
            assert(node.ping() == 42);

            // time-out
            node.ctrl.sleep(2000.msecs);
            assertThrown!Exception(node.ping());
        }
    }

    auto node_1 = RemoteAPI!API.spawn!Node();
    auto node_2 = RemoteAPI!API.spawn!Node();
    node_tid = node_2.tid;
    node_1.check();
    node_1.ctrl.shutdown();
    node_2.ctrl.shutdown();
}

// test-case for zombie responses
unittest
{
    static import geod24.concurrency;
    import std.exception;

    __gshared C.Tid node_tid;

    static interface API
    {
        void check ();
        int get42 ();
        int get69 ();
    }

    static class Node : API
    {
        override int get42 () { return 42; }
        override int get69 () { return 69; }

        override void check ()
        {
            auto node = new RemoteAPI!API(node_tid, 500.msecs);

            // time-out
            node.ctrl.sleep(2000.msecs);
            assertThrown!Exception(node.get42());

            // no time-out
            node.ctrl.sleep(10.msecs);
            assert(node.get69() == 69);
        }
    }

    auto node_1 = RemoteAPI!API.spawn!Node();
    auto node_2 = RemoteAPI!API.spawn!Node();
    node_tid = node_2.tid;
    node_1.check();
    node_1.ctrl.shutdown();
    node_2.ctrl.shutdown();
}

// request timeouts with dropped messages
unittest
{
    static import geod24.concurrency;
    import std.exception;

    __gshared C.Tid node_tid;

    static interface API
    {
        void check ();
        int ping ();
    }

    static class Node : API
    {
        override int ping () { return 42; }

        override void check ()
        {
            auto node = new RemoteAPI!API(node_tid, 420.msecs);

            // Requests are dropped, so it times out
            assert(node.ping() == 42);
            node.ctrl.sleep(10.msecs, true);
            assertThrown!Exception(node.ping());
        }
    }

    auto node_1 = RemoteAPI!API.spawn!Node();
    auto node_2 = RemoteAPI!API.spawn!Node();
    node_tid = node_2.tid;
    node_1.check();
    node_1.ctrl.shutdown();
    node_2.ctrl.shutdown();
}

// Test a node that gets a replay while it's delayed
unittest
{
    static import geod24.concurrency;
    import std.exception;

    __gshared C.Tid node_tid;

    static interface API
    {
        void check ();
        int ping ();
    }

    static class Node : API
    {
        override int ping () { return 42; }

        override void check ()
        {
            auto node = new RemoteAPI!API(node_tid, 5000.msecs);
            assert(node.ping() == 42);
            // We need to return immediately so that the main thread
            // puts us to sleep
            runTask(() {
                    node.ctrl.sleep(200.msecs);
                    assert(node.ping() == 42);
                });
        }
    }

    auto node_1 = RemoteAPI!API.spawn!Node(500.msecs);
    auto node_2 = RemoteAPI!API.spawn!Node();
    node_tid = node_2.tid;
    node_1.check();
    node_1.ctrl.sleep(300.msecs);
    assert(node_1.ping() == 42);
    node_1.ctrl.shutdown();
    node_2.ctrl.shutdown();
}

// Test explicit shutdown
unittest
{
    import std.exception;

    static interface API
    {
        int myping (int value);
    }

    static class Node : API
    {
        override int myping (int value)
        {
            return value;
        }
    }

    auto node = RemoteAPI!API.spawn!Node(1.seconds);
    assert(node.myping(42) == 42);
    node.ctrl.shutdown();

    try
    {
        node.myping(69);
        assert(0);
    }
    catch (Exception ex)
    {
        assert(ex.msg == `"Request timed-out"`);
    }
}

unittest
{
    import core.thread : thread_joinAll;
    static import geod24.concurrency;
    __gshared C.Tid node_tid;

    static interface API
    {
        void segfault ();
        void check ();
    }

    import std.stdio;

    static class Node : API
    {
        override void segfault ()
        {
            int* ptr; *ptr = 1;
        }

        override void check ()
        {
            auto node = new RemoteAPI!API(node_tid);

            // We need to return immediately so that the main thread can continue testing
            runTask(() {
                node.ctrl.sleep(500.msecs);
                node.segfault();
            });
        }
    }

    auto node_1 = RemoteAPI!API.spawn!Node();
    auto node_2 = RemoteAPI!API.spawn!Node();
    node_tid = node_2.tid;
    node_1.check();
    node_2.ctrl.shutdown();  // shut it down before wake-up, segfault() command will be ignored
    node_1.ctrl.shutdown();
    thread_joinAll();
}
