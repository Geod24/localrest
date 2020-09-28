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
    methods, for some defined time or until unblocked, as well as trigger
    shutdowns or restart. See the methods for more details.
    The `withTimeout` control method can be used to spawn a scoped copy
    of the RemoteAPI with a custom configured timeout. The user-provided
    delegate will be called with this scoped copy that uses the new timeout.

    Shutdown:
    The control interface has a shutdown method that can be used to terminate
    a node gracefully. When the shutdown request is handled by the node,
    the event loop will exit and the thread will terminate. While the destructor
    of the node will be called, it might not usable for some actions, for example
    because D destructors may not allocate GC memory, or one may want
    to perform some test-specific operations, such a logging some data in case of failure.
    Therefore, you may provide a shutdown routine in the call to `shutdown`.
    It must accept a single argument of the interface type, and will be called
    with the implementation object just before the node is destroyed.
    If this routine throws, LocalRest will log the error in the console and
    proceed with destroying the stack-allocated node.
    Note that control requests are asynchronous, hence requests from the node
    might be processed / send by the node until the request is actually processed.
    There is also a `restart` method which accepts the same callback argument.

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
    This dependency is however not enforced by the dub project file,
    as users can provide their own serializer (see `geod24.Serialization`).
    If the default parameter for serialization is never used,
    one's project need not depend on `vibe-d:data`.

    Author:         Mathias 'Geod24' Lang
    License:        MIT (See LICENSE.txt)
    Copyright:      Copyright (c) 2018-2019 Mathias Lang. All rights reserved.

*******************************************************************************/

module geod24.LocalRest;

static import C = geod24.concurrency;
import geod24.Serialization;
import std.format;
import std.meta : AliasSeq;
import std.traits : fullyQualifiedName, Parameters, ReturnType;

import core.thread;
import core.time;

/// Request / Response ID
private struct ID
{
    /// A node may restart, in which case it will spawn a new request scheduler
    size_t sched_id;
    /// In order to support re-entrancy, every request contains an id
    /// which should be copied in the `Response`
    /// Initialized to `size_t.max` so not setting it crashes the program
    size_t id = size_t.max;
}

/// Data sent by the caller
private struct Command
{
    /// Tid of the sender thread (cannot be JSON serialized)
    C.Tid sender;
    /// ID used for request re-entrancy (See ID definition)
    ID id;
    /// Method to call
    string method;
    /// Serialized arguments to the method
    SerializedData args;
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
private struct ShutdownCommand (API)
{
    /// Any callback to call before the Node's destructor is called
    void function (API) callback;

    /// Whether we're restarting or really shutting down
    bool restart;
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

    /// The request failed to to a client error (4xx style error code)
    ClientFailed,

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
    /// ID used for request re-entrancy (See ID definition)
    ID id;
    /// If `status == Status.Success`, the serialized return value.
    /// Otherwise, it contains `Exception.toString()`.
    SerializedData data;
}

/// Thrown when the sent request is faulty (e.g. 4xx HTTP error types)
public class ClientException : Exception
{
    /// Constructor
    public this (string msg,
        string file = __FILE__, int line = __LINE__, Exception next = null)
        @safe pure nothrow
    {
        super(msg, file, line, next);
    }
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

    /// Just a FiberBinarySemaphore with a state
    private struct Waiting { FiberBinarySemaphore s; bool busy; }

    /// The 'Response' we are currently processing, if any
    private Response pending;

    /// Request IDs waiting for a response
    private Waiting[ID] waiting;

    /// scheduler ID
    private size_t sched_id;

    /// Initialize this scheduler and its unique ID
    public this () @safe @nogc nothrow
    {
        static size_t last_idx;
        this.sched_id = last_idx++;
    }

    /// Get the next available request ID
    public ID getNextResponseId ()
    {
        static size_t last_idx;
        return ID(this.sched_id, last_idx++);
    }

    public Response waitResponse (ID id, Duration duration) nothrow
    {
        if (id !in this.waiting)
            this.waiting[id] = Waiting(new FiberBinarySemaphore, false);

        Waiting* ptr = &this.waiting[id];
        if (ptr.busy)
            assert(0, "Trying to override a pending request");

        // We yield and wait for an answer
        ptr.busy = true;

        if (duration == Duration.init)
            ptr.s.wait();
        else if (!ptr.s.wait(duration))
            this.pending = Response(Status.Timeout, this.pending.id);

        ptr.busy = false;
        // After control returns to us, `pending` has been filled
        scope(exit) this.pending = Response.init;
        return this.pending;
    }

    /// Called when a waiting condition was handled and can be safely removed
    public void remove (ID id)
    {
        this.waiting.remove(id);
    }
}


/// We need a scheduler to simulate an event loop and to be re-entrant
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

public void runTask (void delegate() dg) nothrow
{
    assert(scheduler !is null, "Cannot call this function from the main thread");
    scheduler.spawn(dg);
}

/// Ditto
public void sleep (Duration timeout) nothrow
{
    assert(scheduler !is null, "Cannot call this function from the main thread");
    scope sem = scheduler.new FiberBinarySemaphore();
    sem.wait(timeout);
}

/*******************************************************************************

    Run an asynchronous task after a given time.

    The task will first run after the given `timeout`, and
    can either repeat or run only once (the default).
    Works similarly to Vibe.d's `setTimer`.

    Params:
        timeout = Determines the minimum amount of time that elapses before
            the timer fires.
        dg = If non-null, this delegate will be called when the timer fires
        periodic = Speficies if the timer fires repeatedly or only once

    Returns:
        A `Timer` instance with the ability to control the timer

*******************************************************************************/

public Timer setTimer (Duration timeout, void delegate() dg,
    bool periodic = false) nothrow
{
    assert(scheduler !is null, "Cannot call this delegate from the main thread");
    assert(dg !is null, "Cannot call this delegate if null");

    Timer timer = new Timer(timeout, dg, periodic);
    scheduler.schedule(&timer.run);
    return timer;
}

/// Simple timer
public final class Timer
{
    private Duration timeout;
    private void delegate () dg;
    // Whether this timer is repeating
    private bool periodic;
    // Whether this timer was stopped
    private bool stopped;

    public this (Duration timeout, void delegate() dg, bool periodic) @safe nothrow
    {
        this.timeout = timeout;
        this.dg = dg;
        this.periodic = periodic;
        this.stopped = false;
    }

    // Run a delegate after timeout, and until this.periodic is false
    private void run ()
    {
        do
        {
            sleep(timeout);
            if (this.stopped)
                return;
            dg();
        } while (this.periodic);
    }

    /// Stop the timer. The next time this timer's fiber wakes up
    /// it will exit the run() function.
    public void stop () @safe nothrow
    {
        this.stopped = true;
        this.periodic = false;
    }
}

/*******************************************************************************

    A reference to an alread-instantiated node

    This class serves the same purpose as a `RestInterfaceClient`:
    it is a client for an already instantiated rest `API` interface.

    In order to instantiate a new server (in a remote thread), use the static
    `spawn` function.

    Serialization:
      In order to support custom serialization policy, one can change the
      `Serializer` parameter. This parameter is expected to be either a
      template or an aggregate with two static methods, but no explicit
      limitation is put on the type.
      See `geod24.Serialization`'s documentation for more informations.

    Params:
      API = The interface defining the API to implement
      S = An aggregate which follows the requirement explained above.

*******************************************************************************/

public final class RemoteAPI (API, alias S = VibeJSONSerializer!()) : API
{
    static assert (!serializerInvalidReason!(S).length, serializerInvalidReason!S);

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
          file = Path to the file that called this function (for diagnostic)
          line = Line number tied to the `file` parameter

        Returns:
          A `RemoteAPI` owning the node reference

    ***************************************************************************/

    public static RemoteAPI spawn (Impl) (
        CtorParams!Impl args, Duration timeout = Duration.init,
        string file = __FILE__, int line = __LINE__)
    {
        auto childTid = C.spawn(&spawned!(Impl), file, line, args);
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
        switch (cmd.method)
        {
            static foreach (member; __traits(allMembers, API))
            static foreach (ovrld; __traits(getOverloads, API, member))
            {
                mixin(
                q{
                    case `%2$s`:
                    Response res = Response(Status.Failed, cmd.id);

                    // Provide informative message in case of filtered method
                    if (cmd.method == filter.func_mangleof)
                        res.data = SerializedData(format("Filtered method '%%s'", filter.pretty_func));
                    else
                    {
                        auto args = S.deserialize!(ArgWrapper!(Parameters!ovrld))(
                            cmd.args.getS!S);

                        try
                        {
                            static if (!is(ReturnType!ovrld == void))
                                res.data = SerializedData(S.serialize(node.%1$s(args.args)));
                            else
                                node.%1$s(args.args);
                            res.status = Status.Success;
                        }
                        catch (Exception e)
                        {
                            res.status = Status.ClientFailed;
                            res.data = SerializedData(e.toString());
                        }
                    }

                    C.trySend(cmd.sender, res);
                    return;
                }.format(member, ovrld.mangleof));
            }
        default:
            C.trySend(cmd.sender,
                      Response(Status.ClientFailed, cmd.id,
                               SerializedData("Method not found")));
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
           self = The channel on which to "listen" to receive new "connections"
           file = Path to the file that spawned this node
           line = Line number in the `file` that spawned this node
           cargs = Arguments to `Implementation`'s constructor

    ***************************************************************************/

    private static void spawned (Implementation) (
        C.Tid self, string file, int line, CtorParams!Implementation cargs)
        nothrow
    {
        import std.datetime.systime : Clock, SysTime;
        import std.algorithm : each;
        import std.range;

        /// Simple exception unwind the stack when we need to terminate/restart
        static final class ExitException : Exception
        {
            bool restart;

            this () @safe pure nothrow @nogc
            {
                super("You should never see this exception - please report a bug");
            }
        }

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
        static struct Control
        {
            FilterAPI filter;    // filter specific messages
            SysTime sleep_until; // sleep until this time
            bool drop;           // drop messages if sleeping

            bool isSleeping() const
            {
                return this.sleep_until != SysTime.init
                    && Clock.currTime < this.sleep_until;
            }
        }

        scope exc = new ExitException();

        void runNode ()
        {
            scope node = new Implementation(cargs);
            scheduler = new LocalScheduler;

            // Control the node behavior
            Control control;

            // we need to keep track of messages which were ignored when
            // node.sleep() was used, and then handle each message in sequence.
            Variant[] await_msgs;

            void handle (T)(T arg)
            {
                static if (is(T == Command))
                {
                    scheduler.spawn(() => handleCommand(arg, node, control.filter));
                }
                else static if (is(T == Response))
                {
                    // response for a previous LocalScheduler instance
                    if (arg.id.sched_id != scheduler.sched_id)
                        return;

                    scheduler.pending = arg;
                    scheduler.waiting[arg.id].s.notify();
                    scheduler.remove(arg.id);
                }
                else static assert(0, "Unhandled type: " ~ T.stringof);
            }

            scheduler.start(() {
                while (1)
                {
                    C.receiveTimeout(self, 10.msecs,
                        (ShutdownCommand!API e)
                        {
                            if (e.callback !is null)
                                e.callback(node);
                            exc.restart = e.restart;
                            throw exc;
                        },
                        (TimeCommand s) {
                            control.sleep_until = Clock.currTime + s.dur;
                            control.drop = s.drop;
                        },
                        (FilterAPI filter_api) {
                            control.filter = filter_api;
                        },
                        (Response res) {
                            if (!control.isSleeping())
                                handle(res);
                            else if (!control.drop)
                                await_msgs ~= Variant(res);
                        },
                        (Command cmd)
                        {
                            if (!control.isSleeping())
                                handle(cmd);
                            else if (!control.drop)
                                await_msgs ~= Variant(cmd);
                        });

                    // now handle any leftover messages after any sleep() call
                    if (!control.isSleeping())
                    {
                        await_msgs.each!(msg => msg.tag == 0 ? handle(msg.res) : handle(msg.cmd));
                        await_msgs.length = 0;
                        assumeSafeAppend(await_msgs);
                    }
                }
            });
        }

        try
        {
            while (true)
            {
                try runNode();
                // We use this exception to exit the event loop
                catch (ExitException e)
                {
                    if (!e.restart)
                        break;
                }
            }
        }
        catch (Throwable t)
        {
            import core.stdc.stdio, std.stdio;
            printf("#### FATAL ERROR: %.*s\n", cast(int) t.msg.length, t.msg.ptr);
            printf("This node was started at %.*s:%d\n",
                   cast(int) file.length, file.ptr, line);
            printf("This most likely means that the node crashed due to an uncaught exception\n");
            printf("If not, please file a bug at https://github.com/Geod24/localrest/\n");

            try writeln("Full error: ", t);
            catch (Exception e) { /* Nothing more we can do at this point */ }
        }
    }

    /// Where to send message to
    private C.Tid childTid;

    /// Timeout to use when issuing requests
    private const Duration timeout;

    /***************************************************************************

        Create an instante of a client

        This connects to an already instantiated node.
        In order to instantiate a node, see the static `spawn` function.

        Params:
          tid = `geod24.concurrency.Tid` of the node.
                This can usually be obtained by `geod24.concurrency.locate`.
          timeout = any timeout to use

    ***************************************************************************/

    public this (C.Tid tid, Duration timeout = Duration.init)
        @safe @nogc pure nothrow
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

            Params:
                callback = if not null, the callback to call in the Node's
                           thread before the Node is destroyed. Can be used
                           for cleanup / logging routines.

        ***********************************************************************/

        public void shutdown (void function (API) callback = null)
            @trusted
        {
            C.send(this.childTid, ShutdownCommand!API(callback, false));
        }

        /***********************************************************************

            Send an async message to the thread to immediately restart.

            Note that further non-control messages to the node will block until
            the node is back "online".

            Params:
                callback = if not null, the callback to call in the Node's
                           thread before the Node is destroyed, but before
                           it is restarted. Can be used for cleanup or logging.

        ***********************************************************************/

        public void restart (void function (API) callback = null)
            @trusted
        {
            C.send(this.childTid, ShutdownCommand!API(callback, true));
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

        /***********************************************************************

            Call the provided delegate with a custom timeout

            This allow to perform requests on a client with a different timeout,
            usually to allow some requests (e.g. initialization calls) to have longer
            timeout, or no timeout at all, or to put a timeout on an otherwise
            timeout-less client (e.g. when calling the actual test which could fail).

            To disable timeout, pass the special value `Duration.zero`
            (or `0.seconds`, `0.msecs`, etc...).

            Params:
                timeout = the new timeout to use
                dg = the delegate to call with the new scoped RemoteAPI copy

        ***********************************************************************/

        public void withTimeout (Dg) (Duration timeout, scope Dg dg)
        {
            scope api = new RemoteAPI(this.childTid, timeout);
            static assert(is(typeof({ dg(api); })),
                          "Provided argument of type `" ~ Dg.stringof ~
                          "` is not callable with argument type `scope " ~
                          fullyQualifiedName!API ~ "`");
            dg(api);
        }
    }

    // Vibe.d mandates that method must be @safe
    @safe:

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
                        auto serialized = S.serialize(ArgWrapper!(Parameters!ovrld)(params));

                        auto command = Command(C.thisTid(), scheduler.getNextResponseId(), ovrld.mangleof,
                                               SerializedData(serialized));

                        // If the node already shut down, its MessageBox will be
                        // closed. Detect it and notify the user.
                        // Note that it might be expected that the remote died.
                        if (!C.trySend(this.childTid, command))
                            throw new Exception("Connection with peer closed");

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
                                            scheduler.waiting[res.id].s.notify();
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
                        throw new Exception(res.data.get!string);

                    if (res.status == Status.ClientFailed)
                        throw new ClientException(
                            format("Request to %s couldn't be processed : %s",
                                   __PRETTY_FUNCTION__, res.data.get!string));

                    if (res.status == Status.Timeout)
                        throw new Exception("Request timed-out");

                    static if (!is(ReturnType!(ovrld) == void))
                        return S.deserialize!(typeof(return))(res.data.getS!S());
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
        public string getValue (ulong idx);
        public ubyte[32] getQuorumSet ();
        public string recv (string data);
    }

    static class MockAPI : API
    {
        @safe:
        public override @property ulong pubkey ()
        { return 42; }
        public override string getValue (ulong idx)
        { assert(0); }
        public override ubyte[32] getQuorumSet ()
        { assert(0); }
        public override string recv (string data)
        { assert(0); }
    }

    scope test = RemoteAPI!API.spawn!MockAPI();
    assert(test.pubkey() == 42);
    test.ctrl.shutdown();
    thread_joinAll();
}

/// Example where a shutdown() routine must be called on a node before
/// its destructor is called
unittest
{
    __gshared bool dtor_called;
    __gshared bool shutdown_called;
    __gshared bool onDestroy_called;

    static interface API
    {
        @safe:
        public @property ulong pubkey ();
    }

    static class MockAPI : API
    {
        public override @property ulong pubkey () @safe
        { return 42; }
        public void shutdown () { shutdown_called = true; }
        ~this () { dtor_called = true; }
    }

    static void onDestroy (API node)
    {
        assert(!dtor_called);
        auto mock = cast(MockAPI)node;
        assert(mock !is null);
        mock.shutdown();
        onDestroy_called = true;
    }

    scope test = RemoteAPI!API.spawn!MockAPI();
    assert(test.pubkey() == 42);
    test.ctrl.shutdown(&onDestroy);
    thread_joinAll();
    // ctr.shutdown call is asynchronous
    assert(dtor_called);
    assert(onDestroy_called);
    assert(shutdown_called);
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
        public string getValue (ulong idx);
        public string recv (string data);
        public string recv (ulong index, string data);

        public string last ();
    }

    static class Node : API
    {
        @safe:
        public this (bool isByzantine) { this.isByzantine = isByzantine; }
        public override @property ulong pubkey ()
        { lastCall = `pubkey`; return this.isByzantine ? 0 : 42; }
        public override string getValue (ulong idx)
        { lastCall = `getValue`; return null; }
        public override string recv (string data)
        { lastCall = `recv@1`; return null; }
        public override string recv (ulong index, string data)
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

    static void testFunc()
    {
        auto node1 = factory("this does not matter", 1);
        auto node2 = factory("neither does this", 2);
        assert(node1.pubkey() == 42);
        assert(node1.last() == "pubkey");
        assert(node2.pubkey() == 0);
        assert(node2.last() == "pubkey");

        node1.recv(42, null);
        assert(node1.last() == "recv@2");
        node1.recv(null);
        assert(node1.last() == "recv@1");
        assert(node2.last() == "pubkey");
        node1.ctrl.shutdown();
        node2.ctrl.shutdown();
    }

    scope thread = new Thread(&testFunc);
    thread.start();
    // Make sure our main thread terminates after everyone else
    thread_joinAll();
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
    thread_joinAll();
}

/// Support for circular nodes call
unittest
{
    static import geod24.concurrency;

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
    thread_joinAll();
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
    thread_joinAll();
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
    thread_joinAll();
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
    n1.sleep(0.seconds, false);
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
    thread_joinAll();
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
    thread_joinAll();
}

// request timeouts (from main thread)
unittest
{
    import core.thread;
    import std.exception;

    static interface API
    {
        size_t sleepFor (long dur);
        void ping ();
    }

    static class Node : API
    {
        override size_t sleepFor (long dur)
        {
            Thread.sleep(msecs(dur));
            return 42;
        }

        override void ping () { }
    }

    // node with no timeout
    auto node = RemoteAPI!API.spawn!Node();
    assert(node.sleepFor(80) == 42);  // no timeout

    // custom timeout
    bool called;
    node.ctrl.withTimeout(100.msecs,
        (scope API api) {
            assertThrown!Exception(api.sleepFor(2000));
            called = true;
        });
    assert(called);

    called = false;
    struct S
    {
        void opCall (scope API api)
        {
            assertThrown!Exception(api.sleepFor(2000));
            called = true;
        }
    }
    S s;
    node.ctrl.withTimeout(100.msecs, s);
    assert(called);

    // Test that attributes are inferred based on the delegate
    void doTest () @safe pure nothrow @nogc
    {
        called = false;
        node.ctrl.withTimeout(Duration.zero,
                              (scope API api) { called = true; });
        assert(called);
    }
    doTest();

    // node with a configured timeout
    auto to_node = RemoteAPI!API.spawn!Node(500.msecs);

    /// none of these should time out
    assert(to_node.sleepFor(10) == 42);
    assert(to_node.sleepFor(20) == 42);
    assert(to_node.sleepFor(30) == 42);
    assert(to_node.sleepFor(40) == 42);

    assertThrown!Exception(to_node.sleepFor(2000));
    to_node.ctrl.withTimeout(3.seconds,  // wait for the node to wake up
          (scope API api) { api.ping(); });

    to_node.ctrl.shutdown();
    node.ctrl.shutdown();
    thread_joinAll();
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
    to_node.ctrl.withTimeout(3.seconds,  // wait for the node to wake up
      (scope API api) { assert(cast(int)api.getFloat() == 69); });
    to_node.ctrl.shutdown();
    node.ctrl.shutdown();
    thread_joinAll();
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
    thread_joinAll();
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
    thread_joinAll();
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
    thread_joinAll();
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
    thread_joinAll();
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
        assert(ex.msg == "Request timed-out");
    }
    thread_joinAll();
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

/// Example of a custom (de)serialization policy
unittest
{
    static struct Serialize
    {
    static:
        public immutable(ubyte[]) serialize (T) (auto ref T value) @trusted
        {
            static assert(is(typeof({ T v = immutable(T).init; })));
            static if (is(T : const(ubyte)[]))
                return value.idup;
            else
                return (cast(ubyte*)&value)[0 .. T.sizeof].idup;
        }

        public QT deserialize (QT) (immutable(ubyte)[] data) @trusted
        {
            return *cast(QT*)(data.dup.ptr);
        }
    }

    static struct ValueType
    {
        ulong v1;
        uint v2;
        uint v3;
    }

    static interface API
    {
        @safe:
        public @property ulong pubkey ();
        public ValueType getValue (string val);
        // Note: Vibe.d's JSON serializer cannot serialize this
        public immutable(ubyte[32]) getHash (const ubyte[] val);
    }

    static class MockAPI : API
    {
        @safe:
        public override @property ulong pubkey () { return 42; }
        public override ValueType getValue (string val) { return ValueType(val.length, 2, 3); }
        public override immutable(ubyte[32]) getHash (const ubyte[] val)
        {
            return val.length >= 32 ? val[0 .. 32] : typeof(return).init;
        }
    }

    scope test = RemoteAPI!(API, Serialize).spawn!MockAPI();
    assert(test.pubkey() == 42);
    assert(test.getValue("Hello world") == ValueType(11, 2, 3));
    ubyte[64] val = 42;
    assert(test.getHash(val) == val[0 .. 32]);
    test.ctrl.shutdown();
    thread_joinAll();
}

/// Test node2 responding to a dead node1
/// See https://github.com/Geod24/localrest/issues/64
unittest
{
    static interface API
    {
        @safe:
        // Main thread calls this on the first node
        public void call0 ();
        // ... which then calls this on the second node
        public void call1 ();
        public void call2 ();
    }

    __gshared C.Tid node1Addr;
    __gshared C.Tid node2Addr;

    static class Node : API
    {
        private RemoteAPI!API self;

        @trusted:
        // Main -> Node 1
        public override void call0 ()
        {
            this.self = new RemoteAPI!API(node1Addr);
            scope node2 = new RemoteAPI!API(node2Addr);
            node2.call1();
            assert(0, "This should never return as call2 shutdown this node");
        }

        // Node 1 -> Node 2
        public override void call1 ()
        {
            assert(this.self is null);
            scope node1 = new RemoteAPI!API(node1Addr);
            node1.call2();
            // Make really sure Node 1 is dead
            while (!node1Addr.mbox.isClosed())
                sleep(100.msecs);
        }

        // Node 2 -> Node 1
        public override void call2 ()
        {
            assert(this.self !is null);
            this.self.ctrl.shutdown();
        }
    }

    // Long timeout to ensure we don't spuriously pass
    auto node1 = RemoteAPI!API.spawn!Node(500.msecs);
    auto node2 = RemoteAPI!API.spawn!Node();
    node1Addr = node1.ctrl.tid();
    node2Addr = node2.ctrl.tid();

    // This will timeout (because the node will be gone)
    // However if something is wrong, either `joinall` will never return,
    // or the `assert(0)` in `call0` will be triggered.
    try
    {
        node1.call0();
        assert(0, "This should have timed out");
    }
    catch (Exception e) {}

    node2.ctrl.shutdown();
    thread_joinAll();
}

/// Test Timer
unittest
{
    static import core.thread;
    import core.time;

    static interface API
    {
        public void startTimer (bool periodic);
        public void stopTimer ();
        public ulong getCounter ();
        public void resetCounter ();
    }

    static class Node : API
    {
        private ulong counter;
        private Timer timer;

        public override void startTimer (bool periodic)
        {
            this.timer = setTimer(100.msecs, &callback, periodic);
        }

        public override void stopTimer ()
        {
            this.timer.stop();
        }

        public void callback ()
        {
            this.counter++;
            if (this.counter == 3)
                this.timer.stop();
        }

        public override ulong getCounter ()
        {
            scope (exit) this.counter = 0;
            return this.counter;
        }

        public override void resetCounter ()
        {
            this.counter = 0;
        }
    }

    auto node = RemoteAPI!API.spawn!Node();
    assert(node.getCounter() == 0);
    node.startTimer(true);
    core.thread.Thread.sleep(1.seconds);
    // The expected count is 3
    // Check means the timer repeated and the timer stoped
    assert(node.getCounter() == 3);
    node.resetCounter();
    node.startTimer(false);
    node.stopTimer();
    core.thread.Thread.sleep(500.msecs);
    assert(node.getCounter() == 0);
    node.ctrl.shutdown();
    thread_joinAll();
}

/// Test restarting a node
unittest
{
    static interface API
    {
        public uint[2] getCount () @safe;
    }

    static class Node : API
    {
        private static uint instantiationCount;
        private static uint destructionCount;

        this ()
        {
            Node.instantiationCount++;
        }

        ~this ()
        {
            Node.destructionCount++;
        }

        public override uint[2] getCount () const @safe
        {
            return [ Node.instantiationCount, Node.destructionCount, ];
        }
    }

    auto node = RemoteAPI!API.spawn!Node();
    assert(node.getCount == [1, 0]);
    node.ctrl.restart();
    assert(node.getCount == [2, 1]);
    node.ctrl.restart();
    assert(node.getCount == [3, 2]);
    node.ctrl.shutdown();
    thread_joinAll();
}

/// Test restarting a node that has responses waiting for it
unittest
{
    import core.atomic : atomicLoad, atomicStore;
    static interface API
    {
        @safe:
        public void call0 ();
        public void call1 ();
    }

    __gshared C.Tid node2Addr;
    static shared bool done;

    static class Node : API
    {
        @trusted:

        public override void call0 ()
        {
            scope node2 = new RemoteAPI!API(node2Addr);
            node2.call1();
        }

        public override void call1 ()
        {
            // when this event runs we know call1() has already returned
            scheduler.schedule({ atomicStore(done, true); });
        }
    }

    auto node1 = RemoteAPI!API.spawn!Node(500.msecs);
    auto node2 = RemoteAPI!API.spawn!Node();
    node2Addr = node2.ctrl.tid();
    node2.ctrl.sleep(2.seconds, false);

    try
    {
        node1.call0();
        assert(0, "This should have timed out");
    }
    catch (Exception e) {}

    node1.ctrl.restart();

    // after a while node 1 will receive a response to the timed-out request
    // to call1(), but the node restarted and is no longer interested in this
    // request (the request map / LocalScheduler is different), so it's filtered
    size_t count;
    while (!atomicLoad(done))
    {
        assert(count < 300);  // up to 3 seconds wait
        count++;
        Thread.sleep(10.msecs);
    }

    node1.ctrl.shutdown();
    node2.ctrl.shutdown();
    thread_joinAll();
}

unittest
{
    import geod24.concurrency;

    static interface API
    {
        public void start ();
        public int getValue ();
    }

    static class Node : API
    {
        int value;

        public override void start ()
        {
            // if this is a scoped delegate, it might not have a closure,
            // and when the task is resumed again it will segfault.
            // therefore runTask() must take a non-scope delegate.
            // note: once upstream issue #20868 is fixed, it would become
            // a compiler error to escape a scope delegate.
            runTask(
            {
                value = 1;
                FiberScheduler.yield();
                value = 2;
            });
        }

        public override int getValue () { return this.value; }
    }

    auto node = RemoteAPI!API.spawn!Node();
    node.start();
    assert(node.getValue() == 2);
    node.ctrl.shutdown();
    thread_joinAll();
}

/// Situation: Calling a node with an interface that doesn't exists
/// Expectation: The client throws an exception with a useful error message
/// This can happen by mistake (API mixup) or when a method is optional.
unittest
{
    import std.exception : assertThrown;

    static interface BaseAPI
    {
        public int required ();
    }

    static interface APIExtended : BaseAPI
    {
        public int optional ();
    }

    static class BaseNode : BaseAPI
    {
        public override int required () { return 42; }
    }

    auto node = RemoteAPI!BaseAPI.spawn!BaseNode();
    scope extnode = new RemoteAPI!APIExtended(node.ctrl.tid());
    assert(extnode.required() == 42);
    assertThrown!ClientException(extnode.optional());
    node.ctrl.shutdown();
    thread_joinAll();
}
