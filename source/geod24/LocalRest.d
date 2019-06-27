/*******************************************************************************

    Provides utilities to mock an async REST node in unittests

    Using `vibe.web.rest` allows to cleanly separate business code
    from network code, as implementing an interface is all that's
    needed to create a server.

    However, in order for tests to simulate an asynchronous system
    accurately, multiple nodes need to be able to run concurrently.

    There are two common solutions to this, to use either fibers or threads.
    Fibers have the advantage of being simpler to implement and predictable.
    Threads have the advantage of more accurately describing an asynchronous
    system and thus have the ability to uncover more issues.
    Fibers also need to cooperate (by yielding), which means the code must
    be more cautiously instrumented to allow it to be used for tests,
    while Threads will just work.

    The later reason is why this module went with Thread.
    When spawning a node, a thread is spawned, a node is instantiated with
    the provided arguments, and an event loop waits for messages sent
    to the Tid. Messages consist of the sender's Tid, the mangled name
    of the function to call (to support overloading) and the arguments,
    serialized as a JSON string.

    While this module's main motivation was to test REST nodes,
    the only dependency to Vibe.d is actually to it's JSON module,
    as Vibe.d is the only available JSON module known to the author
    to provide an interface to deserialize composite types.

    Author:         Mathias 'Geod24' Lang
    License:        MIT (See LICENSE.txt)
    Copyright:      Copyright (c) 2018-2019 Mathias Lang. All rights reserved.

*******************************************************************************/

module geod24.LocalRest;

import vibe.data.json;

static import C = std.concurrency;
import std.meta : AliasSeq;
import std.traits : Parameters, ReturnType;

/// Data sent by the caller
private struct Command
{
    /// Tid of the sender thread (cannot be JSON serialized)
    C.Tid sender;
    /// Method to call
    string method;
    /// Arguments to the method, JSON formatted
    string args;
}

/// Data sent by the callee back to the caller
private struct Response
{
    /// `true` if the method returned successfully,
    /// `false` if an `Exception` occured
    bool success;
    /// If `success == true`, the JSON-serialized return value.
    /// Otherwise, it contains `Exception.toString()`.
    string data;
}

/// Simple wrapper to deal with tuples
/// Vibe.d might emit a pragma(msg) when T.length == 0
private struct ArgWrapper (T...)
{
    T args;
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

        Returns:
          A `RemoteAPI` owning the node reference

    ***************************************************************************/

    public static RemoteAPI!(API) spawn (Impl) (CtorParams!Impl args)
    {
        auto childTid = C.spawn(&spawned!(Impl), args);
        return new RemoteAPI(childTid, true);
    }

    /// Helper template to get the constructor's parameters
    private static template CtorParams (Impl)
    {
        static if (is(typeof(Impl.__ctor)))
            private alias CtorParams = Parameters!(Impl.__ctor);
        else
            private alias CtorParams = AliasSeq!();
    }

    ///
    private static void handleCommand (Command cmd, API node)
    {
        import std.format;

        switch (cmd.method)
        {
            foreach (member; __traits(allMembers, API))
            foreach (ovrld; __traits(getOverloads, API, member))
            {
                mixin(
                q{
                    case `%2$s`:
                    alias Method = ovrld;
                    try
                    {
                        auto args = cmd.args.deserializeJson!(ArgWrapper!(Parameters!ovrld));

                        static if (!is(ReturnType!ovrld == void))
                        {
                            C.send(cmd.sender,
                                Response(
                                    true,
                                    node.%1$s(args.args).serializeToJsonString()));
                        }
                        else
                        {
                            node.%1$s(args.args);
                            C.send(cmd.sender, Response(true));
                        }
                    }
                    catch (Throwable t)
                    {
                        // Our sender expects a response
                        C.send(cmd.sender, Response(false, t.toString()));
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

       `std.concurrency.receive` is not `@safe`, so neither is this.

       Params:
           Implementation = Type of the implementation to instantiate
           args = Arguments to `Implementation`'s constructor

    ***************************************************************************/

    private static void spawned (Implementation) (CtorParams!Implementation cargs)
    {
        import std.format;

        bool terminated = false;
        scope node = new Implementation(cargs);

        while (!terminated)
        {
            C.receive((C.OwnerTerminated e) { terminated = true; },
                      (Command cmd)         { handleCommand(cmd, node); });
        }
    }

    /// Where to send message to
    private C.Tid childTid;

    /// Whether or not the destructor should destroy the thread
    private bool owner;

    // Vibe.d mandates that method must be @safe
    @safe:

    /***************************************************************************

        Create an instante of a client

        This connects to an already instantiated node.
        In order to instantiate a node, see the static `spawn` function.

        Params:
          tid = `std.concurrency.Tid` of the node.
                This can usually be obtained by `std.concurrency.locate`.

    ***************************************************************************/

    public this (C.Tid tid) @nogc pure nothrow
    {
        this(tid, false);
    }

    /// Private overload used by `spawn`
    private this (C.Tid tid, bool isOwner) @nogc pure nothrow
    {
        this.childTid = tid;
        this.owner = isOwner;
    }

    public C.Tid tid () @nogc pure nothrow
    {
        return this.childTid;
    }

    /***************************************************************************

        Generate the API `override` which forward to the actual object

    ***************************************************************************/

    mixin ForeachInst!(GenerateMember, null, __traits(allMembers, API));

    /// Predicate for ForeachInst
    private template GenerateMember(string name)
    {
        mixin ForeachInst!(GenerateOverload, name, __traits(getOverloads, API, name));
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
}

/// In a real world usage, users will most likely need to use the registry
unittest
{
    import std.conv;
    static import std.concurrency;

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

    static API factory (string type, ulong hash)
    {
        const name = hash.to!string;
        auto tid = std.concurrency.locate(name);
        if (tid != tid.init)
            return new RemoteAPI!API(tid);

        switch (type)
        {
        case "normal":
            auto ret =  RemoteAPI!API.spawn!Node(false);
            std.concurrency.register(name, ret.tid());
            return ret;
        case "byzantine":
            auto ret =  RemoteAPI!API.spawn!Node(true);
            std.concurrency.register(name, ret.tid());
            return ret;
        default:
            assert(0, type);
        }
    }

    auto node1 = factory("normal", 1);
    auto node2 = factory("byzantine", 2);

    static void testFunc(std.concurrency.Tid parent)
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
        std.concurrency.send(parent, 42);
    }

    auto testerFiber = std.concurrency.spawn(&testFunc, std.concurrency.thisTid);
    // Make sure our main thread terminates after everyone else
    std.concurrency.receiveOnly!int();
}

/// This network have different types of nodes in it
unittest
{
    import std.concurrency;

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

    API[4] nodes;
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
}


/*******************************************************************************

    Applies a template predicate to a list of arguments and merge the generated
    overload set.

    This template is explicitly design to generate function definitions within
    a scope and add them to an overload set.
    The use of `sym` is necessary because overload sets from mixed-in templates
    are not merged in the parent.

    Params:
        Pred = Predicate to generate one of more functions named `sym`
        sym  = Name of the symbol to merge into an overload set
        Args = Arguments to instantiate `Pred` with

*******************************************************************************/

private template ForeachInst (alias Pred, string sym, Args...)
{
    static if (Args.length >= 1)
    {
        mixin Pred!(Args[0]) A0;
        static if (sym.length)
            mixin("alias " ~ sym ~ " = A0." ~ sym ~ ";");
    }
    static if (Args.length >= 2)
    {
        mixin Pred!(Args[1]) A1;
        static if (sym.length)
            mixin("alias " ~ sym ~ " = A1." ~ sym ~ ";");
    }
    static if (Args.length >= 3)
    {
        mixin Pred!(Args[2]) A2;
        static if (sym.length)
            mixin("alias " ~ sym ~ " = A2." ~ sym ~ ";");
    }
    static if (Args.length >= 4)
    {
        mixin Pred!(Args[3]) A3;
        static if (sym.length)
            mixin("alias " ~ sym ~ " = A3." ~ sym ~ ";");
    }
    static if (Args.length >= 5)
    {
        mixin Pred!(Args[4]) A4;
        static if (sym.length)
            mixin("alias " ~ sym ~ " = A4." ~ sym ~ ";");
    }
    static if (Args.length >= 6)
    {
        mixin Pred!(Args[5]) A5;
        static if (sym.length)
            mixin("alias " ~ sym ~ " = A5." ~ sym ~ ";");
    }
    static if (Args.length >= 7)
    {
        mixin Pred!(Args[6]) A6;
        static if (sym.length)
            mixin("alias " ~ sym ~ " = A6." ~ sym ~ ";");
    }
    static if (Args.length > 7)
    {
        mixin ForeachInst!(Pred, sym, Args[7 .. $]) Unrolled;
        static if (sym.length)
            mixin("alias " ~ sym ~ " = Unrolled." ~ sym ~ ";");
    }
}

/// Predicate for `ForeachInst`
private template GenerateOverload (alias ovrld)
{
    mixin(q{
            override ReturnType!(ovrld) } ~ __traits(identifier, ovrld) ~ q{ (Parameters!ovrld params)
            {
                auto serialized = ArgWrapper!(Parameters!ovrld)(params)
                    .serializeToJsonString();
                auto command = Command(C.thisTid(), ovrld.mangleof, serialized);
                // `std.concurrency.send/receive[Only]` is not `@safe` but
                // this overload needs to be
                auto res = () @trusted {
                    C.send(this.childTid, command);
                    return C.receiveOnly!(Response);
                }();
                if (!res.success)
                    throw new Exception(res.data);
                static if (!is(ReturnType!(ovrld) == void))
                    return res.data.deserializeJson!(typeof(return));
            }
        });
}
