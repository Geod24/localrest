/*******************************************************************************

    Provides utilities to moch an async REST node on the network

    Using `vibe.web.rest` allow to cleanly separate business code
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

import std.concurrency;
import std.meta : AliasSeq;
import std.traits : Parameters, ReturnType;

/// Data sent by the caller
private struct Command
{
    /// Tid of the sender thread (cannot be JSON serialized)
    Tid sender;
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

/// Ditto
public final class RemoteAPI (API, Implementation : API) : API
{
    static if (is(typeof(Implementation.__ctor)))
        private alias CtorParams = Parameters!(Implementation.__ctor);
    else
        private alias CtorParams = AliasSeq!();

    /***************************************************************************

        Main dispatch function

       This function receive string-serialized messages from the calling thread,
       which is a struct with the sender's Tid, the method's mangleof,
       and the method's arguments as a tuple, serialized to a JSON string.

       `std.concurrency.receive` is not `@safe`, so neither is this.

       Params:
           args = Arguments to `Implementation`'s constructor

    ***************************************************************************/

    private static void spawned (CtorParams...) (CtorParams args)
    {
        import std.format;

        bool terminated = false;
        scope node = new Implementation(args);

        scope handler = (Command cmd) {
            SWITCH:
                switch (cmd.method)
                {
                    static foreach (member; __traits(allMembers, API))
                        static foreach (ovrld; __traits(getOverloads, API, member))
                        {
                            mixin(q{
                                    case `%2$s`:
                                    alias Method = ovrld;
                                    try {
                                        auto args = cmd.args.deserializeJson!(
                                            ArgWrapper!(Parameters!ovrld));
                                        static if (!is(ReturnType!ovrld == void)) {
                                            cmd.sender.send(
                                                Response(
                                                    true,
                                                    node.%1$s(args.args).serializeToJsonString()));
                                        } else {
                                            node.%1$s(args.args);
                                            cmd.sender.send(Response(true));
                                        }
                                    } catch (Throwable t) {
                                        // Our sender expects a response
                                        cmd.sender.send(Response(false, t.toString()));
                                    }
                                    break SWITCH;
                                }.format(member, ovrld.mangleof));
                        }
                default:
                    assert(0, "Unmatched method name: " ~ cmd.method);
                }
            };

        while (!terminated)
        {
            receive((OwnerTerminated e) { terminated = true; },
                    handler);
        }
    }

    /// Where to send message to
    private Tid childTid;

    /// Whether or not the destructor should destroy the thread
    private bool owner;

    /***************************************************************************

        Instantiate a node node and start it

        This is usually called from the main thread, which will start all the
        nodes and then start to process request.
        In order to have a connected network, no nodes in any thread should have
        a different reference to the same node.
        In practice, this means there should only be one `Tid` per `Hash`.

        When this class is finalized, the child thread will be shut down.

        Params:
          args = Arguments to the object's constructor

    ***************************************************************************/

    public this (CtorParams...) (CtorParams args)
    {
        this.childTid = spawn(&spawned!(CtorParams), args);
        this.owner = true;
    }

    // Vibe.d mandates that method must be @safe
    @safe:

    /***************************************************************************

        Create a reference to an already existing Tid

        This overload should be used by non-main Threads to get a reference
        to an already instantiated Node.

    ***************************************************************************/

    public this (Tid tid) @nogc pure nothrow
    {
        this.childTid = tid;
        this.owner = false;
    }

    public Tid tid () @nogc pure nothrow
    {
        return this.childTid;
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
                    auto serialized = ArgWrapper!(Parameters!ovrld)(params)
                        .serializeToJsonString();
                    auto command = Command(thisTid(), ovrld.mangleof, serialized);
                    // `std.concurrency.send/receive[Only]` is not `@safe` but
                    // this overload needs to be
                    auto res = () @trusted {
                        this.childTid.send(command);
                        return receiveOnly!(Response);
                    }();
                    if (!res.success)
                        throw new Exception(res.data);
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

    scope test = new RemoteAPI!(API, MockAPI)();
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
            return new RemoteAPI!(API, Node)(tid);

        switch (type)
        {
        case "normal":
            auto ret =  new RemoteAPI!(API, Node)(false);
            std.concurrency.register(name, ret.tid());
            return ret;
        case "byzantine":
            auto ret =  new RemoteAPI!(API, Node)(true);
            std.concurrency.register(name, ret.tid());
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

        node1.recv(42, Json.init);
        assert(node1.last() == "recv@2");
        node1.recv(Json.init);
        assert(node1.last() == "recv@1");
        assert(node2.last() == "pubkey");
    }

    auto testerFiber = spawn(&testFunc);
}
