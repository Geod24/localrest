# LocalRest

[![Build Status](https://travis-ci.com/geod24/localrest.svg?branch=v0.x.x)](https://travis-ci.com/geod24/localrest)
[![DUB Package](https://img.shields.io/dub/v/localrest.svg)](https://code.dlang.org/packages/localrest)
[![codecov](https://codecov.io/gh/Geod24/localrest/branch/v0.x.x/graph/badge.svg)](https://codecov.io/gh/Geod24/localrest)

A library to allow integration testing of `vibe.web.rest` based code as unittests.

## Example

The [main source file](source/geod24/LocalRest.d) is extensively documented, and includes many unittests.

The straightforward approach is to do:
```D
    static interface API
    {
        @safe: // Vibe.d requirement, but LocalRest works with `@system`
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

    // This will start a new thread, create a `MockAPI` object in it,
    // and return a handle that allows communicating with it.
    scope API test = RemoteAPI!API.spawn!MockAPI();
    // Note that the handle will be of type `RemoteAPI!API`, which implements `API`
    // This is so one can have an array of `API`, but different underlying implementations.

    // This will send a message to the remote thread, which will ultimately call `MockAPI.pubkey`
    // Any parameter and return value is serialized to a type that is safe to pass accross thread.
    // By default, Vibe.d's JSON serializer is used, but it can be replaced (see `geod24.Serialization`).
    assert(test.pubkey() == 42);

    // Remote nodes can be controlled, to extend testing parameters:
    // For example, they can be made to be unresponsive for a set duration,
    // or to ignore calls to certain methods.
    test.ctrl.shutdown();
```

For this to work, one need to have Vibe.d already fetched. For a simple test, `dub fetch vibe-d` will suffice.
For project depending on this, make sure your `dub.json` includes the following in `dependencies`:
```json
{
    "vibe-d:data": "~>0.8"
}
```
