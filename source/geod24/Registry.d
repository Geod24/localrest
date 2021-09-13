/*******************************************************************************

    Registry implementation for multi-threaded access

    This registry allows to look up a connection based on a `string`.
    Conceptually, it can be seen as an equivalent to a network router,
    as it turns addresses into "concrete" routes (pointers).

    It was originally part of the `std.concurrency` module,
    but was extracted to make it reusable.

    There are two kinds of registries: typed ones (`Registry`), which will
    provide a bit more type safety for network without intersection, or with
    a base type, and untyped one (`AnyRegistry). The latter matches real-world
    heterogenous networks better, as it allows to store unrelated nodes in the
    same data structure.

*******************************************************************************/

module geod24.Registry;

import core.sync.mutex;
import geod24.concurrency;
import geod24.LocalRest;

/// A typed network router
public shared struct Registry (API)
{
    /// Map from a name to a connection.
    /// Multiple names may point to the same connection.
    private Listener!API[string] connections;
    /// Gives all the names associated with a specific connection.
    private string[][Listener!API] names;
    private Mutex registryLock;

    /// Initialize this registry, creating the Mutex
    public void initialize () @safe nothrow
    {
        this.registryLock = new shared Mutex;
    }

    /**
     * Gets the binding channel associated with `name`.
     *
     * Params:
     *   name = The name to locate within the registry.
     *
     * Returns:
     *   The associated binding channel or an invalid state
     *   (such as its `init` value) if `name` is not registered.
     */
    public Listener!API locate (string name)
    {
        synchronized (registryLock)
        {
            if (shared(Listener!API)* c = name in this.connections)
                return *cast(Listener!API*)c;
            return Listener!API.init;
        }
    }

    /**
     * Register a new name for a connection.
     *
     * Associates `name` with `conn` in a process-local map. When the thread
     * represented by `conn` terminates, any names associated with it will be
     * automatically unregistered.
     *
     * Params:
     *   name = The name to associate with `conn`.
     *   conn = The connection to register.
     *
     * Returns:
     *  `true` if the name is available and `conn` is not known to represent a
     *  defunct thread.
     */
    public bool register (string name, Listener!API conn)
    {
        synchronized (registryLock)
        {
            if (name in this.connections)
                return false;
            if (conn.data.isClosed)
                return false;
            this.names[conn] ~= name;
            this.connections[name] = cast(shared)conn;
            return true;
        }
    }

    /**
     * Removes the registered name associated with a connection.
     *
     * Params:
     *  name = The name to unregister.
     *
     * Returns:
     *  true if the name is registered, false if not.
     */
    public bool unregister (string name)
    {
        import std.algorithm.mutation : remove, SwapStrategy;
        import std.algorithm.searching : countUntil;

        synchronized (registryLock)
        {
            if (shared(Listener!API)* tid = name in this.connections)
            {
                auto allNames = *cast(Listener!API*)tid in this.names;
                auto pos = countUntil(*allNames, name);
                remove!(SwapStrategy.unstable)(*allNames, pos);
                this.connections.remove(name);
                return true;
            }
            return false;
        }
    }
}

/// An untyped network router
public shared struct AnyRegistry
{
    /// This struct just presents a different API, but forwards to
    /// an instance of `Registry!(void*)` under the hood.
    private Registry!(void*) impl;

    /// See `Registry.initialize`
    public void initialize () @safe nothrow
    {
        this.impl.initialize();
    }

    /// See `Registry.locate`
    public Listener!API locate (API = void*) (string name)
    {
        return cast(Listener!API) this.impl.locate(name);
    }

    /// See `Registry.register`
    public bool register (ListenerT : Listener!APIT, APIT) (string name, ListenerT conn)
    {
        return this.impl.register(name, cast(Listener!(void*)) conn);
    }

    /// See `Registry.unregister`
    public bool unregister (string name)
    {
        return this.impl.unregister(name);
    }
}
