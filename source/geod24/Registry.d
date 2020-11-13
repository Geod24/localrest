/*******************************************************************************

    Registry implementation for multi-threaded access

    This registry allows to look up a connection based on a `string`.
    Conceptually, it can be seen as an equivalent to a DNS server,
    as it turns symbolic names into "concrete" addresses (pointers).

    It was originally part of the `std.concurrency` module,
    but was extracted to make it reusable.

*******************************************************************************/

module geod24.Registry;

import core.sync.mutex;
import geod24.concurrency;
import geod24.LocalRest;

/// Ditto
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
