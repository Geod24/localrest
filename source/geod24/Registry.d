/*******************************************************************************

    Registry implementation for multi-threaded access

    This registry allows to look up a `Tid` based on a `string`.
    It is extracted from the `std.concurrency` module to make it reusable

*******************************************************************************/

module geod24.Registry;

import core.sync.mutex;
import geod24.concurrency;

/// Ditto
public shared struct Registry
{
    private Object[string] objectByName;
    private string[][Object] namesByObject;
    private Mutex registryLock;

    /// Initialize this registry, creating the Mutex
    public void initialize() @safe nothrow
    {
        this.registryLock = new shared Mutex;
    }

    /**
     * Gets the Object associated with name.
     *
     * Params:
     *  name = The name to locate within the registry.
     *
     * Returns:
     *  The associated Object or Object.init if name is not registered.
     */
    Object locate(string name)
    {
        synchronized (registryLock)
        {
            if (shared(Object)* obj = name in this.objectByName)
                return *cast(Object*)obj;
            return null;
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
    bool register(string name, Object obj)
    {
        synchronized (registryLock)
        {
            if (name in objectByName)
                return false;
            this.namesByObject[obj] ~= name;
            this.objectByName[name] = cast(shared)obj;
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
            if (shared(Object)* obj = name in this.objectByName)
            {
                auto allNames = *cast(Object*)obj in this.namesByObject;
                auto pos = countUntil(*allNames, name);
                remove!(SwapStrategy.unstable)(*allNames, pos);
                this.objectByName.remove(name);
                return true;
            }
            return false;
        }
    }
}
