/*******************************************************************************

    Registry implementation for multi-threaded access

    This registry allows to look up a `MessageChannel` based on a `string`.
    It is extracted from the `std.concurrency` module to make it reusable

*******************************************************************************/

module geod24.Registry;

import core.sync.mutex;
import geod24.concurrency;
import geod24.LocalRestMessage;

/// Ditto
public shared struct Registry
{
    private MessageChannel[string] channelByName;
    private string[][MessageChannel] namesByChannel;
    private Mutex registryLock;

    /// Initialize this registry, creating the Mutex
    public void initialize() @safe nothrow
    {
        this.registryLock = new shared Mutex;
    }

    /**
     * Gets the MessageChannel associated with name.
     *
     * Params:
     *  name = The name to locate within the registry.
     *
     * Returns:
     *  The associated MessageChannel or null if name is not registered.
     */
    MessageChannel locate(string name)
    {
        synchronized (registryLock)
        {
            if (shared(MessageChannel)* channel = name in this.channelByName)
                return *cast(MessageChannel*)channel;
            return null;
        }
    }

    /**
     * Associates name with MessageChannel.
     *
     * Associates name with MessageChannel in a process-local map.  When the thread
     * represented by MessageChannel terminates, any names associated with it will be
     * automatically unregistered.
     *
     * Params:
     *  name = The name to associate with MessageChannel.
     *  channel  = The MessageChannel register by name.
     *
     * Returns:
     *  true if the name is available and MessageChannel is not known to represent a
     *  defunct thread.
     */
    bool register(string name, MessageChannel channel)
    {
        synchronized (registryLock)
        {
            if (name in channelByName)
                return false;
            if (channel.isClosed)
                return false;
            this.namesByChannel[channel] ~= name;
            this.channelByName[name] = cast(shared)channel;
            return true;
        }
    }

    /**
     * Removes the registered name associated with a MessageChannel.
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
            if (shared(MessageChannel)* channel = name in this.channelByName)
            {
                auto allNames = *cast(MessageChannel*)channel in this.namesByChannel;
                auto pos = countUntil(*allNames, name);
                remove!(SwapStrategy.unstable)(*allNames, pos);
                this.channelByName.remove(name);
                return true;
            }
            return false;
        }
    }
}
