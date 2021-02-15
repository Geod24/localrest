/**
 * This is a low-level messaging API upon which more structured or restrictive
 * APIs may be built.  The general idea is that every messageable entity is
 * represented by a common handle type called a Tid, which allows messages to
 * be sent to logical threads that are executing in both the current process
 * and in external processes using the same interface.  This is an important
 * aspect of scalability because it allows the components of a program to be
 * spread across available resources with few to no changes to the actual
 * implementation.
 *
 * A logical thread is an execution context that has its own stack and which
 * runs asynchronously to other logical threads.  These may be preemptively
 * scheduled kernel threads, fibers (cooperative user-space threads), or some
 * other concept with similar behavior.
 *
 * The type of concurrency used when logical threads are created is determined
 * by the Scheduler selected at initialization time.  The default behavior is
 * currently to create a new kernel thread per call to spawn, but other
 * schedulers are available that multiplex fibers across the main thread or
 * use some combination of the two approaches.
 *
 * Note:
 * Copied (almost verbatim) from Phobos at commit 3bfccf4f1 (2019-11-27)
 * Changes are this notice, and the module rename, from `std.concurrency`
 * to `geod24.concurrency`.
 *
 * Copyright: Copyright Sean Kelly 2009 - 2014.
 * License:   <a href="http://www.boost.org/LICENSE_1_0.txt">Boost License 1.0</a>.
 * Authors:   Sean Kelly, Alex Rønne Petersen, Martin Nowak
 * Source:    $(PHOBOSSRC std/concurrency.d)
 */
module geod24.concurrency;

public import std.variant;

import core.atomic;
import core.sync.condition;
import core.sync.mutex;
import core.sync.semaphore;
import core.thread;
import core.time : MonoTime;
import std.range.primitives;
import std.traits;
import std.algorithm;
import std.typecons;
import std.container : DList, SList;
import std.exception : assumeWontThrow;

import geod24.RingBuffer;

private
{
    bool hasLocalAliasing(Types...)()
    {
        // Works around "statement is not reachable"
        bool doesIt = false;
        static foreach (T; Types)
            doesIt |= hasLocalAliasingImpl!T;
        return doesIt;
    }

    template hasLocalAliasingImpl (T)
    {
        import std.typecons : Rebindable;

        static if (is(T : Channel!CT, CT))
            immutable bool hasLocalAliasingImpl = hasLocalAliasing!CT;
        else static if (is(T : Rebindable!R, R))
            immutable bool hasLocalAliasingImpl = hasLocalAliasing!R;
        else static if (is(T == struct))
            immutable bool hasLocalAliasingImpl = hasLocalAliasing!(typeof(T.tupleof));
        else
            immutable bool hasLocalAliasingImpl = std.traits.hasUnsharedAliasing!(T);
    }

    @safe unittest
    {
        static struct Container { Channel!int t; Channel!string m; }
        static assert(!hasLocalAliasing!(Channel!(Channel!int), Channel!int, Container, int));
        static assert( hasLocalAliasing!(Channel!(Channel!(int[]))));
    }

    @safe unittest
    {
        /* Issue 20097 */
        import std.datetime.systime : SysTime;
        static struct Container { SysTime time; }
        static assert(!hasLocalAliasing!(SysTime, Container));
    }
}

// Exceptions

class FiberBlockedException : Exception
{
    this(string msg = "Fiber is blocked") @safe pure nothrow @nogc
    {
        super(msg);
    }
}


// Thread Creation

private FiberScheduler thisSchedulerStorage;

/***************************************************************************

    Get current running FiberScheduler

    Returns:
        Returns a reference to the current scheduler or null no
        scheduler is running

    TODO: Support for nested schedulers

***************************************************************************/

public FiberScheduler thisScheduler () nothrow
{
    return thisSchedulerStorage;
}

/***************************************************************************

    Set current running FiberScheduler

    Params:
        value = Reference to the current FiberScheduler

***************************************************************************/

public void thisScheduler (FiberScheduler value) nothrow
{
    thisSchedulerStorage = value;
}

/**
 * An example Scheduler using Fibers.
 *
 * This is an example scheduler that creates a new Fiber per call to spawn
 * and multiplexes the execution of all fibers within the main thread.
 */
public class FiberScheduler
{
    /***************************************************************************

        Default ctor

        Params:
            max_parked_fibers = Maximum number of parked fibers

    ***************************************************************************/

    public this (size_t max_parked_fibers = 8) nothrow
    {
        this.sem = assumeWontThrow(new Semaphore());
        this.blocked_ex = new FiberBlockedException();
        this.max_parked = max_parked_fibers;
    }

    /**
     * This creates a new Fiber for the supplied op and then starts the
     * dispatcher.
     */
    void start (void delegate() op)
    {
        this.create(op, true);
        // Make sure the just-created fiber is run first
        this.dispatch();
    }

    /**
     * This created a new Fiber for the supplied op and adds it to the
     * dispatch list.
     */
    void spawn (void delegate() op) nothrow
    {
        this.create(op);
        FiberScheduler.yield();
    }

    /**************************************************************************

        Schedule a task to be run next time the scheduler yields

        Behave similarly to `spawn`, but instead of running the task
        immediately, it simply adds it to the queue and continue executing
        the current task.

        Params:
            op = Operation to run

    **************************************************************************/

    public void schedule (void delegate() op) nothrow
    {
        this.create(op);
    }

    /**
     * If the caller is a scheduled Fiber, this yields execution to another
     * scheduled Fiber.
     */
    public static void yield () nothrow
    {
        // NOTE: It's possible that we should test whether the calling Fiber
        //       is an InfoFiber before yielding, but I think it's reasonable
        //       that any fiber should yield here.
        if (Fiber.getThis())
            Fiber.yield();
    }

    /**
     * If the caller is a scheduled Fiber, this yields execution to another
     * scheduled Fiber.
     */
    public static void yieldAndThrow (Throwable t) nothrow
    {
        // NOTE: It's possible that we should test whether the calling Fiber
        //       is an InfoFiber before yielding, but I think it's reasonable
        //       that any fiber should yield here.
        if (Fiber.getThis())
            Fiber.yieldAndThrow(t);
    }

    /// Resource type that will be tracked by FiberScheduler
    protected interface Resource
    {
        ///
        void release () nothrow;
    }

    /***********************************************************************

        Add Resource to the Resource list of runnning Fiber

        Param:
            r = Resource instace

    ***********************************************************************/

    private void addResource (Resource r, InfoFiber info_fiber = cast(InfoFiber) Fiber.getThis()) nothrow
    {
        assert(info_fiber, "Called from outside of an InfoFiber");
        info_fiber.resources.insert(r);
    }

    /***********************************************************************

        Remove Resource from the Resource list of runnning Fiber

        Param:
            r = Resource instance

        Returns:
            Success/failure

    ***********************************************************************/

    private bool removeResource (Resource r, InfoFiber info_fiber = cast(InfoFiber) Fiber.getThis()) nothrow
    {
        assert(info_fiber, "Called from outside of an InfoFiber");
        // TODO: For some cases, search is not neccesary. We can just pop the last element
        return assumeWontThrow(info_fiber.resources.linearRemoveElement(r));
    }

    /**
     * Creates a new Fiber which calls the given delegate.
     *
     * Params:
     *   op = The delegate the fiber should call
     *   insert_front = Fiber will be added to the front
     *                  of the ready queue to be run first
     */
    protected void create (void delegate() op, bool insert_front = false) nothrow
    {
        InfoFiber new_fiber;
        if (this.parked_count > 0)
        {
            new_fiber = this.parked_fibers.front();
            new_fiber.reuse(op);

            this.parked_fibers.removeFront();
            this.parked_count--;
        }
        else
        {
            new_fiber = new InfoFiber(op);
        }

        if (insert_front)
            this.readyq.insertFront(new_fiber);
        else
            this.readyq.insertBack(new_fiber);
    }

    /**
     * Fiber which embeds neccessary info for FiberScheduler
     */
    protected static class InfoFiber : Fiber
    {
        /// Semaphore reference that this Fiber is blocked on
        FiberBlocker blocker;

        /// List of Resources held by this Fiber
        DList!Resource resources;

        this (void delegate() op, size_t sz = 512 * 1024) nothrow
        {
            super(op, sz);
        }

        /***********************************************************************

            Reset the Fiber to be reused with a new delegate

            Param:
                op = Delegate

        ***********************************************************************/

        void reuse (void delegate() op) nothrow
        {
            assert(this.state == Fiber.State.TERM, "Can not reuse a non terminated Fiber");
            this.blocker = null;
            this.resources.clear();
            this.reset(op);
        }
    }

    public final class FiberBlocker
    {

        /***********************************************************************

            Associate `FiberBlocker` with the running `Fiber`

            `FiberScheduler` will check to see if the `Fiber` is blocking on a
            `FiberBlocker` to avoid rescheduling it unnecessarily

        ***********************************************************************/

        private void registerToInfoFiber (InfoFiber info_fiber = cast(InfoFiber) Fiber.getThis()) nothrow
        {
            assert(info_fiber !is null, "This Fiber does not belong to FiberScheduler");
            assert(info_fiber.blocker is null, "This Fiber already has a registered FiberBlocker");
            info_fiber.blocker = this;

        }

        /***********************************************************************

            Wait on the blocker with optional timeout

            Params:
                period = Timeout period

        ***********************************************************************/

        bool wait (Duration period = Duration.init) nothrow
        {
            if (period != Duration.init)
                this.limit = MonoTime.currTime + period;

            if (this.shouldBlock())
            {
                this.registerToInfoFiber();
                FiberScheduler.yieldAndThrow(this.outer.blocked_ex);
            }

            this.limit = MonoTime.init;
            this.notified = false;
            return !this.hasTimedOut();
        }

        /***********************************************************************

            Unblock the Fiber waiting on this blocker

        ***********************************************************************/

        void notify () nothrow
        {
            this.stopTimer();
            this.notified = true;
            assumeWontThrow(this.outer.sem.notify());
        }

        /***********************************************************************

            Query if `FiberBlocker` should still block

            FiberBlocker will block the Fiber until it is notified or
            the specified timeout is reached.

        ***********************************************************************/

        bool shouldBlock () nothrow
        {
            bool timed_out = (this.limit != MonoTime.init
                                && MonoTime.currTime >= this.limit);

            if (timed_out)
                cas(&this.timer_state, TimerState.Running, TimerState.TimedOut);

            return atomicLoad(this.timer_state) != TimerState.TimedOut && !this.notified;
        }

        /***********************************************************************

            Try freezing the internal timer

        ***********************************************************************/

        bool stopTimer () nothrow
        {
            return cas(&this.timer_state, TimerState.Running, TimerState.Stopped);
        }

        /***********************************************************************

            Query if the internal timer has timed out

        ***********************************************************************/

        bool hasTimedOut () nothrow
        {
            return atomicLoad(this.timer_state) == TimerState.TimedOut;
        }

        MonoTime getTimeout () nothrow
        {
            return limit;
        }

    private:
        enum TimerState
        {
            Running,
            TimedOut,
            Stopped
        }

        /// Time limit that will eventually unblock the caller if a timeout is specified
        MonoTime limit = MonoTime.init;

        /// State of the blocker
        bool notified;

        /// State of the internal timer
        shared(TimerState) timer_state;
    }

    /***********************************************************************

        Start the scheduling loop

    ***********************************************************************/

    private void dispatch ()
    {
        thisScheduler(this);
        scope (exit) thisScheduler(null);

        MonoTime earliest_timeout;

        while (!this.readyq.empty() || !this.wait_list.empty())
        {
            while (!readyq.empty())
            {
                InfoFiber cur_fiber = this.readyq.front();
                this.readyq.removeFront();

                assert(cur_fiber.state != Fiber.State.TERM);

                auto t = cur_fiber.call(Fiber.Rethrow.no);

                // Fibers that block on a FiberBlocker throw an
                // exception for scheduler to catch
                if (t is this.blocked_ex)
                {
                    auto cur_timeout = cur_fiber.blocker.getTimeout();

                    // Keep track of the earliest timeout in the system
                    if (cur_timeout != MonoTime.init
                            && (earliest_timeout == MonoTime.init || cur_timeout < earliest_timeout))
                    {
                        earliest_timeout = cur_timeout;
                    }

                    this.wait_list.insert(cur_fiber);
                    continue;
                }
                else if (t)
                {
                    // We are exiting the dispatch loop prematurely, all resources
                    // held by Fibers should be released.
                    this.releaseResources(cur_fiber);
                    throw t;
                }

                if (cur_fiber.state != Fiber.State.TERM)
                {
                    this.readyq.insert(cur_fiber);
                }
                // Park terminated Fiber
                else if (this.parked_count < this.max_parked)
                {
                    this.parked_fibers.insertFront(cur_fiber);
                    this.parked_count++;
                }
                // Destroy the terminated Fiber to immediately reclaim
                // the stack space
                else
                {
                    destroy!false(cur_fiber);
                }

                // See if there are Fibers to be woken up if we reach a timeout
                // or the scheduler semaphore was notified
                if (MonoTime.currTime >= earliest_timeout || this.sem.tryWait())
                    earliest_timeout = wakeFibers();
            }

            if (!this.wait_list.empty())
            {
                Duration time_to_timeout = earliest_timeout - MonoTime.currTime;

                // Sleep until a timeout or an event
                if (earliest_timeout == MonoTime.init)
                    this.sem.wait();
                else if (time_to_timeout > 0.seconds)
                    this.sem.wait(time_to_timeout);
            }

            // OS Thread woke up populate ready queue
            earliest_timeout = wakeFibers();
        }
    }

    /***********************************************************************

        Move unblocked Fibers to ready queue

        Return:
            Returns the earliest timeout left in the waiting list

    ***********************************************************************/

    private MonoTime wakeFibers()
    {
        import std.range;
        MonoTime earliest_timeout;

        auto wait_range = this.wait_list[];
        while (!wait_range.empty)
        {
            auto fiber = wait_range.front;

            // Remove the unblocked Fiber from wait list and
            // append it to the end ofready queue
            if (!fiber.blocker.shouldBlock())
            {
                this.wait_list.popFirstOf(wait_range);
                fiber.blocker = null;
                this.readyq.insert(fiber);
            }
            else
            {
                auto timeout = fiber.blocker.getTimeout();
                if (timeout != MonoTime.init
                        && (earliest_timeout == MonoTime.init || timeout < earliest_timeout))
                {
                    earliest_timeout = timeout;
                }
                wait_range.popFront();
            }
        }

        return earliest_timeout;
    }

    /***********************************************************************

        Release all resources currently held by all Fibers owned by this
        scheduler

        Param:
            cur_fiber = Running Fiber

    ***********************************************************************/

    private void releaseResources (InfoFiber cur_fiber)
    {
        foreach (ref resource; cur_fiber.resources)
            resource.release();
        foreach (ref fiber; this.readyq)
            foreach (ref resource; fiber.resources)
                resource.release();
        foreach (ref fiber; this.wait_list)
            foreach (ref resource; fiber.resources)
                resource.release();
    }

private:

    /// OS semaphore for scheduler to sleep on
    Semaphore sem;

    /// A FIFO Queue of Fibers ready to run
    DList!InfoFiber readyq;

    /// List of Fibers waiting for an event
    DList!InfoFiber wait_list;

    /// Cached instance of FiberBlockedException
    FiberBlockedException blocked_ex;

    /// List of parked fibers to be reused
    SList!InfoFiber parked_fibers;

    /// Number of currently parked fibers
    size_t parked_count;

    /// Maximum number of parked fibers
    immutable size_t max_parked;
}

/// Ensure argument to `start` is run first
unittest
{
    {
        scope sched = new FiberScheduler();
        bool startHasRun;
        sched.spawn(() => assert(startHasRun));
        sched.start(() { startHasRun = true; });
    }
    {
        scope sched = new FiberScheduler();
        bool startHasRun;
        sched.schedule(() => assert(startHasRun));
        sched.start(() { startHasRun = true; });
    }
}

/***********************************************************************

    A common interface for objects that can be used in `select()`

***********************************************************************/

public interface Selectable
{
    /***********************************************************************

        Try to read/write to the `Selectable` without blocking. If the
        operation would block, queue and link it with the `sel_state`

        Params:
            ptr = pointer to the data for the select operation
            sel_state = SelectState instace of the select call being executed
            sel_id = id of the select call being executed

    ***********************************************************************/

    void selectWrite (void* ptr, SelectState sel_state, int sel_id);

    /// Ditto
    void selectRead (void* ptr, SelectState sel_state, int sel_id);
}

/***********************************************************************

    An aggregate to hold neccessary information for a select operation

***********************************************************************/

public struct SelectEntry
{
    /// Reference to a Selectable object
    Selectable selectable;

    /// Pointer to the select data
    void* select_data;

    /***********************************************************************

        Default ctor

        Params:
            selectable = A selectable interface reference
            select_data = pointer to the data for the select operation

    ***********************************************************************/

    this (Selectable selectable, void* select_data) @safe pure nothrow @nogc
    {
        this.selectable = selectable;
        this.select_data = select_data;
    }
}

/// Consists of the id and result of the select operation that was completed
public alias SelectReturn = Tuple!(bool, "success", int, "id");

/***********************************************************************

    Block on multiple `Channel`s

    Only one operation is completed per `select()` call

    Params:
        read_list = List of `Channel`s to select for read operation
        write_list = List of `Channel`s to select for write operation
        timeout = Optional timeout

    Return:
        Returns success/failure status of the operation and the index
        of the `Channel` that the operation was carried on. The index is
        the position of the SelectEntry in `read_list ~ write_list`, ie
        concatenated lists.

***********************************************************************/

public SelectReturn select (ref SelectEntry[] read_list, ref SelectEntry[] write_list,
    Duration timeout = Duration.init)
{
    import std.random : randomShuffle;

    auto ss = new SelectState();
    int sel_id = 0;
    thisScheduler().addResource(ss);
    scope (exit) thisScheduler().removeResource(ss);

    read_list = read_list.randomShuffle();
    write_list = write_list.randomShuffle();

    foreach (ref list; [read_list, write_list])
    {
        foreach (ref entry; list)
        {
            if (ss.isConsumed())
                break;

            if (list is read_list)
                entry.selectable.selectRead(entry.select_data, ss, sel_id++);
            else if (list is write_list)
                entry.selectable.selectWrite(entry.select_data, ss, sel_id++);
        }
    }

    if (!ss.blocker.wait(timeout))
        return SelectReturn(false, -1); // Timed out

    return SelectReturn(ss.success, ss.id);
}

/***********************************************************************

    Holds the state of a group of `selectRead`/`selectWrite` calls.
    Synchronizes peers that will consume those calls, so that only one
    `selectRead`/`selectWrite` call is completed.

***********************************************************************/

final private class SelectState : FiberScheduler.Resource
{
    /// Shared blocker object for multiple ChannelQueueEntry objects
    FiberScheduler.FiberBlocker blocker;

    /***********************************************************************

        Default constructor

    ***********************************************************************/

    this () nothrow
    {
        this.blocker = thisScheduler().new FiberBlocker();
    }

    /***********************************************************************

        Tries to atomically consume a `SelectState` and sets `id` and `success`
        fields

        Param:
            id = ID of the `selectRead`/`selectWrite` call that is consuming
                 this `SelectState`
            success_in = Success/Failure of the select call

        Return:
            Returns true if `SelectState` was not already consumed, false otherwise

    ***********************************************************************/

    bool tryConsume (int id, bool success_in = true) nothrow
    {
        if (cas(&this.consumed, false, true))
        {
            this.id = id;
            this.success = success_in;
            return true;
        }
        return false;
    }

    /***********************************************************************

        Returns if `SelectState` is already consumed or not

    ***********************************************************************/

    bool isConsumed () nothrow
    {
        return atomicLoad(this.consumed);
    }

    /***********************************************************************

        Consume SelectState so that it is neutralized

    ***********************************************************************/

    void release () nothrow
    {
        this.tryConsume(-1, false);
    }

    /// ID of the select call that consumed this `SelectState`
    int id;

    /// Success/failure state of the select call with ID of `id`
    bool success;

private:
    /// Indicates if this `SelectState` is consumed or not
    shared(bool) consumed;
}

/***********************************************************************

    A golang style channel implementation with buffered and unbuffered
    operation modes

    Intended to be used between Fibers

    Param:
        T = Type of the messages carried accross the `Channel`. Currently
            all reference and values types are supported.

***********************************************************************/

final public class Channel (T) : Selectable
{
    /***********************************************************************

        Constructs a Channel

        Param:
            max_size = Maximum amount of T a Channel can buffer
                       (0 -> Unbuffered operation,
                        Positive integer -> Buffered operation)

    ***********************************************************************/

    this (ulong max_size = 0) nothrow
    {
        this.max_size = max_size;
        this.lock = new FiberMutex;
        if (max_size)
            this.buffer = new RingBuffer!T(max_size);
    }

    /***********************************************************************

        Write a message to the `Channel` with an optional timeout

        Unbuffered mode:

            If a reader is already blocked on the `Channel`, writer copies the
            message to reader's buffer and wakes up the reader by yielding

            If no reader is ready in the wait queue, writer appends itself
            to write wait queue and blocks

        Buffered mode:

            If a reader is already blocked on the `Channel`, writer copies the
            message to reader's buffer and wakes up the reader and returns
            immediately.

            If the buffer is not full writer puts the message in the `Channel`
            buffer and returns immediately

            If buffer is full writer appends itself to write wait queue and blocks

        If `Channel` is closed, it returns immediately with a failure,
        regardless of the operation mode

        Param:
            val = Message to write to `Channel`
            duration = Timeout duration

        Returns:
            Success/Failure - Fails when `Channel` is closed or timeout is reached.

    ***********************************************************************/

    bool write () (auto ref T val, Duration duration = Duration.init) nothrow
    {
        this.lock.lock_nothrow();

        bool success = tryWrite(val);

        if (!success && !this.isClosed())
        {
            ChannelQueueEntry q_ent = this.enqueueEntry(this.writeq, &val);
            thisScheduler().addResource(q_ent);
            scope (exit) thisScheduler().removeResource(q_ent);

            this.lock.unlock_nothrow();
            return q_ent.blocker.wait(duration) && q_ent.success;
        }

        this.lock.unlock_nothrow();
        return success;
    }

    /***********************************************************************

        Try to write a message to `Channel` without blocking

        tryWrite writes a message if it is possible to do so without blocking.

        If the tryWrite is being executed in a select call, it tries to consume the
        `caller_sel` with the given `caller_sel_id`. It only proceeds if it can
        successfully consume the `caller_sel`

        Param:
            val = Message to write to `Channel`
            caller_sel = SelectState instace of the select call being executed
            caller_sel_id = id of the select call being executed

        Returns:
            Success/Failure

    ***********************************************************************/

    private bool tryWrite () (auto ref T val, SelectState caller_sel = null, int caller_sel_id = 0) nothrow
    {
        if (this.isClosed())
        {
            if (caller_sel)
                caller_sel.tryConsume(caller_sel_id, false);
            return false;
        }

        if (ChannelQueueEntry readq_ent = this.dequeueEntry(this.readq, caller_sel, caller_sel_id))
        {
            *readq_ent.pVal = val;
            readq_ent.blocker.notify();
            return true;
        }

        if (this.max_size > 0 // this.max_size > 0 = buffered
                    && this.buffer.length < this.max_size
                    && (!caller_sel || caller_sel.tryConsume(caller_sel_id)))
        {
            this.buffer.insert(val);
            return true;
        }

        return false;
    }

    /***********************************************************************

        Try to write a message to Channel without blocking and if it fails,
        create a write queue entry using the given `sel_state` and `sel_id`

        Param:
            ptr = Message to write to channel
            sel_state = SelectState instace of the select call being executed
            sel_id = id of the select call being executed

    ***********************************************************************/

    void selectWrite (void* ptr, SelectState sel_state, int sel_id) nothrow
    {
        assert(ptr !is null);
        assert(sel_state !is null);
        T* val = cast(T*) ptr;

        this.lock.lock_nothrow();

        bool success = tryWrite(*val, sel_state, sel_id);

        if (!sel_state.isConsumed())
            this.enqueueEntry(this.writeq, val, sel_state, sel_id);

        if (success || this.isClosed() || sel_state.id == -1)
        {
            this.lock.unlock_nothrow();
            sel_state.blocker.notify();
        }
        else
            this.lock.unlock_nothrow();
    }

    /***********************************************************************

        Read a message from the Channel with an optional timeout

        Unbuffered mode:

            If a writer is already blocked on the Channel, reader copies the
            value to `output` and wakes up the writer by yielding

            If no writer is ready in the wait queue, reader appends itself
            to read wait queue and blocks

            If channel is closed, it returns immediatly with a failure

        Buffered mode:

            If there are existing messages in the buffer, reader pops one off
            the buffer and returns immediatly with success, regardless of the
            Channel being closed or not

            If there are no messages in the buffer it behaves exactly like the
            unbuffered operation

        Param:
            output = Reference to output variable
            duration = Timeout duration

        Returns:
            Success/Failure - Fails when channel is closed and there are
            no existing messages to be read. Fails when timeout is reached.

    ***********************************************************************/

    bool read (ref T output, Duration duration = Duration.init) nothrow
    {
        this.lock.lock_nothrow();

        bool success = tryRead(output);

        if (!success && !this.isClosed())
        {
            ChannelQueueEntry q_ent = this.enqueueEntry(this.readq, &output);
            thisScheduler().addResource(q_ent);
            scope (exit) thisScheduler().removeResource(q_ent);

            this.lock.unlock_nothrow();
            return q_ent.blocker.wait(duration) && q_ent.success;
        }

        this.lock.unlock_nothrow();
        return success;
    }

    /***********************************************************************

        Try to read a message from Channel without blocking

        tryRead reads a message if it is possible to do so without blocking.

        If the tryRead is being executed in a select call, it tries to consume the
        `caller_sel` with the given `caller_sel_id`. It only proceeds if it can
        successfully consume the `caller_sel`

        Param:
            output = Field to write the message to
            caller_sel = SelectState instace of the select call being executed
            caller_sel_id = id of the select call being executed\

        Returns:
            Success/Failure

    ***********************************************************************/

    private bool tryRead (ref T output, SelectState caller_sel = null, int caller_sel_id = 0) nothrow
    {
        ChannelQueueEntry write_ent = this.dequeueEntry(this.writeq, caller_sel, caller_sel_id);

        if (this.max_size > 0 && !this.buffer.empty())
        {
            // if dequeueEntry fails, we will try to consume caller_sel again.
            if (!caller_sel || write_ent || caller_sel.tryConsume(caller_sel_id))
            {
                output = this.buffer.front();
                this.buffer.popFront();

                if (write_ent)
                {
                    this.buffer.insert(*write_ent.pVal);
                }
            }
            else
            {
                return false;
            }
        }
        // if dequeueEntry returns a valid entry, it always successfully consumes the related select states.
        // the race between 2 select calls is resolved in dequeueEntry.
        else if (write_ent)
        {
            output = *write_ent.pVal;
        }
        else
        {
            if (this.isClosed() && caller_sel)
                caller_sel.tryConsume(caller_sel_id, false);
            return false;
        }

        if (write_ent)
            write_ent.blocker.notify();
        return true;
    }

    /***********************************************************************

        Try to read a message from Channel without blocking and if it fails,
        create a read queue entry using the given `sel_state` and `sel_id`

        Param:
            ptr = Buffer to write the message to
            sel_state = SelectState instace of the select call being executed
            sel_id = id of the select call being executed

    ***********************************************************************/

    void selectRead (void* ptr, SelectState sel_state, int sel_id) nothrow
    {
        assert(ptr !is null);
        assert(sel_state !is null);
        T* val = cast(T*) ptr;

        this.lock.lock_nothrow();

        bool success = tryRead(*val, sel_state, sel_id);

        if (!sel_state.isConsumed())
            this.enqueueEntry(this.readq, val, sel_state, sel_id);

        if (success || this.isClosed() || sel_state.id == -1)
        {
            this.lock.unlock_nothrow();
            sel_state.blocker.notify();
        }
        else
            this.lock.unlock_nothrow();
    }

    /***********************************************************************

        Close the channel

        Closes the channel by marking it closed and flushing all the wait
        queues

    ***********************************************************************/

    void close () nothrow
    {
        if (cas(&this.closed, false, true))
        {
            this.lock.lock_nothrow();
            scope (exit) this.lock.unlock_nothrow();

            // Wake blocked Fibers up, report the failure
            foreach (ref entry; this.readq)
            {
                entry.terminate();
            }
            foreach (ref entry; this.writeq)
            {
                entry.terminate();
            }

            this.readq.clear();
            this.writeq.clear();
        }
    }

    /***********************************************************************

        Return the length of the internal buffer

    ***********************************************************************/

    size_t length () nothrow
    {
        this.lock.lock_nothrow();
        scope (exit) this.lock.unlock_nothrow();
        return this.buffer.length;
    }

    /***********************************************************************

        Return the closed status of the `Channel`

    ***********************************************************************/

    bool isClosed () const @safe pure nothrow @nogc scope
    {
        return atomicLoad(this.closed);
    }

    /***********************************************************************

        An aggrate of neccessary information to block a Fiber and record
        their request

    ***********************************************************************/

    private static class ChannelQueueEntry : FiberScheduler.Resource
    {
        /// FiberBlocker blocking the `Fiber`
        FiberScheduler.FiberBlocker blocker;

        /// Pointer to the variable that we will read to/from
        T* pVal;

        /// Result of the blocking read/write call
        bool success = true;

        /// State of the select call that this entry was created for
        SelectState select_state;

        /// Id of the select call that this entry was created for
        int sel_id;

        this (T* pVal, SelectState select_state = null, int sel_id = 0) nothrow
        {
            this.pVal = pVal;
            this.select_state = select_state;
            this.sel_id = sel_id;

            if (this.select_state)
                this.blocker = this.select_state.blocker;
            else
                this.blocker = thisScheduler().new FiberBlocker();
        }

        /***********************************************************************

            Terminate a `ChannelQueueEntry` by waking up the blocked Fiber
            and reporting the failure

            This is called on all the `ChannelQueueEntry` instances still in
            the wait queues when Channel is closed

        ***********************************************************************/

        void terminate () nothrow
        {
            this.success = false;
            if (!this.select_state || this.select_state.tryConsume(this.sel_id, this.success))
                this.blocker.notify();
        }


        /***********************************************************************

            Terminate ChannelQueueEntry so that it is neutralized

        ***********************************************************************/

        void release () nothrow
        {
            if (this.blocker.stopTimer())
                this.pVal = null; // Sanitize pVal so that we can catch illegal accesses
        }
    }

    /***********************************************************************

        Create and enqueue a `ChannelQueueEntry` to the given entryq

        Param:
            entryq = Queue to append the new ChannelQueueEntry
            pVal =  Pointer to the message buffer
            sel_state = SelectState object to associate with the
                        newly created ChannelQueueEntry
            sel_id = id of the select call creating the new ChannelQueueEntry

        Return:
            newly created ChannelQueueEntry

    ***********************************************************************/

    private ChannelQueueEntry enqueueEntry (ref DList!ChannelQueueEntry entryq, T* pVal,
                                            SelectState sel_state = null, int sel_id = 0) nothrow
    {
        assert(pVal !is null);

        ChannelQueueEntry q_ent = new ChannelQueueEntry(pVal, sel_state, sel_id);
        entryq.insert(q_ent);

        return q_ent;
    }

    /***********************************************************************

        Dequeue a `ChannelQueueEntry` from the given `entryq`

        Walks the `entryq` until it finds a suitable entry or the queue
        empties. If `dequeueEntry` is called from a select, it tries to
        consume the `caller_sel` if the `peer_sel` is not currently consumed.
        If it fails to consume the `caller_sel`, returns with a failure.

        If selected queue entry is part of a select operation, it is also
        consumed. If it consumes `caller_sel` but `peer_sel` was already
        consumed whole select operation would fail and caller would need to try
        again. This should be a rare case, where the `peer_sel` gets consumed by
        someone else between the first if check which verifies that it is not
        consumed and the point we actually try to consume it.

        Param:
            entryq = Queue to append the new ChannelQueueEntry
            caller_sel = SelectState instace of the select call being executed
            caller_sel_id = id of the select call being executed

        Return:
            a valid ChannelQueueEntry or null

    ***********************************************************************/

    private ChannelQueueEntry dequeueEntry (ref DList!ChannelQueueEntry entryq,
                                            SelectState caller_sel = null, int caller_sel_id = 0) nothrow
    {
        while (!entryq.empty())
        {
            ChannelQueueEntry qent = entryq.front();
            auto peer_sel = qent.select_state;

            if ((!peer_sel || !peer_sel.isConsumed()) && qent.blocker.shouldBlock())
            {
                // If we are in a select call, try to consume the caller select
                // if we can't consume the caller select, no need to continue
                if (caller_sel && !caller_sel.tryConsume(caller_sel_id))
                {
                    return null;
                }

                // At this point, caller select is consumed.
                // Try to consume the peer select if it exists
                // If peer_sel was consumed by someone else, tough luck
                // In that case, whole select will fail since we consumed the caller_sel
                if ((!peer_sel || peer_sel.tryConsume(qent.sel_id)) && qent.blocker.stopTimer())
                {
                    entryq.removeFront();
                    return qent;
                }
                else if (caller_sel)
                {
                    // Mark caller_sel failed
                    caller_sel.id = -1;
                    caller_sel.success = false;
                    return null;
                }
            }

            entryq.removeFront();
        }

        return null;
    }

private:
    /// Internal data storage
    RingBuffer!T buffer;

    /// Closed flag
    bool closed;

    /// Per channel lock
    FiberMutex lock;

    /// List of fibers blocked on read()
    DList!ChannelQueueEntry readq;

    /// List of fibers blocked on write()
    DList!ChannelQueueEntry writeq;

public:
    /// Maximum amount of T a Channel can buffer
    immutable ulong max_size;
}

// Test non blocking operation
@system unittest
{
    string str = "DEADBEEF";
    string rcv_str;
    FiberScheduler scheduler = new FiberScheduler;
    auto chn = new Channel!string(2);

    scheduler.start({
        chn.write(str ~ " 1");
        assert(chn.length() == 1);
        chn.write(str ~ " 2");
        assert(chn.length() == 2);

        assert(chn.read(rcv_str));
        assert(rcv_str == str ~ " 1");
        assert(chn.length() == 1);

        chn.write(str ~ " 3");
        assert(chn.length() == 2);

        assert(chn.read(rcv_str));
        assert(rcv_str == str ~ " 2");
        assert(chn.length() == 1);

        assert(chn.read(rcv_str));
        assert(rcv_str == str ~ " 3");
        assert(chn.length() == 0);
    });
}

// Test unbuffered blocking operation with multiple receivers
// Receiver should read every message in the order they were sent
@system unittest
{
    FiberScheduler scheduler = new FiberScheduler;
    auto chn = new Channel!int();
    int n = 1000;
    long sum;

    scheduler.spawn(
        () {
            int val, prev;
            bool ret = chn.read(prev);
            sum += prev;
            assert(ret);

            while (chn.read(val))
            {
                sum += val;
                assert(ret);
                assert(prev < val);
                prev = val;
            }
        }
    );

    scheduler.spawn(
        () {
            int val, prev;
            bool ret = chn.read(prev);
            sum += prev;
            assert(ret);

            while (chn.read(val))
            {
                sum += val;
                assert(ret);
                assert(prev < val);
                prev = val;
            }
        }
    );

    scheduler.start(
        () {
            for (int i = 0; i <= n; i++)
            {
                assert(chn.write(i));
            }
            chn.close();

            assert(!chn.write(0));
        }
    );

    // Sum of [0..1000]
    assert(sum == n*(n+1)/2);
}

// Test that writer is not blocked until buffer is full and a read unblocks the writer
// Reader should be able to read remaining messages after chn is closed
@system unittest
{
    FiberScheduler scheduler = new FiberScheduler;
    auto chn = new Channel!int(5);

    scheduler.spawn(
        () {
            int val;
            assert(chn.max_size == chn.length);
            assert(chn.read(val));
            chn.close();
            // Read remaining messages after channel is closed
            for (int i = 0; i < chn.max_size; i++)
            {
                assert(chn.read(val));
            }
            // No more messages to read on closed chn
            assert(!chn.read(val));
        }
    );

    scheduler.start(
        () {
            for (int i = 0; i < chn.max_size; i++)
            {
                assert(chn.write(i));
            }
            assert(chn.max_size == chn.length);
            // Wait for read.
            assert(chn.write(42));
            // Reader already closed the channel
            assert(!chn.write(0));
        }
    );
}

@system unittest
{
    struct HyperLoopMessage
    {
        int id;
        MonoTime time;
    }

    FiberScheduler scheduler = new FiberScheduler;
    auto chn1 = new Channel!HyperLoopMessage();
    auto chn2 = new Channel!HyperLoopMessage();
    auto chn3 = new Channel!HyperLoopMessage();

    scheduler.spawn(
        () {
            HyperLoopMessage msg;

            for (int i = 0; i < 1000; ++i)
            {
                assert(chn2.read(msg));
                assert(msg.id % 3 == 1);
                msg.id++;
                msg.time = MonoTime.currTime;
                assert(chn3.write(msg));
            }
        }
    );

    scheduler.spawn(
        () {
            HyperLoopMessage msg;

            for (int i = 0; i < 1000; ++i)
            {
                assert(chn1.read(msg));
                assert(msg.id % 3 == 0);
                msg.id++;
                msg.time = MonoTime.currTime;
                assert(chn2.write(msg));
            }
        }
    );

    scheduler.start(
        () {
            HyperLoopMessage msg = {
                id : 0,
                time : MonoTime.currTime
            };

            for (int i = 0; i < 1000; ++i)
            {
                assert(chn1.write(msg));
                assert(chn3.read(msg));
                assert(msg.id % 3 == 2);
                msg.id++;
                msg.time = MonoTime.currTime;
            }
        }
    );
}

// Multiple writer threads writing to a buffered channel
// Reader should receive all messages
@system unittest
{
    FiberScheduler scheduler = new FiberScheduler;
    immutable int n = 5000;
    auto chn1 = new Channel!int(n/10);

    shared int sharedVal = 0;
    shared int writer_sum = 0;

    auto t1 = new Thread({
        FiberScheduler scheduler = new FiberScheduler();
        scheduler.start(
            () {
                int val = atomicOp!"+="(sharedVal, 1);
                while (chn1.write(val))
                {
                    atomicOp!"+="(writer_sum, val);
                    val = atomicOp!"+="(sharedVal, 1);
                }
            }
        );
    });
    t1.start();

    auto t2 = new Thread({
        FiberScheduler scheduler = new FiberScheduler();
        scheduler.start(
            () {
                int val = atomicOp!"+="(sharedVal, 1);
                while (chn1.write(val))
                {
                    atomicOp!"+="(writer_sum, val);
                    val = atomicOp!"+="(sharedVal, 1);
                }
            }
        );
    });
    t2.start();

    scheduler.start(
        () {
            int reader_sum, readVal, count;

            while(chn1.read(readVal))
            {
                reader_sum += readVal;
                if (count++ == n) chn1.close();
            }

            thread_joinAll();
            assert(reader_sum == writer_sum);
        }
    );
}

@system unittest
{
    FiberScheduler scheduler = new FiberScheduler;
    auto chn1 = new Channel!int();
    auto chn2 = new Channel!int(1);
    auto chn3 = new Channel!int();

    scheduler.spawn(
        () {
            chn1.write(42);
            chn1.close();
            chn3.write(37);
        }
    );

    scheduler.spawn(
        () {
            chn2.write(44);
            chn2.close();
        }
    );

    scheduler.start(
        () {
            bool[3] chn_closed;
            int[3] read_val;
            for (int i = 0; i < 5; ++i)
            {
                SelectEntry[] read_list;
                SelectEntry[] write_list;

                if (!chn_closed[0])
                    read_list ~= SelectEntry(chn1, &read_val[0]);
                if (!chn_closed[1])
                    read_list ~= SelectEntry(chn2, &read_val[1]);
                read_list ~= SelectEntry(chn3, &read_val[2]);

                auto select_return = select(read_list, write_list);

                if (!select_return.success)
                {
                    if (!chn_closed[0])  chn_closed[0] = true;
                    else if (!chn_closed[1])  chn_closed[1] = true;
                    else chn_closed[2] = true;
                }
            }
            assert(read_val[0] == 42 && read_val[1] == 44 && read_val[2] == 37);
            assert(chn_closed[0] && chn_closed[1] && !chn_closed[2]);
        }
    );
}

@system unittest
{
    FiberScheduler scheduler = new FiberScheduler;

    auto chn1 = new Channel!int(20);
    auto chn2 = new Channel!int();
    auto chn3 = new Channel!int(20);
    auto chn4 = new Channel!int();

    void thread_func (T) (ref T write_chn, ref T read_chn, int _tid)
    {
        FiberScheduler scheduler = new FiberScheduler;
        int read_val, write_val;
        int prev_read = -1;
        int n = 10000;

        scheduler.start(
            () {
                while(read_val < n || write_val <= n)
                {
                    int a;
                    SelectEntry[] read_list;
                    SelectEntry[] write_list;

                    if (write_val <= n)
                        write_list ~= SelectEntry(write_chn, &write_val);

                    if (read_val < n)
                        read_list ~= SelectEntry(read_chn, &read_val);

                    auto select_return = select(read_list, write_list);

                    if (select_return.success)
                    {
                        if (read_list.length > 0 && select_return.id == 0)
                        {
                            assert(prev_read + 1 == read_val);
                            prev_read = read_val;
                        }
                        else
                        {
                            write_val++;
                        }
                    }
                }
            }
        );
    }

    auto t1 = new Thread({
        thread_func(chn1, chn2, 0);
    });
    t1.start();

    auto t2 = new Thread({
        thread_func(chn2, chn3, 1);
    });
    t2.start();

    auto t3 = new Thread({
        thread_func(chn3, chn4, 2);
    });
    t3.start();

    thread_func(chn4, chn1, 3);

    thread_joinAll();
}

@system unittest
{
    FiberScheduler scheduler = new FiberScheduler;
    auto chn1 = new Channel!int();
    auto chn2 = new Channel!int();

    scheduler.spawn(
        () {
            FiberScheduler.yield();
            chn1.close();
        }
    );

    scheduler.spawn(
        () {
            FiberScheduler.yield();
            chn2.close();
        }
    );

    scheduler.spawn(
        () {
            for (int i = 0; i < 2; ++i)
            {
                int write_val = 42;
                SelectEntry[] read_list;
                SelectEntry[] write_list;
                write_list ~= SelectEntry(chn1, &write_val);
                auto select_return = select(read_list, write_list);

                assert(select_return.id == 0);
                assert(!select_return.success);
            }
        }
    );

    scheduler.start(
        () {
            for (int i = 0; i < 2; ++i)
            {
                int read_val;
                SelectEntry[] read_list;
                SelectEntry[] write_list;
                read_list ~= SelectEntry(chn2, &read_val);
                auto select_return = select(read_list, write_list);

                assert(select_return.id == 0);
                assert(!select_return.success);
            }
        }
    );
}

@system unittest
{
    import core.sync.semaphore;
    FiberScheduler scheduler = new FiberScheduler;
    auto chn1 = new Channel!int();

    auto start = MonoTime.currTime;
    Semaphore writer_sem = new Semaphore();
    Semaphore reader_sem = new Semaphore();

    auto t1 = new Thread({
        FiberScheduler scheduler = new FiberScheduler;
        scheduler.start(
            () {
                writer_sem.wait();
                assert(chn1.write(42));
            }
        );
    });
    t1.start();

    auto t2 = new Thread({
        FiberScheduler scheduler = new FiberScheduler;
        scheduler.start(
            () {
                int read_val;
                reader_sem.wait();
                assert(chn1.read(read_val));
                assert(read_val == 43);
            }
        );
    });
    t2.start();

    scheduler.start(
        () {
            int read_val;

            scope (failure) {
                reader_sem.notify();
                writer_sem.notify();
                chn1.close();
            }

            assert(!chn1.read(read_val, 1000.msecs));
            assert(MonoTime.currTime - start >= 1000.msecs);

            writer_sem.notify();
            assert(chn1.read(read_val, 1000.msecs));
            assert(read_val == 42);

            start = MonoTime.currTime;

            assert(!chn1.write(read_val + 1, 1000.msecs));
            assert(MonoTime.currTime - start >= 1000.msecs);

            reader_sem.notify();
            assert(chn1.write(read_val + 1, 1000.msecs));
        }
    );

    thread_joinAll();
}

/// A simple spinlock
struct SpinLock
{
    /// Spin until lock is free
    void lock() nothrow
    {
        while (!cas(&locked, false, true)) { }
    }

    /// Atomically unlock
    void unlock() nothrow
    {
        atomicStore!(MemoryOrder.rel)(locked, false);
    }

    /// Lock state
    private shared(bool) locked;
}

/// A Fiber level Semaphore
class FiberSemaphore
{

    /***********************************************************************

        Ctor

        Param:
            count = Initial count of FiberSemaphore

    ************************************************************************/

    this (size_t count = 0) nothrow
    {
        this.count = count;
    }

    /***********************************************************************

        Wait for FiberSemaphore count to be greater than 0

    ************************************************************************/

    void wait () nothrow
    {
        this.slock.lock();

        if (this.count > 0)
        {
            this.count--;
            this.slock.unlock();
            return;
        }
        auto entry = new SemaphoreQueueEntry();
        thisScheduler().addResource(entry);
        scope (exit) thisScheduler().removeResource(entry);
        this.queue.insert(entry);

        this.slock.unlock();
        entry.blocker.wait();
    }

    /***********************************************************************

        Increment the FiberSemaphore count

    ************************************************************************/

    void notify () nothrow
    {
        this.slock.lock();
        if (auto entry = this.dequeueEntry())
            entry.blocker.notify();
        else
            this.count++;
        this.slock.unlock();
    }

    ///
    private class SemaphoreQueueEntry : FiberScheduler.Resource
    {

        ///
        this () nothrow
        {
            assert(thisScheduler(), "Can not block with no FiberScheduler running!");
            this.blocker = thisScheduler(). new FiberBlocker();
        }

        /***********************************************************************

            Terminate SemaphoreQueueEntry so that it is neutralized

        ************************************************************************/

        void release () nothrow
        {
            if (!blocker.stopTimer())
                this.outer.notify();
        }

        /// FiberBlocker blocking the `Fiber`
        FiberScheduler.FiberBlocker blocker;
    }

    /***********************************************************************

        Dequeue a `SemaphoreQueueEntry` from the waiting queue

        Return:
            a valid SemaphoreQueueEntry or null

    ***********************************************************************/

    private SemaphoreQueueEntry dequeueEntry () nothrow
    {
        while (!this.queue.empty())
        {
            auto entry = this.queue.front();
            this.queue.removeFront();
            if (entry.blocker.shouldBlock() && entry.blocker.stopTimer())
            {
                return entry;
            }
        }
        return null;
    }

    ///
    private SpinLock slock;

    /// Waiting queue for Fibers
    private DList!SemaphoreQueueEntry queue;

    /// Current semaphore count
    private size_t count;
}

/// A Fiber level Mutex, essentially a binary FiberSemaphore
class FiberMutex : FiberSemaphore
{
    this () nothrow
    {
        super(1);
    }

    ///
    alias lock = wait;

    ///
    alias unlock = notify;

    ///
    alias lock_nothrow = lock;

    ///
    alias unlock_nothrow = unlock;
}

// Test releasing a queue entry
@system unittest
{
    FiberMutex mtx = new FiberMutex();
    int sharedVal;

    auto t1 = new Thread({
        FiberScheduler scheduler = new FiberScheduler;
        scheduler.start(
            () {
                mtx.lock();
                Thread.sleep(400.msecs);
                sharedVal += 1;
                mtx.unlock();
            }
        );
    });
    t1.start();

    auto t2 = new Thread({
        FiberScheduler scheduler = new FiberScheduler;
        Thread.sleep(100.msecs);

        scheduler.spawn(
            () {
                Thread.sleep(200.msecs);
                throw new Exception("");
            }
        );

        try
        {
            scheduler.start(
                () {
                    mtx.lock();
                    sharedVal += 1;
                    mtx.unlock();
                }
            );
        } catch (Exception e) { }
    });
    t2.start();

    auto t3 = new Thread({
        FiberScheduler scheduler = new FiberScheduler;
        scheduler.start(
            () {
                Thread.sleep(200.msecs);
                mtx.lock();
                assert(sharedVal == 1);
                sharedVal += 1;
                mtx.unlock();
            }
        );
    });
    t3.start();

    thread_joinAll();
}
