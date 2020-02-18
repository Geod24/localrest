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
/*          Copyright Sean Kelly 2009 - 2014.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          http://www.boost.org/LICENSE_1_0.txt)
 */
module geod24.concurrency;

import core.sync.condition;
import core.sync.mutex;
import core.thread;
import std.container;


public @property ref ThreadInfo thisInfo() nothrow
{
    auto t = cast(InfoThread)Thread.getThis();

    if (t !is null)
        return t.info;

    return ThreadInfo.thisInfo;
}

/**
 * Thrown on calls to `receive` if the thread that spawned the receiving
 * thread has terminated and no more messages exist.
 */
class OwnerTerminated : Exception
{
    ///
    this(string msg = "Owner terminated") @safe pure nothrow @nogc
    {
        super(msg);
    }
}


/**
 * Encapsulates all implementation-level data needed for scheduling.
 *
 * When defining a Scheduler, an instance of this struct must be associated
 * with each logical thread.  It contains all implementation-level information
 * needed by the internal API.
 */
struct ThreadInfo
{
    /// Storage of information required for scheduling, message passing, etc.
    public Object[string] objects;

    /**
     * Gets a thread-local instance of ThreadInfo.
     *
     * Gets a thread-local instance of ThreadInfo, which should be used as the
     * default instance when info is requested for a thread not created by the
     * Scheduler.
     */
    static @property ref thisInfo() nothrow
    {
        static ThreadInfo val;
        return val;
    }

    /**
     * Cleans up this ThreadInfo.
     *
     * This must be called when a scheduled thread terminates.  It tears down
     * the messaging system for the thread and notifies interested parties of
     * the thread's termination.
     */
    void cleanup()
    {
    }
}


/***************************************************************************

    Getter of FiberScheduler assigned to a called thread.

***************************************************************************/

public @property FiberScheduler thisScheduler () nothrow
{
    auto p = ("scheduler" in thisInfo.objects);
    if (p !is null)
        return cast(FiberScheduler)*p;
    else
        return null;
}


/***************************************************************************

    Setter of FiberScheduler assigned to a called thread.

***************************************************************************/

public @property void thisScheduler (FiberScheduler value) nothrow
{
    thisInfo.objects["scheduler"] = value;
}


/***************************************************************************

    Thread with ThreadInfo,
    This is implemented to avoid using global variables.

***************************************************************************/

public class InfoThread : Thread
{
    public ThreadInfo info;

    /***************************************************************************

        Initializes a thread object which is associated with a static

        Params:
            fn = The thread function.
            sz = The stack size for this thread.

    ***************************************************************************/

    this (void function() fn, size_t sz = 0) @safe pure nothrow @nogc
    {
        super(fn, sz);
    }


    /***************************************************************************

        Initializes a thread object which is associated with a dynamic

        Params:
            dg = The thread function.
            sz = The stack size for this thread.

    ***************************************************************************/

    this (void delegate() dg, size_t sz = 0) @safe pure nothrow @nogc
    {
        super(dg, sz);
    }
}


/**
 * An example Scheduler using Fibers.
 *
 * This is an example scheduler that creates a new Fiber per call to spawn
 * and multiplexes the execution of all fibers within the main thread.
 */
class FiberScheduler
{
    private bool dispatching;

    /**
     * This creates a new Fiber for the supplied op and then starts the
     * dispatcher.
     */
    void start(void delegate() op)
    {
        create(op);
        dispatch();
    }

    /**
     * This created a new Fiber for the supplied op and adds it to the
     * dispatch list.
     */
    void spawn(void delegate() op) nothrow
    {
        create(op);
        FiberScheduler.yield();
    }

    /**
     * If the caller is a scheduled Fiber, this yields execution to another
     * scheduled Fiber.
     */
    static void yield() nothrow
    {
        // NOTE: It's possible that we should test whether the calling Fiber
        //       is an InfoFiber before yielding, but I think it's reasonable
        //       that any fiber should yield here.
        if (Fiber.getThis())
            Fiber.yield();
    }

    /**
     * Returns a Condition analog that yields when wait or notify is called.
     *
     * Bug:
     * For the default implementation, `notifyAll`will behave like `notify`.
     *
     * Params:
     *   m = A `Mutex` to use for locking if the condition needs to be waited on
     *       or notified from multiple `Thread`s.
     *       If `null`, no `Mutex` will be used and it is assumed that the
     *       `Condition` is only waited on/notified from one `Thread`.
     */
    Condition newCondition(Mutex m) nothrow
    {
        return new FiberCondition();
    }

protected:
    /**
     * Creates a new Fiber which calls the given delegate.
     *
     * Params:
     *   op = The delegate the fiber should call
     */
    void create(void delegate() op) nothrow
    {
        void wrap()
        {
            op();
        }

        m_fibers ~= new InfoFiber(&wrap);
    }

    /**
     * Fiber which embeds a ThreadInfo
     */
    static class InfoFiber : Fiber
    {
        this(void delegate() op, size_t sz = 16 * 1024 * 1024) nothrow
        {
            super(op, sz);
        }
    }

    protected class FiberCondition : Condition
    {
        this() nothrow
        {
            super(null);
            notified = false;
        }

        override void wait() nothrow
        {
            scope (exit) notified = false;

            while (!notified)
                FiberScheduler.yield();
        }

        override bool wait(Duration period) nothrow
        {
            import core.time : MonoTime;

            scope (exit) notified = false;

            for (auto limit = MonoTime.currTime + period;
                 !notified && !period.isNegative;
                 period = limit - MonoTime.currTime)
            {
                FiberScheduler.yield();
            }
            return notified;
        }

        override void notify() nothrow
        {
            notified = true;
            FiberScheduler.yield();
        }

        override void notifyAll() nothrow
        {
            notified = true;
            FiberScheduler.yield();
        }

        private bool notified;
    }

private:
    void dispatch()
    {
        import std.algorithm.mutation : remove;

        assert(!this.dispatching, "Already called start. Scheduling already started.");

        this.dispatching = true;
        scope (exit) this.dispatching = false;

        while (m_fibers.length > 0)
        {
            auto t = m_fibers[m_pos].call(Fiber.Rethrow.no);
            if (t !is null)
            {
                if (cast(OwnerTerminated) t)
                    break;
                else
                    throw t;
            }
            if (m_fibers[m_pos].state == Fiber.State.TERM)
            {
                if (m_pos >= (m_fibers = remove(m_fibers, m_pos)).length)
                    m_pos = 0;
            }
            else if (m_pos++ >= m_fibers.length - 1)
            {
                m_pos = 0;
            }
        }
    }

private:
    Fiber[] m_fibers;
    size_t m_pos;
}


/*******************************************************************************

    This is similar to go channels.

    If you set the size of the queue to 0 in the constructor,
    you can create unbuffered channel.

    There are two kinds of fiber(thread). One reads, and one writes.

    1.  buffered channel
        When the write-fiber(thread) is working,
        it puts the message in the queue.

        When the read-fiber(thread) is working,
        take the message out of the queue and take it.
        If there are no messages in the queue, wait for write-fiber(thread) to put
        the messages in the queue.

    2.  unbuffered channel
        When the write-fiber(thread) is working,
        it sends the data to the read-fiber(thread) if it already exists.
        Otherwise, the write-fiber(thread) waits for the read-fiber(thread)
        to come in.

        When read-fiber(thread) works,
        it imports data from the write-fiber(thread) if it already exists.
        Otherwise, the read-fiber(thread) waits for the write-fiber(thread)
        to come in.

    Params:
        T = The type of message to deliver. (int, string, struct, ......)

*******************************************************************************/

public class Channel (T)
{
    /// closed
    private bool closed;

    /// lock for queue and status
    private Mutex mutex;

    /// size of queue
    private size_t qsize;

    /// queue of data
    private DList!T queue;

    /// the number of message in queue
    private size_t mcount;

    /// collection of send waiters
    private DList!(ChannelContext!T) sendq;

    /// collection of recv waiters
    private DList!(ChannelContext!T) recvq;


    /***************************************************************************

        Creator

        Params:
            qsize = If this is 0, it becomes a unbuffered channel.
            Otherwise, it has a buffer size as large as qsize.

    ***************************************************************************/

    public this (size_t qsize = 0)
    {
        this.closed = false;
        this.mutex = new Mutex;
        this.qsize = qsize;
        this.mcount = 0;
    }


    /***************************************************************************

        Send data `msg`.

        Params:
            msg = message to send

        Return:
            If successful, return true, otherwise return false

    ***************************************************************************/

    public bool send (T msg)
    {
        this.mutex.lock();

        if (this.closed)
        {
            this.mutex.unlock();
            return false;
        }

        if (!this.recvq.empty)
        {
            ChannelContext!T context = this.recvq.front;
            this.recvq.removeFront();
            *(context.msg_ptr) = msg;
            this.mutex.unlock();
            context.notify();
            return true;
        }

        if (this.mcount < this.qsize)
        {
            this.queue.insertBack(msg);
            this.mcount++;
            this.mutex.unlock();
            return true;
        }

        void waitForReader (Mutex mutex, Condition condition)
        {
            ChannelContext!T new_context;
            new_context.msg_ptr = null;
            new_context.msg = msg;
            new_context.mutex = mutex;
            new_context.condition = condition;
            this.sendq.insertBack(new_context);
            this.mutex.unlock();
            new_context.wait();
        }

        // queue is full or channel is unbuffered
        auto scheduler = thisScheduler;
        if (Fiber.getThis() && scheduler !is null)
        {
            waitForReader(null, scheduler.newCondition(null));
            return true;
        }
        else
        {
            auto mutex = new Mutex();
            waitForReader(mutex, new Condition(mutex));
            return true;
        }
    }


    /***************************************************************************

        Return the received message.

        Return:
            msg = message to receive

    ***************************************************************************/

    public T receive ()
    {
        this.mutex.lock();

        if (this.closed)
        {
            this.mutex.unlock();
            assert(0, "Channel is closed.");
        }

        if (!this.sendq.empty)
        {
            T msg;
            ChannelContext!T context = this.sendq.front;
            this.sendq.removeFront();
            msg = context.msg;
            this.mutex.unlock();
            context.notify();
            return msg;
        }

        if (!this.queue.empty)
        {
            T msg;
            msg = this.queue.front;
            this.queue.removeFront();
            assert(this.mcount > 0, "mcount was zero!");
            this.mcount--;
            this.mutex.unlock();
            return msg;
        }

        T waitForSender (Mutex mutex, Condition condition)
        {
            T msg;
            ChannelContext!T new_context;
            new_context.msg_ptr = &msg;
            new_context.mutex = mutex;
            new_context.condition = condition;
            this.recvq.insertBack(new_context);
            this.mutex.unlock();
            new_context.wait();
            return msg;
        }

        auto scheduler = thisScheduler;
        if (Fiber.getThis() && scheduler !is null)
        {
            return waitForSender(null, scheduler.newCondition(null));
        }
        else
        {
            auto mutex = new Mutex();
            return waitForSender(mutex, new Condition(mutex));
        }
    }


    /***************************************************************************

	    Return the received message.

        Params:
            msg = point of message to send

        Return:
            If successful, return true, otherwise return false

    ***************************************************************************/

    public bool tryReceive (T* msg)
    {
        assert(msg !is null);

        this.mutex.lock();

        if (this.closed)
        {
            this.mutex.unlock();
            return false;
        }

        if (!this.sendq.empty)
        {
            ChannelContext!T context = this.sendq.front;
            this.sendq.removeFront();
            *(msg) = context.msg;
            this.mutex.unlock();
            context.notify();
            return true;
        }

        if (!this.queue.empty)
        {
            *(msg) = this.queue.front;
            this.queue.removeFront();
            assert(this.mcount > 0, "mcount was zero!");
            this.mcount--;
            this.mutex.unlock();
            return true;
        }

        this.mutex.unlock();
        return false;
    }


    /***************************************************************************

        Return closing status

        Return:
            If channel is closed, return true, otherwise return false

    ***************************************************************************/

    public @property bool isClosed ()
    {
        synchronized (this.mutex)
        {
            return this.closed;
        }
    }


    /***************************************************************************

        Called when the channel is no longer in use.
        This function turns off Fiber and Thread, which were waiting for
        the message to pass.
        And remove the message that was in the queue.
        And once closed, the channel is no longer available for message passing.

    ***************************************************************************/

    public void close ()
    {
        ChannelContext!T context;

        this.mutex.lock();
        scope (exit) this.mutex.unlock();

        this.closed = true;

        while (true)
        {
            if (this.recvq.empty)
                break;

            context = this.recvq.front;
            this.recvq.removeFront();
            context.notify();
        }

        this.queue.clear();
        this.mcount = 0;

        while (true)
        {
            if (this.sendq.empty)
                break;

            context = this.sendq.front;
            this.sendq.removeFront();
            context.notify();
        }
    }
}

/***************************************************************************

    A structure to be stored in a queue.
    It has information to use in standby.

***************************************************************************/

private struct ChannelContext (T)
{
    /// This is a message. Used in put
    public T  msg;

    /// This is a message point. Used in get
    public T* msg_ptr;

    //  Waiting Condition
    public Condition condition;

    /// lock for thread waiting
    public Mutex mutex;
}

private void wait (T) (ChannelContext!T context)
{
    if (context.condition is null)
        return;

    if (context.mutex is null)
        context.condition.wait();
    else
    {
        synchronized(context.mutex)
        {
            context.condition.wait();
        }
    }
}

private void notify (T) (ChannelContext!T context)
{
    if (context.condition is null)
        return;

    if (context.mutex is null)
        context.condition.notify();
    else
    {
        synchronized(context.mutex)
        {
            context.condition.notify();
        }
    }
}

/// Test of a buffered channel
unittest
{
    void mainTask ()
    {
        void otherTask (T) (Channel!T chan)
        {
            thisScheduler.spawn({
                assert(chan.receive() == "Hello World");
                assert(chan.receive() == "Handing off");
            });
        }

        auto chan = new Channel!string(2);

        otherTask(chan);

        chan.send("Hello World");
        chan.send("Handing off");
    }

    /// Create FiberScheduler of main thread
    thisScheduler = new FiberScheduler();
    thisScheduler.start({
        mainTask();
    });
}

/// Test of a unbuffered channel
unittest
{
    string[] results;
    void mainTask ()
    {
        void otherTask (T) (Channel!T chan)
        {
            thisScheduler.spawn({
                results ~= "+ Ping";
                chan.receive();
                results ~= "+ Ping";
                chan.receive();
                results ~= "+ Ping";
                chan.receive();
            });
        }

        auto chan = new Channel!int(0);

        otherTask(chan);

        results ~= "* Ping";
        chan.send(42);
        results ~= "* Ping";
        chan.send(42);
        results ~= "* Ping";
        chan.send(42);
        results ~= "* Boom";
    }

    /// Create FiberScheduler of main thread
    thisScheduler = new FiberScheduler();
    thisScheduler.start({
        mainTask();
    });

    assert(results == ["+ Ping", "* Ping", "+ Ping", "* Ping", "+ Ping", "* Ping", "* Boom"]);
}

/// spawn thread
version (unittest) void spawnThread (void delegate() op)
{
    auto t1 = new InfoThread({
        thisScheduler = new FiberScheduler();
        op();
    });
    t1.start();
}

/// Min Thread -> [ channel1 ] -> Thread1 -> [ channel2 ] -> Min Thread
unittest
{
    auto channel1 = new Channel!int;
    auto channel2 = new Channel!int;

    // Thread1
    spawnThread({
        int msg = channel1.receive();
        channel2.send(msg*msg);
    });

    // Main Thread1
    channel1.send(2);
    assert(channel2.receive() == 4);
}


/// Min Thread -> [ channel1 ] -> Fiber1 in Thread1 -> [ channel2 ] -> Min Thread
unittest
{
    auto channel1 = new Channel!int;
    auto channel2 = new Channel!int;

    // Thread1
    spawnThread({
        // Fiber1
        thisScheduler.start({
            int msg = channel1.receive();
            channel2.send(msg*msg);
        });
    });

    // Main Thread
    channel1.send(2);
    assert(channel2.receive() == 4);
}

/// Fiber in Min Thread -> [ channel1 ] -> Fiber1 in Thread1 -> [ channel2 ] -> Fiber in Min Thread
unittest
{
    auto channel1 = new Channel!int;
    auto channel2 = new Channel!int;
    int result;

    // Thread1
    spawnThread({
        // Fiber1
        thisScheduler.start({
            int msg = channel1.receive();
            channel2.send(msg*msg);
        });
    });

    // Main Thread
    thisScheduler = new FiberScheduler();
    thisScheduler.start({
        channel1.send(2);
        result = channel2.receive();
    });
    assert(result == 4);
}

/// Fiber1 -> [ channel2 ] -> Fiber2 -> [ channel1 ] -> Fiber1
unittest
{
    auto channel1 = new Channel!int;
    auto channel2 = new Channel!int;
    int result;

    Mutex mutex = new Mutex;
    Condition condition = new Condition(mutex);

    // Thread1
    spawnThread({
        scope scheduler = thisScheduler;
        scheduler.start({
            //  Fiber1
            scheduler.spawn({
                channel2.send(2);
                result = channel1.receive();
                synchronized (mutex)
                {
                    condition.notify;
                }
            });
            //  Fiber2
            scheduler.spawn({
                int msg = channel2.receive();
                channel1.send(msg*msg);
            });
        });
    });

    synchronized (mutex)
    {
        condition.wait(1000.msecs);
    }

    assert(result == 4);
}


/// Fiber1 in Thread1 -> [ channel2 ] -> Fiber2 in Thread2 -> [ channel1 ] -> Fiber1 in Thread1
unittest
{
    auto channel1 = new Channel!int;
    auto channel2 = new Channel!int;
    int result;

    Mutex mutex = new Mutex;
    Condition condition = new Condition(mutex);

    // Thread1
    spawnThread({
        // Fiber1
        thisScheduler.start({
            channel2.send(2);
            result = channel1.receive();
            synchronized (mutex)
            {
                condition.notify;
            }
        });
    });

    // Thread2
    spawnThread({
        // Fiber2
        thisScheduler.start({
            int msg = channel2.receive();
            channel1.send(msg*msg);
        });
    });

    synchronized (mutex)
    {
        condition.wait(1000.msecs);
    }
    assert(result == 4);
}


/// Thread1 -> [ channel2 ] -> Thread2 -> [ channel1 ] -> Thread1
unittest
{
    auto channel1 = new Channel!int;
    auto channel2 = new Channel!int;
    int result;

    Mutex mutex = new Mutex;
    Condition condition = new Condition(mutex);

    // Thread1
    spawnThread({
        channel2.send(2);
        result = channel1.receive();
        synchronized (mutex)
        {
            condition.notify;
        }
    });

    // Thread2
    spawnThread({
        int msg = channel2.receive();
        channel1.send(msg*msg);
    });

    synchronized (mutex)
    {
        condition.wait(3000.msecs);
    }

    assert(result == 4);
}


/// Thread1 -> [ channel2 ] -> Fiber1 in Thread 2 -> [ channel1 ] -> Thread1
unittest
{
    auto channel1 = new Channel!int;
    auto channel2 = new Channel!int;
    int result;

    Mutex mutex = new Mutex;
    Condition condition = new Condition(mutex);

    // Thread1
    spawnThread({
        channel2.send(2);
        result = channel1.receive();
        synchronized (mutex)
        {
            condition.notify;
        }
    });

    // Thread2
    spawnThread({
        // Fiber1
        thisScheduler.start({
            int msg = channel2.receive();
            channel1.send(msg*msg);
        });
    });

    synchronized (mutex)
    {
        condition.wait(1000.msecs);
    }
    assert(result == 4);
}


// If the queue size is 0, it will block when it is sent and received on the same thread.
unittest
{
    auto channel1 = new Channel!int(0);
    auto channel2 = new Channel!int(1);
    int result = 0;

    Mutex mutex = new Mutex;
    Condition condition = new Condition(mutex);

    // Thread1 - It'll be tangled.
    spawnThread({
        channel1.send(2);
        result = channel1.receive();
        synchronized (mutex)
        {
            condition.notify;
        }
    });

    synchronized (mutex)
    {
        condition.wait(1000.msecs);
    }
    assert(result == 0);

    // Thread2 - Unravel a tangle
    spawnThread({
        result = channel1.receive();
        channel1.send(2);
    });

    synchronized (mutex)
    {
        condition.wait(1000.msecs);
    }
    assert(result == 2);

    result = 0;
    // Thread3 - It'll not be tangled, because queue size is 1
    spawnThread({
        channel2.send(2);
        result = channel2.receive();
        synchronized (mutex)
        {
            condition.notify;
        }
    });

    synchronized (mutex)
    {
        condition.wait(1000.msecs);
    }
    assert(result == 2);
}


// If the queue size is 0, it will block when it is sent and received on the same fiber.
unittest
{
    auto channel1 = new Channel!int(0);
    auto channel2 = new Channel!int(1);
    int result = 0;

    // Thread1
    spawnThread({

        scope scheduler = thisScheduler;
        scope cond = scheduler.newCondition(null);

        scheduler.start({
            //  Fiber1 - It'll be tangled.
            scheduler.spawn({
                channel1.send(2);
                result = channel1.receive();
                cond.notify();
            });

            assert(!cond.wait(1000.msecs));
            assert(result == 0);

            //  Fiber2 - Unravel a tangle
            scheduler.spawn({
                result = channel1.receive();
                channel1.send(2);
            });

            cond.wait(1000.msecs);
            assert(result == 2);

            //  Fiber3 - It'll not be tangled, because queue size is 1
            scheduler.spawn({
                channel2.send(2);
                result = channel2.receive();
                cond.notify();
            });

            cond.wait(1000.msecs);
            assert(result == 2);
        });
    });
}
