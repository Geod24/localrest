/*******************************************************************************

    This is a low-level messaging API upon which more structured or restrictive
    APIs may be built.  The general idea is that every messageable entity is
    represented by a common handle type called a Tid, which allows messages to
    be sent to logical threads that are executing in both the current process
    and in external processes using the same interface.  This is an important
    aspect of scalability because it allows the components of a program to be
    spread across available resources with few to no changes to the actual
    implementation.

    A logical thread is an execution context that has its own stack and which
    runs asynchronously to other logical threads.  These may be preemptively
    scheduled kernel threads, fibers (cooperative user-space threads), or some
    other concept with similar behavior.

    he type of concurrency used when logical threads are created is determined
    by the Scheduler selected at initialization time.  The default behavior is
    currently to create a new kernel thread per call to spawn, but other
    schedulers are available that multiplex fibers across the main thread or
    use some combination of the two approaches.

    Note:
    Copied (almost verbatim) from Phobos at commit 3bfccf4f1 (2019-11-27)
    Changes are this notice, and the module rename, from `std.concurrency`
    to `geod24.concurrency`.

    Copyright: Copyright Sean Kelly 2009 - 2014.
    License:   <a href="http://www.boost.org/LICENSE_1_0.txt">Boost License 1.0</a>.
    Authors:   Sean Kelly, Alex Rønne Petersen, Martin Nowak
    Source:    $(PHOBOSSRC std/concurrency.d)

    Copyright Sean Kelly 2009 - 2014.
    Distributed under the Boost Software License, Version 1.0.
    (See accompanying file LICENSE_1_0.txt or copy at
    http://www.boost.org/LICENSE_1_0.txt)

*******************************************************************************/

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


/*******************************************************************************

    Thrown on the current thread receives an exit message
    from another thread.

*******************************************************************************/

public class SchedulingTerminated : Exception
{
    /// Ctor
    public this (string msg = "Scheduling terminated") @safe pure nothrow @nogc
    {
        super(msg);
    }
}


/*******************************************************************************

    Encapsulates all implementation-level data needed for scheduling.

    When defining a Scheduler, an instance of this struct must be associated
    with each logical thread.  It contains all implementation-level information
    needed by the internal API.

*******************************************************************************/

public struct ThreadInfo
{
    /// Storage of information required for scheduling, message passing, etc.
    public Object[string] objects;


    /***************************************************************************

        Gets a thread-local instance of ThreadInfo.

        Gets a thread-local instance of ThreadInfo, which should be used as the
        default instance when info is requested for a thread not created by the
        Scheduler.

    ***************************************************************************/

    public static @property ref thisInfo () nothrow
    {
        static ThreadInfo val;
        return val;
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

    public this (void function() fn, size_t sz = 0) @safe pure nothrow @nogc
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


/*******************************************************************************

    An Scheduler using Fibers.

    This is an example scheduler that creates a new Fiber per call to spawn
    and multiplexes the execution of all fibers within the main thread.

*******************************************************************************/

public class FiberScheduler
{

    /// Whether start() has been called
    private bool dispatching;


    /***************************************************************************

        This creates a new Fiber for the supplied op and then starts the
        dispatcher.

        Params:
            op = The delegate the fiber should call

    ***************************************************************************/

    public void start (void delegate () op)
    {
        create(op);
        dispatch();
    }


    /***************************************************************************

        This created a new Fiber for the supplied op and adds it to the
        dispatch list.

        Params:
            op = The delegate the fiber should call

    ***************************************************************************/

    public void spawn (void delegate () op) nothrow
    {
        create(op);
        FiberScheduler.yield();
    }


    /***************************************************************************

        If the caller is a scheduled Fiber, this yields execution to another
        scheduled Fiber.

    ***************************************************************************/

    public static void yield () nothrow
    {
        // NOTE: It's possible that we should test whether the calling Fiber
        //       is an InfoFiber before yielding, but I think it's reasonable
        //       that any fiber should yield here.
        if (Fiber.getThis())
            Fiber.yield();
    }

    /***************************************************************************

     Returns a Condition analog that yields when wait or notify is called.

        Bug:
        For the default implementation, `notifyAll`will behave like `notify`.

        Params:
          m = A `Mutex` to use for locking if the condition needs to be waited on
            or notified from multiple `Thread`s.
            If `null`, no `Mutex` will be used and it is assumed that the
            `Condition` is only waited on/notified from one `Thread`.

    ***************************************************************************/

    public Condition newCondition (Mutex m) nothrow
    {
        return new FiberCondition();
    }


    /***************************************************************************

        Creates a new Fiber which calls the given delegate.

        Params:
            op = The delegate the fiber should call

    ***************************************************************************/

    protected void create (void delegate () op) nothrow
    {
        void wrap()
        {
            op();
        }

        m_fibers ~= new InfoFiber(&wrap);
    }

    /***************************************************************************

        Fiber which embeds a ThreadInfo

    ***************************************************************************/

    public static class InfoFiber : Fiber
    {
        /// Ctor
        public this (void delegate () op, size_t sz = 16 * 1024 * 1024) nothrow
        {
            super(op, sz);
        }
    }


    /***************************************************************************

        A condition variable allows one or more fibers to wait until
        they are notified by another fiber.

    ***************************************************************************/

    protected class FiberCondition : Condition
    {
        /// Ctor
        public this () nothrow
        {
            super(null);
            notified = false;
        }


        /***********************************************************************

            Wait until notified.

        ***********************************************************************/

        public override void wait () nothrow
        {
            scope (exit) notified = false;

            while (!notified)
                FiberScheduler.yield();
        }


        /***********************************************************************

            Suspends the calling thread until a notification occurs or until
            the supplied time period has elapsed.

            Params:
                period = The time to wait.

        ***********************************************************************/

        public override bool wait (Duration period) nothrow
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


        /***********************************************************************

            Notifies one waiter.

        ***********************************************************************/

        public override void notify () nothrow
        {
            notified = true;
            FiberScheduler.yield();
        }


        /***********************************************************************

            Notifies all waiters.

        ***********************************************************************/

        public override void notifyAll () nothrow
        {
            notified = true;
            FiberScheduler.yield();
        }

        /// When notify() is called, this value is set true.
        private bool notified;
    }

    /***************************************************************************

        One thread executes this.
        This function is executed by circulating the fibers until
        all the fibers activity is finished.
        If OwnerTerminated occurs while running, all operations stop.

    ***************************************************************************/

    private void dispatch ()
    {
        import std.algorithm.mutation : remove;

        assert(!this.dispatching, "Already called start(). Scheduling already started.");

        this.dispatching = true;
        scope (exit) this.dispatching = false;

        while (m_fibers.length > 0)
        {
            auto t = m_fibers[m_pos].call(Fiber.Rethrow.no);
            if (t !is null)
            {
                if (cast(SchedulingTerminated) t)
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

    private Fiber[] m_fibers;
    private size_t m_pos;
}

version (unittest)
{
    import std.variant;

    public struct TestMessage
    {
        Variant data;

        this (T) (T val)
        {
            data = val;
        }

        @property auto convertsTo (T)()
        {
            return is(T == Variant) || data.convertsTo!(T);
        }
    }

    public alias ChannelT = Channel!TestMessage;

    // Put it in a TestMessage.
    public static TestMessage toTestMessage (T) (T data)
    {
        return TestMessage(data);
    }
}

/// Test for the use `SchedulingTerminated`
unittest
{
    import std.conv;

    // Data sent by the caller
    struct Request
    {
        ChannelT sender;
        string method;
        string args;
    }

    // Data sent by the callee back to the caller
    struct Response
    {
        string data;
    }

    /// Control command when exiting
    struct Shutdown
    {
        bool throw_except;
    }

    ChannelT spawnThread (void function (ChannelT chan) op)
    {
        auto spawnChannel = new ChannelT(256);
        void exec()
        {
            thisScheduler = new FiberScheduler();
            op(spawnChannel);
        }

        auto t = new InfoThread(&exec);
        t.start();

        return spawnChannel;
    }

    // Send control command to exit.
    static void shutdown (ChannelT chan, bool throw_except)
    {
        chan.send(toTestMessage(Shutdown(throw_except)));
    }

    // Handle requests.
    static void sub_task (Request req)
    {
        auto scheduler = thisScheduler;
        scheduler.spawn({
            if (req.method == "pow")
            {
                immutable int value = to!int(req.args);
                auto msg_res = toTestMessage(Response(to!string(value * value)));
                req.sender.send(msg_res);
            }
            else if (req.method == "loop")
            {
                immutable int value = to!int(req.args);
                auto condition = scheduler.newCondition(null);
                condition.wait(seconds(value));
                auto msg_res = toTestMessage(Response(""));
                req.sender.send(msg_res);
            }
            else
            {
                assert(0, "Unmatched method name: " ~ req.method);
            }
        });
    }

    // This is a function to running on a thread.
    static void main_task (ChannelT self)
    {
        auto scheduler = thisScheduler;
        auto channel = self;
        bool terminate = false;

        scheduler.start({
            while (!terminate)
            {
                TestMessage msg = channel.receive();

                // On received a `Request`
                if (msg.convertsTo!(Request))
                {
                    auto req = msg.data.peek!(Request);
                    sub_task(*req);
                }
                // On received a `Shutdown`
                else if (msg.convertsTo!(Shutdown))
                {
                    terminate = true;
                    auto value = msg.data.peek!(Shutdown);
                    if (value.throw_except)
                    {
                        throw new SchedulingTerminated();
                    }
                }
                else
                {
                    assert(0, "Unexpected type");
                }
            }
        });
    }

    auto server = spawnThread(&main_task);
    auto client = new ChannelT(256);

    server.send(toTestMessage(Request(client, "pow", "2")));

    auto res_msg = client.receive();
    auto res = res_msg.data.peek!(Response);
    assert(res.data == "4");

    server.send(toTestMessage(Request(client, "loop", "5")));

    // All tasks are terminated immediately.
    shutdown(server, true);

    // After the test have been completed,
    // the main thread will not be terminated immediately.
    version(none) shutdown(server, false);
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
    private     DList!T queue;

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

    ***************************************************************************/

    public void send (T msg)
    {
        this.mutex.lock();

        if (this.closed)
        {
            this.mutex.unlock();
            assert(0, "Channel is closed.");
        }

        if (!this.recvq.empty)
        {
            ChannelContext!T context = this.recvq.front;
            this.recvq.removeFront();
            *(context.msg_ptr) = msg;
            this.mutex.unlock();
            context.notify();

            return;
        }

        if (this.mcount < this.qsize)
        {
            this.queue.insertBack(msg);
            this.mcount++;
            this.mutex.unlock();

            return;
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
        }
        else
        {
            auto mutex = new Mutex();
            waitForReader(mutex, new Condition(mutex));
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

        assert(!this.closed, "Channel was already closed!");
        this.closed = true;

        while (!this.recvq.empty)
        {
            context = this.recvq.front;
            this.recvq.removeFront();
            context.notify();
        }

        this.queue.clear();
        this.mcount = 0;

        while (!this.sendq.empty)
        {
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

    /// This is a message pointer. Used in get
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

/// Test for Channel closing
unittest
{
    thisScheduler = new FiberScheduler();
    thisScheduler.start({

        // Create Channel
        auto chan = new Channel!int(1);

        assert(!chan.isClosed);

        // Send message
        chan.send(42);

        // Receive a message
        assert(chan.receive() == 42);

        // Close then Channel
        chan.close();

        assert(chan.isClosed);

        // Returns false on a closed Channel.
        int msg;
        assert(!chan.tryReceive(&msg));
    });
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

/// Parent thread and child thread communicating with 2 channels
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


/// Parent thread and fiber of child thread communicating with 2 channels
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

/// Parent thread's fiber and child thread communicating with 2 channels
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

/// 2 fibers in a child thread communicating with each other using 2 channels
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


/// Fiber in parent thread communicating with fiber in child thread using 2 channels
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


/// Two child threads communicating with each other with 2 channels
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


/// One child thread communicating with another child thread's fiber using 2 channels
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


// Test blocking behavior on threads when using unbuffered channels
unittest
{
    auto channel1 = new Channel!int(0);
    auto channel2 = new Channel!int(1);
    int result = 0;

    Mutex mutex = new Mutex;
    Condition condition = new Condition(mutex);

    // Thread1 - It'll be blocked.
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

    // Thread2 - Unblock a thread
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
    // Thread3 - It'll not be blocked, because queue size is 1
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


// Test blocking behavior on fibers when using unbuffered channels
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
            //  Fiber1 - It'll be blocked.
            scheduler.spawn({
                channel1.send(2);
                result = channel1.receive();
                cond.notify();
            });

            assert(!cond.wait(1000.msecs));
            assert(result == 0);

            //  Fiber2 - Unblock a fiber
            scheduler.spawn({
                result = channel1.receive();
                channel1.send(2);
            });

            cond.wait(1000.msecs);
            assert(result == 2);

            //  Fiber3 - It'll not be blocked, because queue size is 1
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
