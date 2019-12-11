/*******************************************************************************

    Contains an implementation of a virtual clock. The clock by default follows
    the monotonic clock, but may be paused, resumed, and can even time-travel
    by settings its time offset.

    Author:         Andrej Mitrovic
    License:        MIT (See LICENSE.txt)
    Copyright:      Copyright (c) 2018-2019 Mathias Lang. All rights reserved.

*******************************************************************************/

module geod24.VirtualClock;

import std.datetime.stopwatch;

import core.time;

/// Ditto
public class VirtualClock
{
    /// Whether the clock has been paused
    private bool paused;

    /// Stopwatch used to track progress of time
    private StopWatch sw;

    /// The initial start time of the clock
    private const MonoTime start_time;


    /***************************************************************************

        Constructor

        The virtual clock begins keeping track of time right away.

    ***************************************************************************/

    public this () nothrow @nogc @safe
    {
        this.start_time = MonoTime.currTime();
        this.sw = StopWatch(AutoStart.yes);
    }

    /***************************************************************************

        Returns:
            the current virtual time

    ***************************************************************************/

    public MonoTime currTime () nothrow @nogc @safe
    {
        return this.start_time + this.sw.peek();
    }

    /***************************************************************************

        Pauses the clock.

    ***************************************************************************/

    public void pause () nothrow @nogc @safe
    {
        this.paused = true;
        this.sw.stop();
    }

    /***************************************************************************

        Resume the clock from the last paused time.
        If the clock was not paused it's a no-op.

    ***************************************************************************/

    public void resume () nothrow @nogc @safe
    {
        if (!this.paused)
            return;

        this.sw.start();
        this.paused = false;
    }

    /***************************************************************************

        Add a time offset to the virtual clock.

        Note: the offset may be negative, to "time-travel" into the past

    ***************************************************************************/

    public void addTimeOffset (Duration offset) nothrow @nogc @safe
    {
        this.sw.setTimeElapsed(this.sw.peek() + offset);
    }
}

/// Ditto
unittest
{
    import core.thread;
    auto vc = new VirtualClock();
    vc.pause();
    auto pause_time = vc.currTime();

    Thread.sleep(10.msecs);
    assert(vc.currTime() == pause_time);

    vc.resume();
    vc.resume();  // no-op
    Thread.sleep(10.msecs);
    assert(vc.currTime() > pause_time);

    vc.pause();
    auto new_time = vc.currTime();

    // adds a negative offset
    vc.addTimeOffset(-(new_time - pause_time));
    assert(vc.currTime() == pause_time);

    // adds a positive offset
    vc.addTimeOffset(50.msecs);
    assert(vc.currTime() == pause_time + 50.msecs);
}
