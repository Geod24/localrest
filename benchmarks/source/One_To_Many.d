/*******************************************************************************

    Parametized test for the "one to many" scenario, when the many don't
    communicate

*******************************************************************************/

module One_To_Many;

import core.thread;
import std.algorithm;
import std.datetime.stopwatch;
import std.range;
import std.stdio;
import geod24.LocalRest;

alias StringAPI = DataAPI!string;
alias StringNode = DataNode!string;

alias WordAPI = DataAPI!size_t;
alias WordNode = DataNode!size_t;

interface DataAPI (T)
{
    /* MonoTime */ ulong receive (T);
    T forward (T);
}

final class DataNode (T) : DataAPI!T
{
    override ulong receive (T value)
    {
        return MonoTime.currTime.ticks();
    }

    override T forward (T value)
    {
        return value;
    }
}

void runTest (size_t node_count, size_t iterations)
{
    // Setup
    RemoteAPI!(StringAPI)[] nodes =
        iota(node_count).map!(_ => RemoteAPI!StringAPI.spawn!StringNode()).array;
    Duration[][] timings = new Duration[][node_count];
    foreach (i; 0 .. node_count) timings[i] = new Duration[](iterations);
    StopWatch sw = StopWatch(AutoStart.no);

    // Test
    sw.start();
    foreach (count; 0 .. iterations)
    {
        foreach (node_index, node; nodes)
        {
            ulong hnsecs = node.receive("Hello darkness my old friend");
            timings[node_index][count] = MonoTime.currTime - *(cast(MonoTime*) &hnsecs);
        }
    }
    sw.stop();

    // Results & teardown
    writefln("Sending %d messages to %d nodes took took %s total",
             iterations, node_count, sw.peek());
    writefln("The average response time was: %s",
            dur!"hnsecs"(cast(ulong) timings[].frontTransversal.map!(d => d.total!"hnsecs").mean()));
    foreach (idx, node; nodes)
        node.ctrl.shutdown();
    thread_joinAll();
}
