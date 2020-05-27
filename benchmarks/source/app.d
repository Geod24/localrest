module app;

import core.thread;
import std.algorithm;
import std.datetime.stopwatch;
import std.range;
import std.stdio;
import geod24.LocalRest;

import One_To_Many;

enum Nodes = 50;
enum Iterations = 10_000;

void main (string[] args)
{
    writeln("Testing sending from a single thread");
    runTest(/* Nodes: */ 1, /* Iterations: */ 1_000);
    runTest(/* Nodes: */ 4, /* Iterations: */ 1_000);
    runTest(/* Nodes: */ 8, /* Iterations: */ 1_000);
    runTest(/* Nodes: */ 32, /* Iterations: */ 1_000);
    runTest(/* Nodes: */ 50, /* Iterations: */ 1_000);

    runTest(/* Nodes: */ 1, /* Iterations: */ 10_000);
    runTest(/* Nodes: */ 4, /* Iterations: */ 10_000);
    runTest(/* Nodes: */ 8, /* Iterations: */ 10_000);
    runTest(/* Nodes: */ 32, /* Iterations: */ 10_000);
    runTest(/* Nodes: */ 50, /* Iterations: */ 10_000);
}
