/*******************************************************************************

    Contains a ring buffer implementation with fixed size

    Copyright:
        Copyright (c) 2020 BOS Platform Foundation Korea
        All rights reserved.

    License:
        MIT License. See LICENSE for details.

*******************************************************************************/

module geod24.RingBuffer;

import std.range;

/*******************************************************************************

    A ring buffer with fixed size

    Params:
        T = Type of elements

*******************************************************************************/

public class RingBuffer (T)
{
    /*******************************************************************************

        Params:
            size = Maximum number of elements, must be greater than 0

    *******************************************************************************/

    this (size_t size) @safe pure nothrow
    {
        assert(size, "Cannot create a RingBuffer with size of 0");
        this.storage.length = size;
        this.write_range = cycle(this.storage[]);
        this.read_range = cycle(this.storage[]);
    }

    /// Returns: true if full
    public bool full () @safe @nogc pure nothrow
    {
        return this.len == this.storage.length;
    }

    /// Returns: true if empty
    public bool empty () @safe @nogc pure nothrow
    {
        return this.len == 0;
    }

    /// Returns: Number of elements currently in the buffer
    public size_t length () @safe @nogc pure nothrow
    {
        return this.len;
    }

    /*******************************************************************************

        Insert an element to the buffer

        Params:
            val = an element of type T

    *******************************************************************************/

    public void insert () (auto ref T val) @safe @nogc pure nothrow
    {
        assert(!this.full(), "Attempting to insert to a full RingBuffer");

        write_range.front() = val;
        write_range.popFront();
        this.len++;
    }

    /// Returns: Element in the front of the buffer
    public T front () @safe @nogc pure nothrow
    {
        assert(!this.empty(), "Attempting to read from a full RingBuffer");
        return read_range.front();
    }

    /// Removes the element in the front of the buffer
    public void popFront () @safe @nogc pure nothrow
    {
        assert(!this.empty(), "Attempting to pop from a full RingBuffer");
        this.len--;
        read_range.popFront();
    }

    /// Current number of elements
    private size_t len;

    /// Underlying storage
    private T[] storage;

    /// Cyclic range for writes
    private Cycle!(T[]) write_range;

    /// Cyclic range for reads
    private Cycle!(T[]) read_range;
}

unittest
{
    import std.stdio;
    const buffer_size = 3;
    auto buffer = new RingBuffer!int(buffer_size);

    foreach (_; 0..2)
    {
        assert(buffer.empty());
        assert(!buffer.full());

        foreach (i; 0..buffer_size)
        {
            assert(!buffer.full());
            buffer.insert(i);
            assert(!buffer.empty());
            assert(buffer.length == i + 1);
        }
        assert(buffer.full());

        foreach (i; 0..buffer_size)
        {
            assert(!buffer.empty());
            assert(buffer.front() == i);
            buffer.popFront();
        }
    }
}
