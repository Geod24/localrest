/*******************************************************************************

    Serialization-related utilities

    When passing instances accross threads, LocalRest often cannot use the
    provided instance itself, as it would require it to be either `shared`,
    a value type, or `immutable`.
    The term instance here is used to mean an instance of a type,
    that is either passed as parameter, and used as a return value.

    Instead, we support serializing the parameter to one of three types:
    `immutable(void)[]`, `immutable(ubyte)[]`, or `string`.
    The first two are regular serialized target, and `string` can be used
    for JSON, XML, etc.

    A serializer can be a `template` or an aggregate, or even a function
    returning an aggregate. No limitation is put on the type itself,
    only on what operations should be supported.

    For a type `T` to be a valid serializer, it should support
    the following actions:
    - `T.serialize()` should compile when passed on parameter.
    - This one parameter can be any type that is either a parameter or a
      return value of `API`'s methods. This is not checked explicitly,
      but will lead to compilation error in LocalRest if not followed.
    - `T.serialize(param)` should return an one of `string`,
      `immutable(void)[]`, `immutable(ubyte)[]`
    - `T.deserialize!QT` is a template function that returns an
      instance of type `QT` (`QT` is potentially qualified, e.g. `const`).
    - `T.deserialize!!T()` should accept one runtime non-default
       parameter, of the type returned by `T.serialize`.

    The default implementation is the `VibeJSONSerializer`.
    Other utilities in this module are used to facilitate development
    and diagnostics.

    Author:         Mathias 'Geod24' Lang
    License:        MIT (See LICENSE.txt)
    Copyright:      Copyright (c) 2020 Mathias Lang. All rights reserved.

*******************************************************************************/

module geod24.Serialization;

import std.traits;

/*******************************************************************************

    Serialize arbitrary data to / from JSON

    This is the default serializer used by LocalRest, when none is provided.
    It is a template so that LocalRest does not import Vibe.d if not used.

*******************************************************************************/

public template VibeJSONSerializer ()
{
    import vibe.data.json;

    public string serialize (T) (auto ref T value) @safe
    {
        return serializeToJsonString(value);
    }

    public QT deserialize (QT) (string data) @safe
    {
        return deserializeJson!(QT)(data);
    }
}

///
unittest
{
    alias S = VibeJSONSerializer!();
    static assert(!serializerInvalidReason!(S).length, serializerInvalidReason!(S));
    static assert(is(SerializedT!(S) == string));
}

/// Utility function to check if a serializer conforms to the requirements
/// Returns: An error message, or `null` if the serializer is conformant
public string serializerInvalidReason (alias S) ()
{
    // Give codegen a break
    if (!__ctfe) return null;

    static if (!is(typeof(() { alias X = S.serialize; })))
        return "`" ~ S.stringof ~ "` is missing a `serialize` method";
    else static if (!is(typeof(() { alias X = S.deserialize; })))
        return "`" ~ S.stringof ~ "` is missing a `deserialize` method";

    /// We need a type to deserialize to test this function
    /// While we should only test with return types / parameter types,
    /// pretty much anything that can't (de)serialize an `int` is broken.

    else static if (!is(typeof(() { S.serialize(0); })))
        return "`" ~ S.stringof ~ ".serialize` is not callable using argument `int`";
    else static if (!isCallable!(S.deserialize!int))
        return "`" ~ S.stringof ~ ".deserialize` is not callable";

    // If the template has an error we want to be informative
    else static if (!is(typeof(S.serialize(0))))
        return "`" ~ S.stringof ~ ".serialize(0)` does not return any value, does it compile?";

    // All accepted types convert to `immutable(void)[]`
    else static if (!is(typeof(S.serialize(0)) : immutable(void)[]))
        return "`" ~ S.stringof ~ ".serialize` return value should be "
        ~ "`string`, `immutable(ubyte)[]`, or `immutable(void)[]`, not: `"
        ~ typeof(S.serialize(0)).stringof ~"`";

    else
    {
        // Actual return type used
        alias RT = SerializedT!S;

        static if (!is(typeof(S.deserialize!int(RT.init))))
            return "`" ~ S.stringof ~ ".deserialize!int` does not accept serialized type: `"
                ~ RT.stringof ~ "`";
        else
            return null;
    }
}

///
unittest
{
    template Nothing () {}
    static assert(serializerInvalidReason!(Nothing!()).length);
    static assert(serializerInvalidReason!(Object).length);
    // Older frontend do not support passing basic type as template alias parameters
    //static assert(serializerInvalidReason!(int).length);

    // Note: We don't test the error message. Invert the condition to see them.

    // Valid
    static struct S1
    {
        static immutable(void)[] serialize (T) (T v) { return null; }
        static QT deserialize (QT) (immutable(void)[] v) { return QT.init; }
    }
    static assert(!serializerInvalidReason!(S1).length, serializerInvalidReason!(S1));

    // Valid: Type conversions are performed (ubyte[] => void[])
    static struct S2
    {
        static immutable(ubyte)[] serialize (T) (T v) { return null; }
        static QT deserialize (QT) (immutable(void)[] v) { return QT.init; }
    }
    static assert(!serializerInvalidReason!(S2).length, serializerInvalidReason!(S2));

    // Invalid: `void[]` =/> `ubyte[]`
    static struct S3
    {
        static immutable(void)[] serialize (T) (T v) { return null; }
        static QT deserialize (QT) (immutable(ubyte)[] v) { return QT.init; }
    }
    static assert(serializerInvalidReason!(S3).length, serializerInvalidReason!(S3));
}

/*******************************************************************************

    Returns:
      The serialized type for a given serializer.
      `immutable(string)` will match `string`, `immutable(ubyte[])` will match
      `immutable(ubyte)[]`, and everything that can convert to
      `immutable(void)[]` will match it.

*******************************************************************************/

public template SerializedT (alias S)
{
    static if (is(Unqual!(typeof(S.serialize(0))) == string))
        public alias SerializedT = string;
    else static if (is(Unqual!(typeof(S.serialize(0))) == immutable(ubyte)[]))
        public alias SerializedT = immutable(ubyte)[];
    // Catch-all convertion
    else static if (is(typeof(S.serialize(0)) : immutable(void)[]))
        public alias SerializedT = immutable(void)[];
    else static if (is(typeof(S.serialize(0))))
        static assert(0, "`" ~ typeof(S.serialize(0)).stringof ~
                      "` is not a valid type for a serializer");
    else
        static assert(0, "`"  ~ S.stringof ~ ".serialize` is invalid or does not compile");
}

///
unittest
{
    static struct S (RT)
    {
        public static RT serialize (T) (auto ref T value, uint opts = 42) @safe;
        // Ignored because it doesn't take 1 non-default parameter
        public static RT serialize (T) (T a, T b) @safe;
    }

    static assert(is(SerializedT!(S!string) == string));
    static assert(is(SerializedT!(S!(immutable(string))) == string));
    static assert(is(SerializedT!(S!(const(string))) == string));

    static assert(is(SerializedT!(S!(immutable(ubyte[]))) == immutable(ubyte)[]));
    static assert(is(SerializedT!(S!(immutable(ubyte)[])) == immutable(ubyte)[]));
    static assert(is(SerializedT!(S!(const(immutable(ubyte)[]))) == immutable(ubyte)[]));

    static assert(is(SerializedT!(S!(immutable(void[]))) == immutable(void)[]));
    static assert(is(SerializedT!(S!(immutable(void)[])) == immutable(void)[]));
    static assert(is(SerializedT!(S!(const(immutable(void)[]))) == immutable(void)[]));

    static struct Struct { uint v; }
    static assert(is(SerializedT!(S!(immutable(Struct)[])) == immutable(void)[]));

    static struct Struct2 { void[] data; alias data this; }
    static assert(is(SerializedT!(S!(immutable(Struct2))) == immutable(void)[]));

    static assert(!is(typeof(SerializedT!(S!(ubyte[])))));
    static assert(!is(typeof(SerializedT!(S!(void[])))));
    static assert(!is(typeof(SerializedT!(S!(char[])))));
}


/// Type-erasing wrapper to handle serialized data
public union SerializedData
{
    ///
    immutable(void)[]  void_;
    ///
    immutable(ubyte)[] ubyte_;
    ///
    immutable(char)[]  string_;

    ///
    this (typeof(void_) arg) pure nothrow @nogc @trusted
    {
        // Note: Arrays have the same memory layout, so we don't care about
        // the actual type here.
        this.void_ = arg;
    }

    /// Helper to wrap `SerializedT`
    public auto getS (alias Serializer) () const pure nothrow @nogc @trusted
    {
        static assert (!serializerInvalidReason!(Serializer).length,
                       serializerInvalidReason!Serializer);

        return this.get!(SerializedT!Serializer);
    }

    /// Return an exact type (the type returned is always `immutable(ET)[]`)
    public auto get (T) () const pure nothrow @nogc @trusted
    {
        static if (is(immutable(T) == immutable(string)))
            return this.string_;
        else static if (is(immutable(T) == immutable(ubyte[])))
            return this.ubyte_;
        else
            return this.void_;
    }
}

static assert (SerializedData.sizeof == size_t.sizeof * 2,
               "Size mismatch for `SerializedData`");
