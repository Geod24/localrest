/*******************************************************************************

    Defines the exception class thrown when an unimplemented API method
    is called, or one with the wrong parameters is attempted to be called.

*******************************************************************************/

module geod24.Exception;

import std.exception;

/// ditto
public class UnhandledMethodException : Exception
{
    ///
    mixin basicExceptionCtors;
}
