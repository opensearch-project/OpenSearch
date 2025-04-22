/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.javaagent.bootstrap;

/**
 * This exception is thrown by
 * {@code doPrivileged(PrivilegedExceptionAction)} to indicate
 * that the action being performed threw a checked exception.  The exception
 * thrown by the action can be obtained by calling the
 * {@code getException} method.  In effect, an
 * {@code PrivilegedActionException} is a "wrapper"
 * for an exception thrown by a privileged action.
 *
 * @see PrivilegedExceptionAction
 * @see AccessController#doPrivileged(PrivilegedExceptionAction)
 */
public class PrivilegedActionException extends Exception {
    /**
     * Constructs a new {@code PrivilegedActionException} &quot;wrapping&quot;
     * the specific Exception.
     *
     * @param exception The exception thrown
     */
    public PrivilegedActionException(Exception exception) {
        super(null, exception);  // Disallow initCause
    }

    /**
     * Returns the exception thrown by the computation that
     * resulted in this {@code PrivilegedActionException}.
     *
     * @apiNote
     * This method predates the general-purpose exception chaining facility.
     * The {@link Throwable#getCause()} method is now the preferred means of
     * obtaining this information.
     *
     * @return the exception thrown by the computation that
     *         resulted in this {@code PrivilegedActionException}.
     * @see PrivilegedExceptionAction
     * @see AccessController#doPrivileged(PrivilegedExceptionAction)
     */
    public Exception getException() {
        return (Exception) super.getCause();
    }

    /**
     * Returns a string representation of this exception.
     *
     * @return a string representation of this exception.
     */
    @Override
    public String toString() {
        String s = getClass().getName();
        Throwable cause = super.getCause();
        return (cause != null) ? (s + ": " + cause.toString()) : s;
    }
}
