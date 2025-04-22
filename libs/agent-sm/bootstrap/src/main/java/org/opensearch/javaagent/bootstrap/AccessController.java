/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.javaagent.bootstrap;

/**
 * Utility class to run code in a privileged block.
 */
public final class AccessController {
    /**
     * Don't allow instantiation an {@code AccessController}
     */
    private AccessController() {}

    /**
     * Performs the specified action in a privileged block.
     *
     * <p> If the action's {@code run} method throws an (unchecked)
     * exception, it will propagate through this method.
     *
     * @param <T> the type of the value returned by the PrivilegedAction's
     *                  {@code run} method
     *
     * @param action the action to be performed
     *
     * @return the value returned by the action's {@code run} method
     *
     * @throws    NullPointerException if the action is {@code null}
     *
     * @apiNote This method performs the specified
     *     {@code PrivilegedAction} with privileges enabled.
     */
    public static <T> T doPrivileged(PrivilegedAction<T> action) {
        T result = action.run();
        return result;
    }

    private static PrivilegedActionException wrapException(Exception e) {
        return new PrivilegedActionException(e);
    }

    /**
     * Performs the specified action.
     *
     * <p> If the action's {@code run} method throws an <i>unchecked</i>
     * exception, it will propagate through this method.
     *
     * @param <T> the type of the value returned by the
     *                  PrivilegedExceptionAction's {@code run} method
     *
     * @param action the action to be performed
     *
     * @return the value returned by the action's {@code run} method
     *
     * @throws    PrivilegedActionException if the specified action's
     *         {@code run} method threw a <i>checked</i> exception
     * @throws    NullPointerException if the action is {@code null}
     */
    public static <T> T doPrivileged(PrivilegedExceptionAction<T> action) throws PrivilegedActionException {
        try {
            T result = action.run();
            return result;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw wrapException(e);
        }
    }
}
