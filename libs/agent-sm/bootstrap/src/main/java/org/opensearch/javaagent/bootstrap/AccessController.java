/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.javaagent.bootstrap;

import java.util.concurrent.Callable;

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
     * @param action the action to be performed
     */
    public static void doPrivileged(Runnable action) {
        action.run();
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
     * @throws Exception if the specified action's
     *         {@code call} method threw a <i>checked</i> exception
     */
    public static <T> T doPrivileged(Callable<T> action) throws Exception {
        return action.call();
    }
}
