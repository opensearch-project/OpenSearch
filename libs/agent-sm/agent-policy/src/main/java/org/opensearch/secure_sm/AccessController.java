/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.secure_sm;

import java.util.concurrent.Callable;
import java.util.function.Supplier;

/**
 * A utility class that provides methods to perform actions in a privileged context.
 *
 * This class is a replacement for Java's {@code java.security.AccessController} functionality which is marked for
 * removal. All new code should use this class instead of the JDK's {@code AccessController}.
 *
 * Running code in a privileged context will ensure that the code has the necessary permissions
 * without traversing through the entire call stack. See {@code org.opensearch.javaagent.StackCallerProtectionDomainChainExtractor}
 *
 * Example usages:
 * <pre>
 * {@code
 * AccessController.doPrivileged(() -> {
 *     // code that requires privileges
 * });
 * }
 * </pre>
 *
 * Example usage with a return value and checked exception:
 *
 * <pre>
 * {@code
 * T something = AccessController.doPrivilegedChecked(() -> {
 *     // code that requires privileges and may throw a checked exception
 *     return something;
 *     // or
 *     throw new Exception();
 * });
 * }
 * </pre>
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
     */
    public static <T> T doPrivileged(Supplier<T> action) {
        return action.get();
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
    public static <T> T doPrivilegedChecked(Callable<T> action) throws Exception {
        return action.call();
    }

    /**
     * Performs the specified action in a privileged block.
     *
     * <p> If the action's {@code run} method throws an (unchecked)
     * exception, it will propagate through this method.
     *
     * @param action the action to be performed
     *
     * @throws T if the specified action's
     *         {@code call} method threw a <i>checked</i> exception
     */
    public static <T extends Exception> void doPrivilegedChecked(CheckedRunnable<T> action) throws T {
        action.run();
    }

    /**
     * A functional interface that represents a runnable action that can throw a checked exception.
     *
     * @param <E> the type of the exception that can be thrown
     */
    public interface CheckedRunnable<E extends Exception> {

        /**
         * Executes the action.
         *
         * @throws E
         */
        void run() throws E;
    }
}
