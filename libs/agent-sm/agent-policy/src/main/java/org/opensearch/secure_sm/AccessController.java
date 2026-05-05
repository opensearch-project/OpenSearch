/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.secure_sm;

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
     * Performs the specified action in a privileged block and returns a value.
     *
     * <p> If the action's {@code call} method throws an exception,
     * it will propagate through this method.
     *
     * @param <R> the type of the value returned by the action
     * @param <T> the type of the exception that can be thrown
     * @param action the action to be performed
     *
     * @return the value returned by the action's {@code call} method
     *
     * @throws T if the specified action's
     *         {@code call} method threw a <i>checked</i> exception
     */
    public static <R, T extends Exception> R doPrivilegedChecked(CheckedSupplier<R, T> action) throws T {
        return action.get();
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

    /**
     * A functional interface that represents a supplier action that can throw a checked exception.
     *
     * @param <R> the type of the value returned
     * @param <E> the type of the exception that can be thrown
     */
    public interface CheckedSupplier<R, E extends Exception> {

        /**
         * Gets a result.
         *
         * @return a result
         * @throws E
         */
        R get() throws E;
    }
}
