/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.javaagent.bootstrap;

import java.security.PrivilegedAction;

/**
 * A computation to be performed with privileges, that throws one or
 * more checked exceptions.  The computation is performed by invoking
 * {@code AccessController.doPrivileged} on the
 * {@code PrivilegedExceptionAction} object.  This interface is
 * used only for computations that throw checked exceptions;
 * computations that do not throw
 * checked exceptions should use {@code PrivilegedAction} instead.
 * @param <T> the type of the result of running the computation
 *
 * @see PrivilegedAction
 */
@FunctionalInterface
public interface PrivilegedExceptionAction<T> {
    /**
     * Performs the computation.  This method will be called by
     * {@code AccessController.doPrivileged}.
     *
     * @return a class-dependent value that may represent the results of the
     *         computation.
     * @throws Exception an exceptional condition has occurred.  Each class
     *         that implements {@code PrivilegedExceptionAction} should
     *         document the exceptions that its run method can throw.
     */

    T run() throws Exception;
}
