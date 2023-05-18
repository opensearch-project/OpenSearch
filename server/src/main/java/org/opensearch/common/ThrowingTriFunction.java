/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common;

/**
 * Represents a function that accepts three arguments and produces a result.
 *
 * @param <S> the type of the first argument
 * @param <T> the type of the second argument
 * @param <U> the type of the third argument
 * @param <R> the return type
 * @param <E> the exception thrown by {@link ThrowingTriFunction#apply}
 *
 * @opensearch.internal
 */
@FunctionalInterface
public interface ThrowingTriFunction<S, T, U, R, E extends Throwable> {
    /**
     * Applies this function to the given arguments.
     *
     * @param s the first function argument
     * @param t the second function argument
     * @param u the third function argument
     * @return the result
     * @throws E extends {@link Throwable}
     */
    R apply(S s, T t, U u) throws E;
}
