/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common;

/**
 * A {@link TriFunction}-like interface which allows throwing checked exceptions.
 *
 * @opensearch.internal
 */
@FunctionalInterface
public interface CheckedTriFunction<S, T, U, R, E extends Exception> {
    R apply(S s, T t, U u) throws E;
}
