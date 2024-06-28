/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common.collect;

import org.opensearch.common.annotation.PublicApi;

/**
 * Java 9 Tuple
 * todo: deprecate and remove w/ min jdk upgrade to 11?
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class Tuple<V1, V2> {

    public static <V1, V2> Tuple<V1, V2> tuple(V1 v1, V2 v2) {
        return new Tuple<>(v1, v2);
    }

    private final V1 v1;
    private final V2 v2;

    public Tuple(V1 v1, V2 v2) {
        this.v1 = v1;
        this.v2 = v2;
    }

    public V1 v1() {
        return v1;
    }

    public V2 v2() {
        return v2;
    }

    /**
     * Returns {@code true} if the given object is also a tuple and the two tuples
     * have equal {@link #v1()} and {@link #v2()} values.
     * <p>
     * Returns {@code false} otherwise, including for {@code null} values or
     * objects of different types.
     * <p>
     * Note: {@code Tuple} instances are equal if the underlying values are
     * equal, even if the types are different.
     *
     * @param o the object to compare to
     * @return {@code true} if the given object is also a tuple and the two tuples
     * have equal {@link #v1()} and {@link #v2()} values.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Tuple<?, ?> tuple = (Tuple<?, ?>) o;

        if (v1 != null ? !v1.equals(tuple.v1) : tuple.v1 != null) return false;
        if (v2 != null ? !v2.equals(tuple.v2) : tuple.v2 != null) return false;

        return true;
    }

    /**
     * Returns the hash code value for this Tuple.
     * @return the hash code value for this Tuple.
     */
    @Override
    public int hashCode() {
        int result = v1 != null ? v1.hashCode() : 0;
        result = 31 * result + (v2 != null ? v2.hashCode() : 0);
        return result;
    }

    /**
     * Returns a string representation of a Tuple
     * @return {@code "Tuple [v1=value1, v2=value2]"}
     */
    @Override
    public String toString() {
        return "Tuple [v1=" + v1 + ", v2=" + v2 + "]";
    }
}
