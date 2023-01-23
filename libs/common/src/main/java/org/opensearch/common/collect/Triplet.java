/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.common.collect;

import java.util.Objects;

public class Triplet<V1, V2, V3> {

    public static <V1, V2, V3> Triplet<V1, V2, V3> tuple(V1 v1, V2 v2, V3 v3) {
        return new Triplet<>(v1, v2, v3);
    }

    private final V1 v1;
    private final V2 v2;

    private final V3 v3;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Triplet<?, ?, ?> triplet = (Triplet<?, ?, ?>) o;
        return Objects.equals(v1, triplet.v1) && Objects.equals(v2, triplet.v2) && Objects.equals(v3, triplet.v3);
    }

    @Override
    public int hashCode() {
        return Objects.hash(v1, v2, v3);
    }

    public Triplet(V1 v1, V2 v2, V3 v3) {
        this.v1 = v1;
        this.v2 = v2;
        this.v3 = v3;
    }

    public V1 v1() {
        return v1;
    }

    public V2 v2() {
        return v2;
    }

    public V3 v3() {
        return v3;
    }

    @Override
    public String toString() {
        return "Tuple [v1=" + v1 + ", v2=" + v2 + ", v3=" + v3 + "]";
    }
}
