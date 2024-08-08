/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.tiering;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.index.Index;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Validation result for tiering
 *
 * @opensearch.experimental
 */

@ExperimentalApi
public class TieringValidationResult {
    private final Set<Index> acceptedIndices;
    private final Map<Index, String> rejectedIndices;

    public TieringValidationResult(Set<Index> concreteIndices) {
        // by default all the indices are added to the accepted set
        this.acceptedIndices = ConcurrentHashMap.newKeySet();
        acceptedIndices.addAll(concreteIndices);
        this.rejectedIndices = new HashMap<>();
    }

    public Set<Index> getAcceptedIndices() {
        return acceptedIndices;
    }

    public Map<Index, String> getRejectedIndices() {
        return rejectedIndices;
    }

    public void addToRejected(Index index, String reason) {
        acceptedIndices.remove(index);
        rejectedIndices.put(index, reason);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TieringValidationResult that = (TieringValidationResult) o;

        if (!Objects.equals(acceptedIndices, that.acceptedIndices)) return false;
        return Objects.equals(rejectedIndices, that.rejectedIndices);
    }

    @Override
    public int hashCode() {
        int result = acceptedIndices != null ? acceptedIndices.hashCode() : 0;
        result = 31 * result + (rejectedIndices != null ? rejectedIndices.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "TieringValidationResult{" + "acceptedIndices=" + acceptedIndices + ", rejectedIndices=" + rejectedIndices + '}';
    }
}
