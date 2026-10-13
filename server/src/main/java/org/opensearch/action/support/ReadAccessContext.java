/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.List;
import java.util.Objects;

/**
 * Index information supplied to a {@link ReadAccessPolicyProvider}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface ReadAccessContext {

    /** Creates a context for the concrete indices authorized for a read. */
    static ReadAccessContext of(List<String> concreteIndices) {
        return new DefaultReadAccessContext(concreteIndices);
    }

    /** Returns the concrete indices authorized for the read. */
    List<String> concreteIndices();
}

final class DefaultReadAccessContext implements ReadAccessContext {
    private final List<String> concreteIndices;

    DefaultReadAccessContext(List<String> concreteIndices) {
        this.concreteIndices = List.copyOf(Objects.requireNonNull(concreteIndices, "concreteIndices must not be null"));
    }

    @Override
    public List<String> concreteIndices() {
        return concreteIndices;
    }
}
