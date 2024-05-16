/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins.sdk.actions;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.index.Index;

import java.util.Objects;

/**
 * ConcreteIndicesResponse contains the set of concrete indices that
 * matched a given set of index expressions.
 */
@ExperimentalApi
public class ConcreteIndicesResponse {

    private final Index[] indices;

    private ConcreteIndicesResponse(Builder builder) {
        this.indices = Objects.requireNonNull(
            builder.indices,
            "indices is required"
        );
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder for a ConcreteIndicesResponse
     */
    @ExperimentalApi
    public static final class Builder {
        private Index[] indices;

        private Builder() {}

        public Builder indices(Index... indices) {
            this.indices = indices;
            return this;
        }

        public ConcreteIndicesResponse build() {
            return new ConcreteIndicesResponse(this);
        }
    }
}
