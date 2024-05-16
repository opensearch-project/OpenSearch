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
 * GetIndexMappingMetadataRequest enables plugins to request MappingMetadata for
 * a set of indices.
 */
@ExperimentalApi
public class GetIndexMappingMetadataRequest {

    private final Index[] indices;

    private GetIndexMappingMetadataRequest(Builder builder) {
        indices = Objects.requireNonNull(
            builder.indices,
            "indices is required"
        );
    }

    public Index[] indices() {
        return this.indices;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder for a GetIndexMappingMetadataRequest.
     */
    @ExperimentalApi
    public static final class Builder {
        private Index[] indices;

        private Builder() {}

        public Builder indices(Index... indices) {
            this.indices = indices;
            return this;
        }

        public GetIndexMappingMetadataRequest build() {
            return new GetIndexMappingMetadataRequest(this);
        }
    }
}
