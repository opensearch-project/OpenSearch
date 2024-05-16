/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins.sdk.actions;

import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.index.Index;

import java.util.Map;
import java.util.Objects;

/**
 * GetIndexMappingMetadataResponse contains index mapping metadata
 * for a set of requested indices.
 */
@ExperimentalApi
public class GetIndexMappingMetadataResponse {

    private final Map<Index, MappingMetadata> mappingMetadata;

    private GetIndexMappingMetadataResponse(Builder builder) {
        this.mappingMetadata = Objects.requireNonNull(builder.mappingMetadata, "mappingMetadata is required");
    }

    public Map<Index, MappingMetadata> mappingMetadata() {
        return this.mappingMetadata;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder for a GetIndexMappingMetadataResponse.
     */
    @ExperimentalApi
    public static class Builder {
        private Map<Index, MappingMetadata> mappingMetadata;

        private Builder() {}

        public Builder mappingMetadata(Map<Index, MappingMetadata> mappingMetadata) {
            this.mappingMetadata = mappingMetadata;
            return this;
        }

        public GetIndexMappingMetadataResponse build() {
            return new GetIndexMappingMetadataResponse(this);
        }
    }
}
