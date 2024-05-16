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
 * GetIndexSettingsRequest enables plugins to request index settings for
 * a set of indices.
 */
@ExperimentalApi
public class GetIndexSettingsRequest {

    private final Index[] indices;
    private final String[] settings;

    private GetIndexSettingsRequest(Builder builder) {
        indices = Objects.requireNonNull(
            builder.indices,
            "indices is required"
        );

        settings = Objects.requireNonNull(
            builder.settings,
            "settings is required"
        );
    }

    public Index[] indices() {
        return this.indices;
    }

    public String[] settings() { return this.settings; }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder for a GetIndexSettingsRequest
     */
    @ExperimentalApi
    public static final class Builder {
        private Index[] indices;
        private String[] settings;

        private Builder() {}

        public Builder indices(Index... indices) {
            this.indices = indices;
            return this;
        }

        public Builder settings(String... settings) {
            this.settings = settings;
            return this;
        }

        public GetIndexSettingsRequest build() {
            return new GetIndexSettingsRequest(this);
        }
    }
}
