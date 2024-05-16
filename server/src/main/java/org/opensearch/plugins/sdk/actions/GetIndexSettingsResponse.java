/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins.sdk.actions;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;

import java.util.Map;
import java.util.Objects;

/**
 * GetIndexSettingsResponse contains index settings for set of indices.
 */
@ExperimentalApi
public class GetIndexSettingsResponse {

    private final Map<Index, Settings> settings;

    private GetIndexSettingsResponse(Builder builder) {
        settings = Objects.requireNonNull(
            builder.settings,
            "settings is required"
        );
    }

    public Map<Index, Settings> settings() { return this.settings; }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder for a GetIndexSettingsResponse.
     */
    @ExperimentalApi
    public static final class Builder {
        private Map<Index, Settings> settings;

        private Builder() {}

        public Builder settings(Map<Index, Settings> settings) {
            this.settings = settings;
            return this;
        }

        public GetIndexSettingsResponse build() {
            return new GetIndexSettingsResponse(this);
        }
    }
}
