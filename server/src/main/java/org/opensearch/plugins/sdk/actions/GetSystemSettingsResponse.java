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

import java.util.Objects;

/**
 * GetSystemSettingsResponse contains a set of system settings.
 */
@ExperimentalApi
public class GetSystemSettingsResponse {

    private final Settings settings;

    private GetSystemSettingsResponse(Builder builder) {
        this.settings = Objects.requireNonNull(builder.settings, "settings is required");
    }

    public Settings settings() {
        return this.settings;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder for a GetSystemSettingsResponse
     */
    @ExperimentalApi
    public static class Builder {
        private Settings settings;

        private Builder() {}

        public Builder settings(Settings settings) {
            this.settings = settings;
            return this;
        }

        public GetSystemSettingsResponse build() {
            return new GetSystemSettingsResponse(this);
        }
    }
}
