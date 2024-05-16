/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins.sdk.actions;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Objects;

/**
 * GetSystemSettingsRequest enables plugins to request a set of system
 * settings.
 */
@ExperimentalApi
public class GetSystemSettingsRequest {

    private final String[] settings;

    private GetSystemSettingsRequest(Builder builder) {
        this.settings = Objects.requireNonNull(builder.settings, "settings is required");
    }

    public String[] settings() {
        return this.settings;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder for a GetSystemSettingsRequest
     */
    @ExperimentalApi
    public static class Builder {

        private String[] settings;

        private Builder() {}

        public Builder settings(String... settings) {
            this.settings = settings;
            return this;
        }

        public GetSystemSettingsRequest build() {
            return new GetSystemSettingsRequest(this);
        }
    }
}
