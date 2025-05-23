/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.pollingingest;

import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Holds the ingestion settings required to update the poller. All values are optional to support partial update.
 */
@ExperimentalApi
public class IngestionSettings {

    // Indicates if poller needs to be paused or resumed.
    @Nullable
    private final Boolean isPaused;

    // Indicates target reset state of the poller.
    @Nullable
    private final StreamPoller.ResetState resetState;

    // Indicates target reset value (offset/timestamp/sequence number etc).
    @Nullable
    private final String resetValue;

    private IngestionSettings(Builder builder) {
        this.isPaused = builder.isPaused;
        this.resetState = builder.resetState;
        this.resetValue = builder.resetValue;
    }

    @Nullable
    public Boolean getIsPaused() {
        return isPaused;
    }

    @Nullable
    public StreamPoller.ResetState getResetState() {
        return resetState;
    }

    @Nullable
    public String getResetValue() {
        return resetValue;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for IngestionSettings. Only set the fields that need to be used for updating ingestion state.
     */
    @ExperimentalApi
    public static class Builder {
        @Nullable
        private Boolean isPaused;
        @Nullable
        private StreamPoller.ResetState resetState;
        @Nullable
        private String resetValue;

        public Builder setIsPaused(Boolean isPaused) {
            this.isPaused = isPaused;
            return this;
        }

        public Builder setResetState(StreamPoller.ResetState resetState) {
            this.resetState = resetState;
            return this;
        }

        public Builder setResetValue(String resetValue) {
            this.resetValue = resetValue;
            return this;
        }

        public IngestionSettings build() {
            return new IngestionSettings(this);
        }
    }

}
