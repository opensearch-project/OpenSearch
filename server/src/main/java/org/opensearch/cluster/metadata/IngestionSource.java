/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Settings;
import org.opensearch.indices.pollingingest.IngestionErrorStrategy;
import org.opensearch.indices.pollingingest.StreamPoller;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.cluster.metadata.IndexMetadata.INGESTION_SOURCE_MAX_POLL_SIZE;
import static org.opensearch.cluster.metadata.IndexMetadata.INGESTION_SOURCE_POLL_TIMEOUT;

/**
 * Class encapsulating the configuration of an ingestion source.
 */
@ExperimentalApi
public class IngestionSource {
    private final String type;
    private final PointerInitReset pointerInitReset;
    private final IngestionErrorStrategy.ErrorStrategy errorStrategy;
    private final Map<String, Object> params;
    private final long maxPollSize;
    private final int pollTimeout;

    private IngestionSource(
        String type,
        PointerInitReset pointerInitReset,
        IngestionErrorStrategy.ErrorStrategy errorStrategy,
        Map<String, Object> params,
        long maxPollSize,
        int pollTimeout
    ) {
        this.type = type;
        this.pointerInitReset = pointerInitReset;
        this.params = params;
        this.errorStrategy = errorStrategy;
        this.maxPollSize = maxPollSize;
        this.pollTimeout = pollTimeout;
    }

    public String getType() {
        return type;
    }

    public PointerInitReset getPointerInitReset() {
        return pointerInitReset;
    }

    public IngestionErrorStrategy.ErrorStrategy getErrorStrategy() {
        return errorStrategy;
    }

    public Map<String, Object> params() {
        return params;
    }

    public long getMaxPollSize() {
        return maxPollSize;
    }

    public int getPollTimeout() {
        return pollTimeout;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IngestionSource ingestionSource = (IngestionSource) o;
        return Objects.equals(type, ingestionSource.type)
            && Objects.equals(pointerInitReset, ingestionSource.pointerInitReset)
            && Objects.equals(errorStrategy, ingestionSource.errorStrategy)
            && Objects.equals(params, ingestionSource.params)
            && Objects.equals(maxPollSize, ingestionSource.maxPollSize)
            && Objects.equals(pollTimeout, ingestionSource.pollTimeout);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, pointerInitReset, params, errorStrategy, maxPollSize, pollTimeout);
    }

    @Override
    public String toString() {
        return "IngestionSource{"
            + "type='"
            + type
            + '\''
            + ",pointer_init_reset='"
            + pointerInitReset
            + '\''
            + ",error_strategy='"
            + errorStrategy
            + '\''
            + ", params="
            + params
            + ", maxPollSize="
            + maxPollSize
            + ", pollTimeout="
            + pollTimeout
            + '}';
    }

    /**
     * Class encapsulating the configuration of a pointer initialization.
     */
    @ExperimentalApi
    public static class PointerInitReset {
        private final StreamPoller.ResetState type;
        private final String value;

        public PointerInitReset(StreamPoller.ResetState type, String value) {
            this.type = type;
            this.value = value;
        }

        public StreamPoller.ResetState getType() {
            return type;
        }

        public String getValue() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PointerInitReset pointerInitReset = (PointerInitReset) o;
            return Objects.equals(type, pointerInitReset.type) && Objects.equals(value, pointerInitReset.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, value);
        }

        @Override
        public String toString() {
            return "PointerInitReset{" + "type='" + type + '\'' + ", value=" + value + '}';
        }
    }

    /**
     * Builder for {@link IngestionSource}.
     *
     */
    @ExperimentalApi
    public static class Builder {
        private String type;
        private PointerInitReset pointerInitReset;
        private IngestionErrorStrategy.ErrorStrategy errorStrategy;
        private Map<String, Object> params;
        private long maxPollSize = INGESTION_SOURCE_MAX_POLL_SIZE.getDefault(Settings.EMPTY);
        private int pollTimeout = INGESTION_SOURCE_POLL_TIMEOUT.getDefault(Settings.EMPTY);

        public Builder(String type) {
            this.type = type;
            this.params = new HashMap<>();
        }

        public Builder(IngestionSource ingestionSource) {
            this.type = ingestionSource.type;
            this.pointerInitReset = ingestionSource.pointerInitReset;
            this.errorStrategy = ingestionSource.errorStrategy;
            this.params = ingestionSource.params;
        }

        public Builder setPointerInitReset(PointerInitReset pointerInitReset) {
            this.pointerInitReset = pointerInitReset;
            return this;
        }

        public Builder setErrorStrategy(IngestionErrorStrategy.ErrorStrategy errorStrategy) {
            this.errorStrategy = errorStrategy;
            return this;
        }

        public Builder setParams(Map<String, Object> params) {
            this.params = params;
            return this;
        }

        public Builder setMaxPollSize(long maxPollSize) {
            this.maxPollSize = maxPollSize;
            return this;
        }

        public Builder addParam(String key, Object value) {
            this.params.put(key, value);
            return this;
        }

        public Builder setPollTimeout(int pollTimeout) {
            this.pollTimeout = pollTimeout;
            return this;
        }

        public IngestionSource build() {
            return new IngestionSource(type, pointerInitReset, errorStrategy, params, maxPollSize, pollTimeout);
        }

    }
}
