/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.pollingingest;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Stats for pull-based ingestion
 */
@ExperimentalApi
public class PollingIngestStats implements Writeable, ToXContentFragment {
    private final MessageProcessorStats messageProcessorStats;
    private final ConsumerStats consumerStats;
    // TODO: add error stats from error handling sink

    public PollingIngestStats(MessageProcessorStats messageProcessorStats, ConsumerStats consumerStats) {
        this.messageProcessorStats = messageProcessorStats;
        this.consumerStats = consumerStats;
    }

    public PollingIngestStats(StreamInput in) throws IOException {
        long totalProcessedCount = in.readLong();
        this.messageProcessorStats = new MessageProcessorStats(totalProcessedCount);
        long totalPolledCount = in.readLong();
        this.consumerStats = new ConsumerStats(totalPolledCount);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(messageProcessorStats.getTotalProcessedCount());
        out.writeLong(consumerStats.getTotalPolledCount());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("polling_ingest_stats");
        builder.startObject("message_processor_stats");
        builder.field("total_processed_count", messageProcessorStats.getTotalProcessedCount());
        builder.endObject();
        builder.startObject("consumer_stats");
        builder.field("total_polled_count", consumerStats.getTotalPolledCount());
        builder.endObject();
        builder.endObject();
        return builder;
    }

    public MessageProcessorStats getMessageProcessorStats() {
        return messageProcessorStats;
    }

    public ConsumerStats getConsumerStats() {
        return consumerStats;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PollingIngestStats)) return false;
        PollingIngestStats that = (PollingIngestStats) o;
        return Objects.equals(messageProcessorStats, that.messageProcessorStats) && Objects.equals(consumerStats, that.consumerStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(messageProcessorStats, consumerStats);
    }

    /**
     * Stats for message processor
     */
    @ExperimentalApi
    public static class MessageProcessorStats {
        private final long totalProcessedCount;

        public MessageProcessorStats(long totalProcessedCount) {
            this.totalProcessedCount = totalProcessedCount;
        }

        public long getTotalProcessedCount() {
            return totalProcessedCount;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof MessageProcessorStats)) return false;
            MessageProcessorStats that = (MessageProcessorStats) o;
            return totalProcessedCount == that.totalProcessedCount;
        }

        @Override
        public int hashCode() {
            return Objects.hash(totalProcessedCount);
        }
    }

    /**
     * Stats for consumer (poller)
     */
    @ExperimentalApi
    public static class ConsumerStats {
        private final long totalPolledCount;

        public ConsumerStats(long totalPolledCount) {
            this.totalPolledCount = totalPolledCount;
        }

        public long getTotalPolledCount() {
            return totalPolledCount;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ConsumerStats)) return false;
            ConsumerStats that = (ConsumerStats) o;
            return totalPolledCount == that.totalPolledCount;
        }

        @Override
        public int hashCode() {
            return Objects.hash(totalPolledCount);
        }
    }

    /**
     * Builder for {@link PollingIngestStats}
     */
    @ExperimentalApi
    public static class Builder {
        private long totalProcessedCount;
        private long totalPolledCount;

        public Builder() {}

        public Builder setTotalProcessedCount(long totalProcessedCount) {
            this.totalProcessedCount = totalProcessedCount;
            return this;
        }

        public Builder setTotalPolledCount(long totalPolledCount) {
            this.totalPolledCount = totalPolledCount;
            return this;
        }

        public PollingIngestStats build() {
            MessageProcessorStats messageProcessorStats = new MessageProcessorStats(totalProcessedCount);
            ConsumerStats consumerStats = new ConsumerStats(totalPolledCount);
            return new PollingIngestStats(messageProcessorStats, consumerStats);
        }
    }

    /**
     * Returns a new builder for creating a {@link PollingIngestStats} instance.
     *
     * @return a new {@code Builder} instance
     */
    public static Builder builder() {
        return new Builder();
    }
}
