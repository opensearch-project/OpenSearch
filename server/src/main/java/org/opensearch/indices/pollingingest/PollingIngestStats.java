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
        long totalSkippedCount = in.readLong();
        this.messageProcessorStats = new MessageProcessorStats(totalProcessedCount, totalSkippedCount);
        long totalPolledCount = in.readLong();
        this.consumerStats = new ConsumerStats(totalPolledCount);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(messageProcessorStats.totalProcessedCount);
        out.writeLong(messageProcessorStats.totalSkippedCount);
        out.writeLong(consumerStats.totalPolledCount);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("polling_ingest_stats");
        builder.startObject("message_processor_stats");
        builder.field("total_processed_count", messageProcessorStats.totalProcessedCount);
        builder.field("total_skipped_count", messageProcessorStats.totalSkippedCount);
        builder.endObject();
        builder.startObject("consumer_stats");
        builder.field("total_polled_count", consumerStats.totalPolledCount);
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
    public record MessageProcessorStats(long totalProcessedCount, long totalSkippedCount) {
    }

    /**
     * Stats for consumer (poller)
     */
    @ExperimentalApi
    public record ConsumerStats(long totalPolledCount) {
    }

    /**
     * Builder for {@link PollingIngestStats}
     */
    @ExperimentalApi
    public static class Builder {
        private long totalProcessedCount;
        private long totalSkippedCount;
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

        public Builder setTotalSkippedCount(long totalSkippedCount) {
            this.totalSkippedCount = totalSkippedCount;
            return this;
        }

        public PollingIngestStats build() {
            MessageProcessorStats messageProcessorStats = new MessageProcessorStats(totalProcessedCount, totalSkippedCount);
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
