/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.pollingingest;

import org.opensearch.Version;
import org.opensearch.common.annotation.PublicApi;
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
@PublicApi(since = "3.6.0")
public class PollingIngestStats implements Writeable, ToXContentFragment {
    private final MessageProcessorStats messageProcessorStats;
    private final ConsumerStats consumerStats;
    private final PipelineStats pipelineStats;

    public PollingIngestStats(MessageProcessorStats messageProcessorStats, ConsumerStats consumerStats) {
        this(messageProcessorStats, consumerStats, new PipelineStats(0, 0, 0, 0, 0));
    }

    public PollingIngestStats(
        MessageProcessorStats messageProcessorStats,
        ConsumerStats consumerStats,
        IngestPipelineExecutor.PipelineMetrics pipelineMetrics
    ) {
        this(
            messageProcessorStats,
            consumerStats,
            new PipelineStats(
                pipelineMetrics.totalExecutionCount(),
                pipelineMetrics.totalExecutionTimeInMillis(),
                pipelineMetrics.totalFailedCount(),
                pipelineMetrics.totalDroppedCount(),
                pipelineMetrics.totalTimeoutCount()
            )
        );
    }

    public PollingIngestStats(MessageProcessorStats messageProcessorStats, ConsumerStats consumerStats, PipelineStats pipelineStats) {
        this.messageProcessorStats = messageProcessorStats;
        this.consumerStats = consumerStats;
        this.pipelineStats = pipelineStats;
    }

    public PollingIngestStats(StreamInput in) throws IOException {
        long totalProcessedCount = in.readLong();
        long totalInvalidMessageCount = in.readLong();
        long totalProcessorVersionConflictsCount = in.readLong();
        long totalProcessorFailedCount = in.readLong();
        long totalProcessorFailuresDroppedCount = in.readLong();
        long totalProcessorThreadInterruptCount = in.readLong();
        this.messageProcessorStats = new MessageProcessorStats(
            totalProcessedCount,
            totalInvalidMessageCount,
            totalProcessorVersionConflictsCount,
            totalProcessorFailedCount,
            totalProcessorFailuresDroppedCount,
            totalProcessorThreadInterruptCount
        );
        long totalPolledCount = in.readLong();
        long lagInMillis = in.readLong();
        long totalConsumerErrorCount = in.readLong();
        long totalPollerMessageFailureCount = in.readLong();
        long totalPollerMessageDroppedCount = in.readLong();
        long totalDuplicateMessageSkippedCount = in.readLong();

        long pointerBasedLag = 0;
        if (in.getVersion().onOrAfter(Version.V_3_4_0)) {
            pointerBasedLag = in.readLong();
        }

        this.consumerStats = new ConsumerStats(
            totalPolledCount,
            lagInMillis,
            totalConsumerErrorCount,
            totalPollerMessageFailureCount,
            totalPollerMessageDroppedCount,
            totalDuplicateMessageSkippedCount,
            pointerBasedLag
        );

        if (in.getVersion().onOrAfter(Version.V_3_7_0)) {
            this.pipelineStats = new PipelineStats(
                in.readLong(),
                in.readLong(),
                in.readLong(),
                in.readLong(),
                in.readLong()
            );
        } else {
            this.pipelineStats = new PipelineStats(0, 0, 0, 0, 0);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(messageProcessorStats.totalProcessedCount);
        out.writeLong(messageProcessorStats.totalInvalidMessageCount);
        out.writeLong(messageProcessorStats.totalVersionConflictsCount);
        out.writeLong(messageProcessorStats.totalFailedCount);
        out.writeLong(messageProcessorStats.totalFailuresDroppedCount);
        out.writeLong(messageProcessorStats.totalProcessorThreadInterruptCount);
        out.writeLong(consumerStats.totalPolledCount);
        out.writeLong(consumerStats.lagInMillis);
        out.writeLong(consumerStats.totalConsumerErrorCount);
        out.writeLong(consumerStats.totalPollerMessageFailureCount);
        out.writeLong(consumerStats.totalPollerMessageDroppedCount);
        out.writeLong(consumerStats.totalDuplicateMessageSkippedCount);

        if (out.getVersion().onOrAfter(Version.V_3_4_0)) {
            out.writeLong(consumerStats.pointerBasedLag);
        }

        if (out.getVersion().onOrAfter(Version.V_3_7_0)) {
            out.writeLong(pipelineStats.totalExecutionCount);
            out.writeLong(pipelineStats.totalExecutionTimeInMillis);
            out.writeLong(pipelineStats.totalFailedCount);
            out.writeLong(pipelineStats.totalDroppedCount);
            out.writeLong(pipelineStats.totalTimeoutCount);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("polling_ingest_stats");
        builder.startObject("message_processor_stats");
        builder.field("total_processed_count", messageProcessorStats.totalProcessedCount);
        builder.field("total_invalid_message_count", messageProcessorStats.totalInvalidMessageCount);
        builder.field("total_version_conflicts_count", messageProcessorStats.totalVersionConflictsCount);
        builder.field("total_failed_count", messageProcessorStats.totalFailedCount);
        builder.field("total_failures_dropped_count", messageProcessorStats.totalFailuresDroppedCount);
        builder.field("total_processor_thread_interrupt_count", messageProcessorStats.totalProcessorThreadInterruptCount);
        builder.endObject();
        builder.startObject("consumer_stats");
        builder.field("total_polled_count", consumerStats.totalPolledCount);
        builder.field("total_consumer_error_count", consumerStats.totalConsumerErrorCount);
        builder.field("total_poller_message_failure_count", consumerStats.totalPollerMessageFailureCount);
        builder.field("total_poller_message_dropped_count", consumerStats.totalPollerMessageDroppedCount);
        builder.field("total_duplicate_message_skipped_count", consumerStats.totalDuplicateMessageSkippedCount);
        builder.field("lag_in_millis", consumerStats.lagInMillis);
        builder.field("pointer_based_lag", consumerStats.pointerBasedLag);
        builder.endObject();
        builder.startObject("pipeline_stats");
        builder.field("total_execution_count", pipelineStats.totalExecutionCount);
        builder.field("total_execution_time_in_millis", pipelineStats.totalExecutionTimeInMillis);
        builder.field("total_failed_count", pipelineStats.totalFailedCount);
        builder.field("total_dropped_count", pipelineStats.totalDroppedCount);
        builder.field("total_timeout_count", pipelineStats.totalTimeoutCount);
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

    public PipelineStats getPipelineStats() {
        return pipelineStats;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PollingIngestStats)) return false;
        PollingIngestStats that = (PollingIngestStats) o;
        return Objects.equals(messageProcessorStats, that.messageProcessorStats)
            && Objects.equals(consumerStats, that.consumerStats)
            && Objects.equals(pipelineStats, that.pipelineStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(messageProcessorStats, consumerStats, pipelineStats);
    }

    /**
     * Stats for message processor
     */
    @PublicApi(since = "3.6.0")
    public record MessageProcessorStats(long totalProcessedCount, long totalInvalidMessageCount, long totalVersionConflictsCount,
        long totalFailedCount, long totalFailuresDroppedCount, long totalProcessorThreadInterruptCount) {
    }

    /**
     * Stats for consumer (poller).
     *
     * totalDuplicateMessageSkippedCount has been deprecated as of version 3.4  and will be removed in a future version.
     */
    @PublicApi(since = "3.6.0")
    public record ConsumerStats(long totalPolledCount, long lagInMillis, long totalConsumerErrorCount, long totalPollerMessageFailureCount,
        long totalPollerMessageDroppedCount, long totalDuplicateMessageSkippedCount, long pointerBasedLag) {
    }

    /**
     * Stats for pipeline execution in pull-based ingestion.
     */
    @PublicApi(since = "3.7.0")
    public record PipelineStats(long totalExecutionCount, long totalExecutionTimeInMillis, long totalFailedCount, long totalDroppedCount,
        long totalTimeoutCount) {
    }

    /**
     * Builder for {@link PollingIngestStats}
     */
    @PublicApi(since = "3.6.0")
    public static class Builder {
        private long totalProcessedCount;
        private long totalInvalidMessageCount;
        private long totalPolledCount;
        private long totalVersionConflictsCount;
        private long totalFailedCount;
        private long totalFailuresDroppedCount;
        private long totalProcessorThreadInterruptCount;
        private long lagInMillis;
        private long totalConsumerErrorCount;
        private long totalPollerMessageFailureCount;
        private long totalPollerMessageDroppedCount;
        private long totalDuplicateMessageSkippedCount;
        private long pointerBasedLag;
        private long pipelineExecutionCount;
        private long pipelineExecutionTimeInMillis;
        private long pipelineFailedCount;
        private long pipelineDroppedCount;
        private long pipelineTimeoutCount;

        public Builder() {}

        public Builder setTotalProcessedCount(long totalProcessedCount) {
            this.totalProcessedCount = totalProcessedCount;
            return this;
        }

        public Builder setTotalPolledCount(long totalPolledCount) {
            this.totalPolledCount = totalPolledCount;
            return this;
        }

        public Builder setTotalInvalidMessageCount(long totalInvalidMessageCount) {
            this.totalInvalidMessageCount = totalInvalidMessageCount;
            return this;
        }

        public Builder setTotalProcessorVersionConflictsCount(long totalVersionConflictsCount) {
            this.totalVersionConflictsCount = totalVersionConflictsCount;
            return this;
        }

        public Builder setTotalProcessorFailedCount(long totalFailedCount) {
            this.totalFailedCount = totalFailedCount;
            return this;
        }

        public Builder setTotalProcessorFailuresDroppedCount(long totalFailuresDroppedCount) {
            this.totalFailuresDroppedCount = totalFailuresDroppedCount;
            return this;
        }

        public Builder setTotalProcessorThreadInterruptCount(long totalProcessorThreadInterruptCount) {
            this.totalProcessorThreadInterruptCount = totalProcessorThreadInterruptCount;
            return this;
        }

        public Builder setLagInMillis(long lagInMillis) {
            this.lagInMillis = lagInMillis;
            return this;
        }

        public Builder setTotalConsumerErrorCount(long totalConsumerErrorCount) {
            this.totalConsumerErrorCount = totalConsumerErrorCount;
            return this;
        }

        public Builder setTotalPollerMessageFailureCount(long totalPollerMessageFailureCount) {
            this.totalPollerMessageFailureCount = totalPollerMessageFailureCount;
            return this;
        }

        public Builder setTotalPollerMessageDroppedCount(long totalPollerMessageDroppedCount) {
            this.totalPollerMessageDroppedCount = totalPollerMessageDroppedCount;
            return this;
        }

        /**
         * @deprecated As of 3.4, this field is no longer used and will be removed in a future version.
         */
        @Deprecated(since = "3.4", forRemoval = true)
        public Builder setTotalDuplicateMessageSkippedCount(long totalDuplicateMessageSkippedCount) {
            this.totalDuplicateMessageSkippedCount = totalDuplicateMessageSkippedCount;
            return this;
        }

        public Builder setPointerBasedLag(long pointerBasedLag) {
            this.pointerBasedLag = pointerBasedLag;
            return this;
        }

        public Builder setPipelineStats(IngestPipelineExecutor.PipelineMetrics metrics) {
            this.pipelineExecutionCount = metrics.totalExecutionCount();
            this.pipelineExecutionTimeInMillis = metrics.totalExecutionTimeInMillis();
            this.pipelineFailedCount = metrics.totalFailedCount();
            this.pipelineDroppedCount = metrics.totalDroppedCount();
            this.pipelineTimeoutCount = metrics.totalTimeoutCount();
            return this;
        }

        public PollingIngestStats build() {
            MessageProcessorStats messageProcessorStats = new MessageProcessorStats(
                totalProcessedCount,
                totalInvalidMessageCount,
                totalVersionConflictsCount,
                totalFailedCount,
                totalFailuresDroppedCount,
                totalProcessorThreadInterruptCount
            );
            ConsumerStats consumerStats = new ConsumerStats(
                totalPolledCount,
                lagInMillis,
                totalConsumerErrorCount,
                totalPollerMessageFailureCount,
                totalPollerMessageDroppedCount,
                totalDuplicateMessageSkippedCount,
                pointerBasedLag
            );
            PipelineStats pipelineStats = new PipelineStats(
                pipelineExecutionCount,
                pipelineExecutionTimeInMillis,
                pipelineFailedCount,
                pipelineDroppedCount,
                pipelineTimeoutCount
            );
            return new PollingIngestStats(messageProcessorStats, consumerStats, pipelineStats);
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
