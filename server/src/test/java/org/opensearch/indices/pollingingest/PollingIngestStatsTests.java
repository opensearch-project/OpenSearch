/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.pollingingest;

import org.opensearch.Version;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class PollingIngestStatsTests extends OpenSearchTestCase {

    public void testToXContent() throws IOException {
        PollingIngestStats stats = createTestInstance();

        XContentBuilder builder = MediaTypeRegistry.contentBuilder(MediaTypeRegistry.JSON);
        builder.startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        String expected = "{\"polling_ingest_stats\":{\"message_processor_stats\":{\"total_processed_count\":"
            + stats.getMessageProcessorStats().totalProcessedCount()
            + ",\"total_invalid_message_count\":"
            + stats.getMessageProcessorStats().totalInvalidMessageCount()
            + ",\"total_version_conflicts_count\":"
            + stats.getMessageProcessorStats().totalVersionConflictsCount()
            + ",\"total_failed_count\":"
            + stats.getMessageProcessorStats().totalFailedCount()
            + ",\"total_failures_dropped_count\":"
            + stats.getMessageProcessorStats().totalFailuresDroppedCount()
            + ",\"total_processor_thread_interrupt_count\":"
            + stats.getMessageProcessorStats().totalProcessorThreadInterruptCount()
            + "},\"consumer_stats\":{\"total_polled_count\":"
            + stats.getConsumerStats().totalPolledCount()
            + ",\"total_consumer_error_count\":"
            + stats.getConsumerStats().totalConsumerErrorCount()
            + ",\"total_poller_message_failure_count\":"
            + stats.getConsumerStats().totalPollerMessageFailureCount()
            + ",\"total_poller_message_dropped_count\":"
            + stats.getConsumerStats().totalPollerMessageDroppedCount()
            + ",\"total_duplicate_message_skipped_count\":"
            + stats.getConsumerStats().totalDuplicateMessageSkippedCount()
            + ",\"lag_in_millis\":"
            + stats.getConsumerStats().lagInMillis()
            + ",\"pointer_based_lag\":"
            + stats.getConsumerStats().pointerBasedLag()
            + "},\"pipeline_stats\":{\"total_execution_count\":"
            + stats.getPipelineStats().totalExecutionCount()
            + ",\"total_execution_time_in_millis\":"
            + stats.getPipelineStats().totalExecutionTimeInMillis()
            + ",\"total_failed_count\":"
            + stats.getPipelineStats().totalFailedCount()
            + ",\"total_dropped_count\":"
            + stats.getPipelineStats().totalDroppedCount()
            + ",\"total_timeout_count\":"
            + stats.getPipelineStats().totalTimeoutCount()
            + "}}}";

        assertEquals(expected, builder.toString());
    }

    public void testSerialization() throws IOException {
        PollingIngestStats original = createTestInstance();

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            original.writeTo(output);

            try (StreamInput input = output.bytes().streamInput()) {
                PollingIngestStats deserialized = new PollingIngestStats(input);
                assertEquals(original, deserialized);
            }
        }
    }

    /**
     * Test serialization to a pre-3.7.0 node — pipeline stats should be omitted.
     */
    public void testSerializationToOlderNode() throws IOException {
        PollingIngestStats original = createTestInstance();

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setVersion(Version.V_3_5_0);
            original.writeTo(output);

            try (StreamInput input = output.bytes().streamInput()) {
                input.setVersion(Version.V_3_5_0);
                PollingIngestStats deserialized = new PollingIngestStats(input);

                // Message processor and consumer stats should match
                assertEquals(original.getMessageProcessorStats(), deserialized.getMessageProcessorStats());
                assertEquals(original.getConsumerStats(), deserialized.getConsumerStats());

                // Pipeline stats should be zeroed out (not serialized to older nodes)
                assertEquals(0, deserialized.getPipelineStats().totalExecutionCount());
                assertEquals(0, deserialized.getPipelineStats().totalExecutionTimeInMillis());
                assertEquals(0, deserialized.getPipelineStats().totalFailedCount());
                assertEquals(0, deserialized.getPipelineStats().totalDroppedCount());
                assertEquals(0, deserialized.getPipelineStats().totalTimeoutCount());
            }
        }
    }

    /**
     * Test deserialization from a pre-3.7.0 node — pipeline stats should default to zero.
     */
    public void testDeserializationFromOlderNode() throws IOException {
        // Simulate a pre-3.6.0 node writing stats without pipeline fields
        PollingIngestStats original = createTestInstance();

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setVersion(Version.V_3_6_0);
            original.writeTo(output);

            // Read back as a 3.6.0 node
            try (StreamInput input = output.bytes().streamInput()) {
                input.setVersion(Version.V_3_6_0);
                PollingIngestStats deserialized = new PollingIngestStats(input);

                assertEquals(original.getMessageProcessorStats(), deserialized.getMessageProcessorStats());
                assertEquals(0, deserialized.getPipelineStats().totalExecutionCount());
            }
        }
    }

    private PollingIngestStats createTestInstance() {
        return new PollingIngestStats(
            new PollingIngestStats.MessageProcessorStats(
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong()
            ),
            new PollingIngestStats.ConsumerStats(
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong()
            ),
            new PollingIngestStats.PipelineStats(
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong()
            )
        );
    }
}
