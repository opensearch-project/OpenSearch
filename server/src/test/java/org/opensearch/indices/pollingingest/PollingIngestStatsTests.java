/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.pollingingest;

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

    private PollingIngestStats createTestInstance() {
        return PollingIngestStats.builder()
            .setTotalProcessedCount(randomNonNegativeLong())
            .setTotalInvalidMessageCount(randomNonNegativeLong())
            .setTotalPolledCount(randomNonNegativeLong())
            .setLagInMillis(randomNonNegativeLong())
            .build();
    }
}
