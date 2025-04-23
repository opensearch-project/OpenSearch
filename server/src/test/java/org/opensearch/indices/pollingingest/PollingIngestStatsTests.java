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
            + ",\"total_skipped_count\":"
            + stats.getMessageProcessorStats().totalSkippedCount()
            + "},\"consumer_stats\":{\"total_polled_count\":"
            + stats.getConsumerStats().totalPolledCount()
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
            .setTotalSkippedCount(randomNonNegativeLong())
            .setTotalPolledCount(randomNonNegativeLong())
            .build();
    }
}
