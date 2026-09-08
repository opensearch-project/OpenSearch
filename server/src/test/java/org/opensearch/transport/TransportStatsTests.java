/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport;

import org.opensearch.Version;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class TransportStatsTests extends OpenSearchTestCase {

    public void testToXContent() throws IOException {
        // Channel types are rendered in sorted order regardless of how the source map iterates.
        TransportStats stats = createTestInstance(Map.of("reg", 12L, "bulk", 7L));

        XContentBuilder builder = MediaTypeRegistry.contentBuilder(MediaTypeRegistry.JSON);
        builder.startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        String expected = "{\"transport\":{\"server_open\":"
            + stats.getServerOpen()
            + ",\"total_outbound_connections\":"
            + stats.getTotalOutboundConnections()
            + ",\"rx_count\":"
            + stats.getRxCount()
            + ",\"rx_size_in_bytes\":"
            + stats.getRxSize().getBytes()
            + ",\"tx_count\":"
            + stats.getTxCount()
            + ",\"tx_size_in_bytes\":"
            + stats.getTxSize().getBytes()
            + ",\"channel_close_by_type\":{\"bulk\":7,\"reg\":12}"
            + ",\"outgoing_timeouts\":"
            + stats.getOutgoingTimeouts()
            + ",\"requests_failed_on_disconnect\":"
            + stats.getRequestsFailedOnDisconnect()
            + ",\"connect_failures\":"
            + stats.getConnectFailures()
            + ",\"connect_time_millis\":"
            + stats.getConnectTimeMillis()
            + ",\"connect_time_millis_max\":"
            + stats.getConnectTimeMillisMax()
            + "}}";

        assertEquals(expected, builder.toString());
    }

    /**
     * An empty channel close map is omitted entirely rather than rendered as an empty object, so stats
     * built without it stay byte-identical to the pre-existing response shape.
     */
    public void testToXContentOmitsEmptyChannelCloseMap() throws IOException {
        TransportStats stats = new TransportStats.Builder().serverOpen(1).build();

        XContentBuilder builder = MediaTypeRegistry.contentBuilder(MediaTypeRegistry.JSON);
        builder.startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        assertFalse(builder.toString().contains("channel_close_by_type"));
    }

    public void testSerialization() throws IOException {
        TransportStats original = createTestInstance(randomChannelCloseByType());

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            original.writeTo(output);

            try (StreamInput input = output.bytes().streamInput()) {
                assertAllFieldsEqual(original, new TransportStats(input));
            }
        }
    }

    /**
     * Test serialization to a pre-3.8.0 node — the transport observability counters should be omitted.
     */
    public void testSerializationToOlderNode() throws IOException {
        TransportStats original = createTestInstance(randomChannelCloseByType());

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setVersion(Version.V_3_7_0);
            original.writeTo(output);

            try (StreamInput input = output.bytes().streamInput()) {
                input.setVersion(Version.V_3_7_0);
                TransportStats deserialized = new TransportStats(input);

                // Pre-existing counters are unaffected.
                assertEquals(original.getServerOpen(), deserialized.getServerOpen());
                assertEquals(original.getTotalOutboundConnections(), deserialized.getTotalOutboundConnections());
                assertEquals(original.getRxCount(), deserialized.getRxCount());
                assertEquals(original.getRxSize(), deserialized.getRxSize());
                assertEquals(original.getTxCount(), deserialized.getTxCount());
                assertEquals(original.getTxSize(), deserialized.getTxSize());

                assertNewCountersAbsent(deserialized);
            }
        }
    }

    /**
     * Test deserialization from a pre-3.8.0 node — the new counters default to zero rather than
     * consuming bytes the older node never wrote.
     */
    public void testDeserializationFromOlderNode() throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setVersion(Version.V_3_7_0);
            // A 3.7.0 node writes only the six original counters.
            output.writeVLong(5L);
            output.writeVLong(6L);
            output.writeVLong(7L);
            output.writeVLong(8L);
            output.writeVLong(9L);
            output.writeVLong(10L);

            try (StreamInput input = output.bytes().streamInput()) {
                input.setVersion(Version.V_3_7_0);
                TransportStats deserialized = new TransportStats(input);

                assertEquals(5L, deserialized.getServerOpen());
                assertEquals(6L, deserialized.getTotalOutboundConnections());
                assertEquals(7L, deserialized.getRxCount());
                assertEquals(8L, deserialized.getRxSize().getBytes());
                assertEquals(9L, deserialized.getTxCount());
                assertEquals(10L, deserialized.getTxSize().getBytes());

                assertNewCountersAbsent(deserialized);
                assertEquals(0, input.available());
            }
        }
    }

    /**
     * {@link TransportService#stats()} seeds a builder from the transport's own stats and overrides only
     * the two request-level counters, so the copy constructor must carry every other field across.
     */
    public void testBuilderCopiesEveryField() {
        TransportStats original = createTestInstance(randomChannelCloseByType());

        TransportStats copy = new TransportStats.Builder(original).build();

        assertAllFieldsEqual(original, copy);
    }

    public void testBuilderOverridesRequestLevelCounters() {
        TransportStats base = createTestInstance(randomChannelCloseByType());

        TransportStats merged = new TransportStats.Builder(base).outgoingTimeouts(42L).requestsFailedOnDisconnect(43L).build();

        assertEquals(42L, merged.getOutgoingTimeouts());
        assertEquals(43L, merged.getRequestsFailedOnDisconnect());
        // Everything the caller did not override is preserved.
        assertEquals(base.getConnectFailures(), merged.getConnectFailures());
        assertEquals(base.getConnectTimeMillis(), merged.getConnectTimeMillis());
        assertEquals(base.getConnectTimeMillisMax(), merged.getConnectTimeMillisMax());
        assertEquals(base.getChannelCloseByType(), merged.getChannelCloseByType());
        assertEquals(base.getServerOpen(), merged.getServerOpen());
    }

    public void testChannelCloseByTypeIsUnmodifiable() {
        TransportStats stats = createTestInstance(randomChannelCloseByType());

        expectThrows(UnsupportedOperationException.class, () -> stats.getChannelCloseByType().put("reg", 1L));
    }

    private void assertAllFieldsEqual(TransportStats expected, TransportStats actual) {
        assertEquals(expected.getServerOpen(), actual.getServerOpen());
        assertEquals(expected.getTotalOutboundConnections(), actual.getTotalOutboundConnections());
        assertEquals(expected.getRxCount(), actual.getRxCount());
        assertEquals(expected.getRxSize(), actual.getRxSize());
        assertEquals(expected.getTxCount(), actual.getTxCount());
        assertEquals(expected.getTxSize(), actual.getTxSize());
        assertEquals(expected.getChannelCloseByType(), actual.getChannelCloseByType());
        assertEquals(expected.getOutgoingTimeouts(), actual.getOutgoingTimeouts());
        assertEquals(expected.getRequestsFailedOnDisconnect(), actual.getRequestsFailedOnDisconnect());
        assertEquals(expected.getConnectFailures(), actual.getConnectFailures());
        assertEquals(expected.getConnectTimeMillis(), actual.getConnectTimeMillis());
        assertEquals(expected.getConnectTimeMillisMax(), actual.getConnectTimeMillisMax());
    }

    private void assertNewCountersAbsent(TransportStats stats) {
        assertTrue(stats.getChannelCloseByType().isEmpty());
        assertEquals(0L, stats.getOutgoingTimeouts());
        assertEquals(0L, stats.getRequestsFailedOnDisconnect());
        assertEquals(0L, stats.getConnectFailures());
        assertEquals(0L, stats.getConnectTimeMillis());
        assertEquals(0L, stats.getConnectTimeMillisMax());
    }

    private static Map<String, Long> randomChannelCloseByType() {
        Map<String, Long> channelCloseByType = new HashMap<>();
        for (TransportRequestOptions.Type type : TransportRequestOptions.Type.values()) {
            channelCloseByType.put(type.name().toLowerCase(Locale.ROOT), randomNonNegativeLong());
        }
        return channelCloseByType;
    }

    private static TransportStats createTestInstance(Map<String, Long> channelCloseByType) {
        return new TransportStats.Builder().serverOpen(randomNonNegativeLong())
            .totalOutboundConnections(randomNonNegativeLong())
            .rxCount(randomNonNegativeLong())
            .rxSize(randomNonNegativeLong())
            .txCount(randomNonNegativeLong())
            .txSize(randomNonNegativeLong())
            .channelCloseByType(channelCloseByType)
            .outgoingTimeouts(randomNonNegativeLong())
            .requestsFailedOnDisconnect(randomNonNegativeLong())
            .connectFailures(randomNonNegativeLong())
            .connectTimeMillis(randomNonNegativeLong())
            .connectTimeMillisMax(randomNonNegativeLong())
            .build();
    }
}
