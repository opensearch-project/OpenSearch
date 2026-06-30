/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.stats;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Wire-format round-trip tests for {@link AnalyticsStats} and its nested
 * records. Required because {@link AnalyticsStats} is shipped from each node
 * to the coordinator inside an
 * {@link org.opensearch.analytics.stats.transport.AnalyticsStatsNodeResponse}
 * via {@link org.opensearch.action.support.nodes.TransportNodesAction}.
 */
public class AnalyticsStatsTests extends OpenSearchTestCase {

    public void testLatencyStatsRoundTrip() throws IOException {
        AnalyticsStats.LatencyStats original = new AnalyticsStats.LatencyStats(100, 12345);
        AnalyticsStats.LatencyStats copy = roundTrip(original, AnalyticsStats.LatencyStats::new);
        assertEquals(original, copy);
    }

    public void testQueriesRoundTrip() throws IOException {
        AnalyticsStats.Queries original = new AnalyticsStats.Queries(
            new AnalyticsStats.LatencyStats(50, 5000),
            new AnalyticsStats.LatencyStats(50, 1500)
        );
        AnalyticsStats.Queries copy = roundTrip(original, AnalyticsStats.Queries::new);
        assertEquals(original, copy);
    }

    public void testStageBucketRoundTrip() throws IOException {
        AnalyticsStats.StageBucket original = new AnalyticsStats.StageBucket(10, 8, 1, 1, 12345, new AnalyticsStats.LatencyStats(10, 500));
        AnalyticsStats.StageBucket copy = roundTrip(original, AnalyticsStats.StageBucket::new);
        assertEquals(original, copy);
    }

    public void testFragmentsRoundTrip() throws IOException {
        AnalyticsStats.Fragments original = new AnalyticsStats.Fragments(42, 40, 2, new AnalyticsStats.LatencyStats(42, 800));
        AnalyticsStats.Fragments copy = roundTrip(original, AnalyticsStats.Fragments::new);
        assertEquals(original, copy);
    }

    public void testAnalyticsStatsRoundTrip() throws IOException {
        Map<String, AnalyticsStats.StageBucket> stages = new TreeMap<>();
        stages.put("SHARD_FRAGMENT", new AnalyticsStats.StageBucket(20, 18, 1, 1, 99999, new AnalyticsStats.LatencyStats(20, 400)));
        stages.put("COORDINATOR_REDUCE", new AnalyticsStats.StageBucket(5, 5, 0, 0, 0, new AnalyticsStats.LatencyStats(5, 50)));

        AnalyticsStats original = new AnalyticsStats(
            new AnalyticsStats.Queries(new AnalyticsStats.LatencyStats(25, 600), new AnalyticsStats.LatencyStats(25, 100)),
            stages,
            new AnalyticsStats.Fragments(40, 38, 2, new AnalyticsStats.LatencyStats(40, 750))
        );

        AnalyticsStats copy = roundTrip(original, AnalyticsStats::new);
        assertEquals(original.queries(), copy.queries());
        assertEquals(original.fragments(), copy.fragments());
        assertEquals(original.stagesByType(), copy.stagesByType());
    }

    public void testEmptyAnalyticsStatsRoundTrip() throws IOException {
        AnalyticsStats original = new AnalyticsStats(
            new AnalyticsStats.Queries(AnalyticsStats.LatencyStats.EMPTY, AnalyticsStats.LatencyStats.EMPTY),
            new TreeMap<>(),
            new AnalyticsStats.Fragments(0, 0, 0, AnalyticsStats.LatencyStats.EMPTY)
        );
        AnalyticsStats copy = roundTrip(original, AnalyticsStats::new);
        assertEquals(original.queries(), copy.queries());
        assertEquals(original.fragments(), copy.fragments());
        assertTrue("empty stages map round-trips empty", copy.stagesByType().isEmpty());
    }

    @FunctionalInterface
    private interface IOFunction<T, R> {
        R apply(T t) throws IOException;
    }

    private static <T extends org.opensearch.core.common.io.stream.Writeable> T roundTrip(T original, IOFunction<StreamInput, T> reader)
        throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return reader.apply(in);
            }
        }
    }
}
