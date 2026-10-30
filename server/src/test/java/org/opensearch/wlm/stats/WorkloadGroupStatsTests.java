/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.stats;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.opensearch.test.VersionUtils;
import org.opensearch.wlm.ResourceType;
import org.opensearch.wlm.WorkloadGroupThrottleSettings;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class WorkloadGroupStatsTests extends AbstractWireSerializingTestCase<WorkloadGroupStats> {

    public void testToXContent() throws IOException {
        final Map<String, WorkloadGroupStats.WorkloadGroupStatsHolder> stats = new HashMap<>();
        final String workloadGroupId = "afakjklaj304041-afaka";
        stats.put(
            workloadGroupId,
            new WorkloadGroupStats.WorkloadGroupStatsHolder(
                123456789,
                13,
                2,
                0,
                5,
                8,
                Map.of(ResourceType.CPU, new WorkloadGroupStats.ResourceStats(0.3, 13, 2))
            )
        );
        XContentBuilder builder = JsonXContent.contentBuilder();
        WorkloadGroupStats workloadGroupStats = new WorkloadGroupStats(stats);
        builder.startObject();
        workloadGroupStats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        assertEquals(
            "{\"workload_groups\":{\"afakjklaj304041-afaka\":{\"total_completions\":123456789,\"total_rejections\":13,"
                + "\"total_cancellations\":0,\"total_throttled\":5,\"total_would_throttle\":8,\"total_queued\":0,"
                + "\"total_queue_rejections\":0,\"queued_current\":0,\"queued_peak\":0,\"total_queue_wait_millis\":0,"
                + "\"cpu\":{\"current_usage\":0.3,\"cancellations\":13,\"rejections\":2}}}}",
            builder.toString()
        );
    }

    public void testThrottledIsVersionGatedAndKeepsOlderStreamsAligned() throws IOException {
        WorkloadGroupStats original = new WorkloadGroupStats(
            Map.of(
                "group-1",
                new WorkloadGroupStats.WorkloadGroupStatsHolder(
                    100,
                    13,
                    2,
                    7,
                    5,
                    9,
                    Map.of(ResourceType.CPU, new WorkloadGroupStats.ResourceStats(0.3, 11, 4))
                )
            )
        );

        // A peer that supports throttling exchanges total_throttled and total_would_throttle, and everything after them on the wire stays
        // aligned.
        WorkloadGroupStats.WorkloadGroupStatsHolder current = copyInstance(original, WorkloadGroupThrottleSettings.MIN_SUPPORTED_VERSION)
            .getStats()
            .get("group-1");
        assertEquals(5, current.getThrottled());
        assertEquals(9, current.getWouldThrottle());
        assertEquals(100, current.getCompletions());
        assertEquals(0.3, current.getResourceStats().get(ResourceType.CPU).getCurrentUsage(), 0.0);

        // A pre-throttling peer writes neither counter: they read as 0 and the following resourceStats map still decodes.
        WorkloadGroupStats.WorkloadGroupStatsHolder legacy = copyInstance(
            original,
            VersionUtils.getPreviousVersion(WorkloadGroupThrottleSettings.MIN_SUPPORTED_VERSION)
        ).getStats().get("group-1");
        assertEquals(0, legacy.getThrottled());
        assertEquals(0, legacy.getWouldThrottle());
        assertEquals(100, legacy.getCompletions());
        assertEquals(13, legacy.getRejections());
        assertEquals(7, legacy.getCancellations());
        assertEquals(0.3, legacy.getResourceStats().get(ResourceType.CPU).getCurrentUsage(), 0.0);
        assertEquals(11, legacy.getResourceStats().get(ResourceType.CPU).getCancellations());
        assertEquals(4, legacy.getResourceStats().get(ResourceType.CPU).getRejections());
    }

    // This version has no request queueing: its own stats report the queue stats, which are on the wire and in toXContent,
    // as 0, and report no other counter in their place.
    public void testFromStateReportsTheQueueStatsAsZero() {
        WorkloadGroupState state = new WorkloadGroupState();
        state.totalCompletions.inc(1);
        state.totalRejections.inc(2);
        state.failures.inc(3);
        state.totalCancellations.inc(4);
        state.totalThrottled.inc(5);
        state.totalWouldThrottle.inc(6);
        WorkloadGroupStats.WorkloadGroupStatsHolder holder = WorkloadGroupStats.WorkloadGroupStatsHolder.from(state);
        assertEquals(5, holder.getThrottled());
        assertEquals(6, holder.getWouldThrottle());
        assertEquals(0, holder.getQueued());
        assertEquals(0, holder.getQueueRejections());
        assertEquals(0, holder.getQueuedCurrent());
        assertEquals(0, holder.getQueuedPeak());
        assertEquals(0, holder.getTotalQueueWaitMillis());
    }

    public void testQueueStatsAreVersionGatedAndKeepOlderStreamsAligned() throws IOException {
        WorkloadGroupStats original = new WorkloadGroupStats(
            Map.of(
                "group-1",
                new WorkloadGroupStats.WorkloadGroupStatsHolder(
                    100,
                    13,
                    2,
                    7,
                    5,
                    9,
                    21,
                    22,
                    23,
                    24,
                    25,
                    Map.of(ResourceType.CPU, new WorkloadGroupStats.ResourceStats(0.3, 11, 4))
                )
            )
        );

        WorkloadGroupStats.WorkloadGroupStatsHolder current = copyInstance(original, WorkloadGroupThrottleSettings.MIN_SUPPORTED_VERSION)
            .getStats()
            .get("group-1");
        assertEquals(original.getStats().get("group-1"), current);

        // A peer that predates throttling (and so queueing) writes none of the queue counters: they read as 0 and the
        // resourceStats map still decodes.
        WorkloadGroupStats.WorkloadGroupStatsHolder legacy = copyInstance(
            original,
            VersionUtils.getPreviousVersion(WorkloadGroupThrottleSettings.MIN_SUPPORTED_VERSION)
        ).getStats().get("group-1");
        assertEquals(0, legacy.getQueued());
        assertEquals(0, legacy.getQueueRejections());
        assertEquals(0, legacy.getQueuedCurrent());
        assertEquals(0, legacy.getQueuedPeak());
        assertEquals(0, legacy.getTotalQueueWaitMillis());
        assertEquals(100, legacy.getCompletions());
        assertEquals(11, legacy.getResourceStats().get(ResourceType.CPU).getCancellations());
    }

    // The holder is positional on the wire, so the order of its counters is part of the format: the queue stats follow
    // wouldThrottle in a fixed order, and the resource stats follow them. Read back field by field, since a round trip
    // through the same reader cannot see an order change made to both sides.
    public void testQueueStatsTrailWouldThrottleOnTheWire() throws IOException {
        Map<ResourceType, WorkloadGroupStats.ResourceStats> resourceStats = Map.of(
            ResourceType.CPU,
            new WorkloadGroupStats.ResourceStats(0.3, 11, 4)
        );
        WorkloadGroupStats.WorkloadGroupStatsHolder holder = new WorkloadGroupStats.WorkloadGroupStatsHolder(
            1,
            2,
            3,
            4,
            5,
            6,
            21,
            22,
            23,
            24,
            25,
            resourceStats
        );
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(WorkloadGroupThrottleSettings.MIN_SUPPORTED_VERSION);
            holder.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setVersion(WorkloadGroupThrottleSettings.MIN_SUPPORTED_VERSION);
                assertEquals("completions", 1, in.readVLong());
                assertEquals("rejections", 2, in.readVLong());
                assertEquals("failures", 3, in.readVLong());
                assertEquals("cancellations", 4, in.readVLong());
                assertEquals("throttled", 5, in.readVLong());
                assertEquals("wouldThrottle", 6, in.readVLong());
                assertEquals("queued", 21, in.readVLong());
                assertEquals("queueRejections", 22, in.readVLong());
                assertEquals("queuedCurrent", 23, in.readVLong());
                assertEquals("queuedPeak", 24, in.readVLong());
                assertEquals("totalQueueWaitMillis", 25, in.readVLong());
                assertEquals(resourceStats, in.readMap(i -> ResourceType.fromName(i.readString()), WorkloadGroupStats.ResourceStats::new));
                assertEquals(0, in.available());
            }
        }
    }

    // Every queue stat holds a different value, so a field written under another stat's name is visible.
    public void testToXContentNamesEachQueueStat() throws IOException {
        WorkloadGroupStats stats = new WorkloadGroupStats(
            Map.of("group-1", new WorkloadGroupStats.WorkloadGroupStatsHolder(1, 2, 3, 4, 5, 6, 21, 22, 23, 24, 25, Map.of()))
        );
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        assertEquals(
            "{\"workload_groups\":{\"group-1\":{\"total_completions\":1,\"total_rejections\":2,\"total_cancellations\":4,"
                + "\"total_throttled\":5,\"total_would_throttle\":6,\"total_queued\":21,\"total_queue_rejections\":22,"
                + "\"queued_current\":23,\"queued_peak\":24,\"total_queue_wait_millis\":25}}}",
            builder.toString()
        );
    }

    @Override
    protected Writeable.Reader<WorkloadGroupStats> instanceReader() {
        return WorkloadGroupStats::new;
    }

    @Override
    protected WorkloadGroupStats createTestInstance() {
        return new WorkloadGroupStats(Map.of(randomAlphaOfLength(10), randomStatsHolder()));
    }

    // A random value for EVERY field, so the wire round-trip (testSerialization) and equals/hashCode
    // (testEqualsAndHashcode) exercise the queue fields too; a write/read order swap among them would otherwise be invisible.
    private static WorkloadGroupStats.WorkloadGroupStatsHolder randomStatsHolder() {
        return new WorkloadGroupStats.WorkloadGroupStatsHolder(
            randomNonNegativeLong(), // completions
            randomNonNegativeLong(), // rejections
            randomNonNegativeLong(), // failures
            randomNonNegativeLong(), // cancellations
            randomNonNegativeLong(), // throttled
            randomNonNegativeLong(), // wouldThrottle
            randomNonNegativeLong(), // queued
            randomNonNegativeLong(), // queueRejections
            randomNonNegativeLong(), // queuedCurrent
            randomNonNegativeLong(), // queuedPeak
            randomNonNegativeLong(), // totalQueueWaitMillis
            Map.of(
                ResourceType.CPU,
                new WorkloadGroupStats.ResourceStats(
                    randomDoubleBetween(0.0, 0.90, false),
                    randomNonNegativeLong(),
                    randomNonNegativeLong()
                )
            )
        );
    }

    // Changes exactly one randomly chosen holder field, copying all others (failures included), so the mutation check
    // proves every field participates in equals/hashCode.
    @Override
    protected WorkloadGroupStats mutateInstance(WorkloadGroupStats instance) {
        Map<String, WorkloadGroupStats.WorkloadGroupStatsHolder> stats = new HashMap<>(instance.getStats());
        String key = stats.keySet().iterator().next();
        WorkloadGroupStats.WorkloadGroupStatsHolder h = stats.get(key);
        long[] fields = new long[] {
            h.getCompletions(),
            h.getRejections(),
            h.getFailures(),
            h.getCancellations(),
            h.getThrottled(),
            h.getWouldThrottle(),
            h.getQueued(),
            h.getQueueRejections(),
            h.getQueuedCurrent(),
            h.getQueuedPeak(),
            h.getTotalQueueWaitMillis() };
        int mutated = randomIntBetween(0, fields.length - 1);
        fields[mutated] = fields[mutated] == Long.MAX_VALUE ? 0 : fields[mutated] + 1;
        stats.put(
            key,
            new WorkloadGroupStats.WorkloadGroupStatsHolder(
                fields[0],
                fields[1],
                fields[2],
                fields[3],
                fields[4],
                fields[5],
                fields[6],
                fields[7],
                fields[8],
                fields[9],
                fields[10],
                h.getResourceStats()
            )
        );
        return new WorkloadGroupStats(stats);
    }
}
