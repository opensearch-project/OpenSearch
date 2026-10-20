/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

import org.opensearch.Version;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.stats.AnalyticsBackendTaskCancellationStats;
import org.opensearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class TaskCancellationStatsTests extends AbstractWireSerializingTestCase<TaskCancellationStats> {
    @Override
    protected Writeable.Reader<TaskCancellationStats> instanceReader() {
        return TaskCancellationStats::new;
    }

    @Override
    protected TaskCancellationStats createTestInstance() {
        return randomInstance();
    }

    public static TaskCancellationStats randomInstance() {
        return new TaskCancellationStats(
            SearchTaskCancellationStatsTests.randomInstance(),
            SearchShardTaskCancellationStatsTests.randomInstance(),
            randomBoolean() ? randomNativeStats() : null
        );
    }

    private static AnalyticsBackendTaskCancellationStats randomNativeStats() {
        return new AnalyticsBackendTaskCancellationStats(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
    }

    // -----------------------------------------------------------------------
    // Property 5: TaskCancellationStats round-trip (with and without native stats)
    // Validates: Requirements 9.4, 9.5, 9.6
    // -----------------------------------------------------------------------

    /**
     * Property test: For any valid TaskCancellationStats instance (with native stats),
     * serializing via writeTo and deserializing via the StreamInput constructor
     * produces an equal instance.
     *
     * **Validates: Requirements 9.4, 9.5, 9.6**
     */
    public void testRoundTripPropertyWithNativeStats() throws IOException {
        for (int i = 0; i < 100; i++) {
            TaskCancellationStats original = new TaskCancellationStats(
                SearchTaskCancellationStatsTests.randomInstance(),
                SearchShardTaskCancellationStatsTests.randomInstance(),
                new AnalyticsBackendTaskCancellationStats(
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong()
                )
            );

            BytesStreamOutput out = new BytesStreamOutput();
            out.setVersion(Version.V_3_7_0);
            original.writeTo(out);

            StreamInput in = out.bytes().streamInput();
            in.setVersion(Version.V_3_7_0);
            TaskCancellationStats deserialized = new TaskCancellationStats(in);

            assertEquals("Round-trip with native stats failed for instance " + i, original, deserialized);
        }
    }

    /**
     * Property test: For any valid TaskCancellationStats instance (without native stats),
     * serializing via writeTo and deserializing via the StreamInput constructor
     * produces an equal instance.
     *
     * **Validates: Requirements 9.4, 9.5, 9.6**
     */
    public void testRoundTripPropertyWithoutNativeStats() throws IOException {
        for (int i = 0; i < 100; i++) {
            TaskCancellationStats original = new TaskCancellationStats(
                SearchTaskCancellationStatsTests.randomInstance(),
                SearchShardTaskCancellationStatsTests.randomInstance(),
                null
            );

            BytesStreamOutput out = new BytesStreamOutput();
            out.setVersion(Version.V_3_7_0);
            original.writeTo(out);

            StreamInput in = out.bytes().streamInput();
            in.setVersion(Version.V_3_7_0);
            TaskCancellationStats deserialized = new TaskCancellationStats(in);

            assertEquals("Round-trip without native stats failed for instance " + i, original, deserialized);
        }
    }

    // -----------------------------------------------------------------------
    // Unit tests for extended TaskCancellationStats
    // Validates: Requirements 9.2, 9.3, 9.4, 9.5
    // -----------------------------------------------------------------------

    /**
     * Test toXContent with native stats renders analytics_search_task and analytics_search_shard_task.
     *
     * Validates: Requirements 9.2
     */
    public void testToXContentWithNativeStats() throws IOException {
        SearchTaskCancellationStats searchStats = new SearchTaskCancellationStats(3, 10);
        SearchShardTaskCancellationStats shardStats = new SearchShardTaskCancellationStats(5, 20);
        AnalyticsBackendTaskCancellationStats nativeStats = new AnalyticsBackendTaskCancellationStats(2, 147, 5, 892);

        TaskCancellationStats stats = new TaskCancellationStats(searchStats, shardStats, nativeStats);

        XContentBuilder builder = MediaTypeRegistry.contentBuilder(MediaTypeRegistry.JSON);
        builder.startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        String json = BytesReference.bytes(builder).utf8ToString();

        // Verify search_task is present
        assertTrue("JSON should contain search_task", json.contains("\"search_task\""));
        // Verify search_shard_task is present
        assertTrue("JSON should contain search_shard_task", json.contains("\"search_shard_task\""));
        // Verify analytics_search_task is present
        assertTrue("JSON should contain analytics_search_task", json.contains("\"analytics_search_task\""));
        // Verify analytics_search_shard_task is present
        assertTrue("JSON should contain analytics_search_shard_task", json.contains("\"analytics_search_shard_task\""));
        // Verify native counter values
        assertTrue("JSON should contain native search task current count", json.contains("\"current_count_post_cancel\":2"));
        assertTrue("JSON should contain native search task total count", json.contains("\"total_count_post_cancel\":147"));
        assertTrue("JSON should contain native shard task current count", json.contains("\"current_count_post_cancel\":5"));
        assertTrue("JSON should contain native shard task total count", json.contains("\"total_count_post_cancel\":892"));
    }

    /**
     * Test toXContent without native stats produces unchanged output (no native fields).
     *
     * Validates: Requirements 9.3
     */
    public void testToXContentWithoutNativeStats() throws IOException {
        SearchTaskCancellationStats searchStats = new SearchTaskCancellationStats(3, 10);
        SearchShardTaskCancellationStats shardStats = new SearchShardTaskCancellationStats(5, 20);

        TaskCancellationStats stats = new TaskCancellationStats(searchStats, shardStats);

        XContentBuilder builder = MediaTypeRegistry.contentBuilder(MediaTypeRegistry.JSON);
        builder.startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        String json = BytesReference.bytes(builder).utf8ToString();

        // Verify search_task and search_shard_task are present
        assertTrue("JSON should contain search_task", json.contains("\"search_task\""));
        assertTrue("JSON should contain search_shard_task", json.contains("\"search_shard_task\""));
        // Verify native fields are NOT present
        assertFalse("JSON should NOT contain analytics_search_task", json.contains("\"analytics_search_task\""));
        assertFalse("JSON should NOT contain analytics_search_shard_task", json.contains("\"analytics_search_shard_task\""));
    }

    /**
     * Test serialization backward compatibility: serialize with version before V_3_7_0,
     * deserialize, verify nativeStats is null.
     *
     * Validates: Requirements 9.4, 9.5
     */
    public void testSerializationBackwardCompatibility() throws IOException {
        SearchTaskCancellationStats searchStats = new SearchTaskCancellationStats(3, 10);
        SearchShardTaskCancellationStats shardStats = new SearchShardTaskCancellationStats(5, 20);
        AnalyticsBackendTaskCancellationStats nativeStats = new AnalyticsBackendTaskCancellationStats(2, 147, 5, 892);

        TaskCancellationStats original = new TaskCancellationStats(searchStats, shardStats, nativeStats);

        // Serialize with a version before V_3_7_0 (native stats should be omitted)
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.V_3_6_0);
        original.writeTo(out);

        // Deserialize with the same older version
        StreamInput in = out.bytes().streamInput();
        in.setVersion(Version.V_3_6_0);
        TaskCancellationStats deserialized = new TaskCancellationStats(in);

        // Native stats should be null after round-trip through older version
        assertNull("Native stats should be null when deserialized from older version", deserialized.getNativeStats());
        // But the other stats should still be correct
        assertEquals(searchStats, deserialized.getSearchTaskCancellationStats());
        assertEquals(shardStats, deserialized.getSearchShardTaskCancellationStats());
    }
}
