/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.slowlogs;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

/**
 * Test class to verify JSON serialization of TieredStoragePerQueryMetricImpl.
 */
public class TieredStoragePerQueryMetricImplJsonTests extends OpenSearchTestCase {

    public void testToXContentBasic() throws IOException {
        TieredStoragePerQueryMetricImpl metric = new TieredStoragePerQueryMetricImpl("task-123", "shard-0");

        // Record some sample data
        metric.recordFileAccess("file1.block_0_1", true);  // hit
        metric.recordFileAccess("file1.block_0_2", false); // miss
        metric.recordPrefetch("file2", 1);
        metric.recordReadAhead("file3", 2);
        metric.recordEndTime();

        // Test XContentBuilder serialization
        XContentBuilder builder = XContentFactory.jsonBuilder();
        metric.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        // Verify JSON contains expected fields
        assertNotNull(json);
        assertTrue(json.contains("\"parentTask\":\"task-123\""));
        assertTrue(json.contains("\"shardId\":\"shard-0\""));
        assertTrue(json.contains("\"summary\""));
        assertTrue(json.contains("\"details\""));
        assertTrue(json.contains("\"timestamps\""));
        assertTrue(json.contains("\"fileCache\""));
        assertTrue(json.contains("\"prefetch\""));
        assertTrue(json.contains("\"readAhead\""));
    }

    public void testToStringUsesXContentBuilder() throws IOException {
        TieredStoragePerQueryMetricImpl metric = new TieredStoragePerQueryMetricImpl("task-456", "shard-1");

        // Record some sample data
        metric.recordFileAccess("test.block_0_5", true);
        metric.recordPrefetch("prefetch-file", 10);
        metric.recordEndTime();

        // Test toString method (which should use XContentBuilder internally)
        String jsonString = metric.toString();

        // Verify the toString output is valid JSON
        assertNotNull(jsonString);
        assertTrue(jsonString.contains("\"parentTask\":\"task-456\""));
        assertTrue(jsonString.contains("\"shardId\":\"shard-1\""));
    }

    public void testEmptyMetricSerialization() throws IOException {
        TieredStoragePerQueryMetricImpl metric = new TieredStoragePerQueryMetricImpl("empty-task", "empty-shard");
        metric.recordEndTime();

        XContentBuilder builder = XContentFactory.jsonBuilder();
        metric.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        // Should still contain basic structure even with no data
        assertTrue(json.contains("\"parentTask\":\"empty-task\""));
        assertTrue(json.contains("\"shardId\":\"empty-shard\""));
        assertTrue(json.contains("\"summary\""));
        assertTrue(json.contains("\"details\""));
    }
}
