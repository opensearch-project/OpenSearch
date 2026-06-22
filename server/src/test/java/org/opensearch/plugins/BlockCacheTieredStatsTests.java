/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class BlockCacheTieredStatsTests extends AbstractWireSerializingTestCase<BlockCacheTieredStats> {

    @Override
    protected BlockCacheTieredStats createTestInstance() {
        return new BlockCacheTieredStats(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
    }

    @Override
    protected Writeable.Reader<BlockCacheTieredStats> instanceReader() {
        return BlockCacheTieredStats::new;
    }

    public void testToXContentRendersDataAndMetadataSections() throws IOException {
        BlockCacheTieredStats stats = new BlockCacheTieredStats(
            100,
            10,
            5000,
            500,
            2,
            200,
            8000,
            10000,
            64,
            50,
            5,
            2500,
            250,
            1,
            100,
            4000,
            5000,
            32
        );
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String json = builder.toString();
        assertTrue(json.contains("\"data_cache_stats\""));
        assertTrue(json.contains("\"metadata_cache_stats\""));
    }

    public void testDataCacheFields() throws IOException {
        BlockCacheTieredStats stats = new BlockCacheTieredStats(100, 10, 5000, 500, 2, 200, 8000, 10000, 64, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String json = builder.toString();
        assertTrue(json.contains("\"hit_count\":100"));
        assertTrue(json.contains("\"miss_count\":10"));
        assertTrue(json.contains("\"hit_bytes\":5000"));
        assertTrue(json.contains("\"miss_bytes\":500"));
        assertTrue(json.contains("\"eviction_count\":2"));
        assertTrue(json.contains("\"evictions_in_bytes\":200"));
        assertTrue(json.contains("\"used_in_bytes\":8000"));
        assertTrue(json.contains("\"capacity_in_bytes\":10000"));
        assertTrue(json.contains("\"active_in_bytes\":64"));
    }

    public void testMetadataCacheFields() throws IOException {
        BlockCacheTieredStats stats = new BlockCacheTieredStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 50, 5, 2500, 250, 1, 100, 4000, 5000, 32);
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String json = builder.toString();
        assertTrue(json.contains("\"metadata_cache_stats\""));
        assertTrue("metadata hit_count", json.contains("\"hit_count\":50"));
        assertTrue("metadata capacity", json.contains("\"capacity_in_bytes\":5000"));
    }
}
