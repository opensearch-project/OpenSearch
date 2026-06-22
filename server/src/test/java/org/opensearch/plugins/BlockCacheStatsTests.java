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

public class BlockCacheStatsTests extends AbstractWireSerializingTestCase<BlockCacheStats> {

    @Override
    protected BlockCacheStats createTestInstance() {
        BlockCacheTieredStats tiered = randomBoolean() ? randomTieredStats() : null;
        return new BlockCacheStats(
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
            tiered
        );
    }

    @Override
    protected Writeable.Reader<BlockCacheStats> instanceReader() {
        return BlockCacheStats::new;
    }

    public void testToXContentStructure() throws IOException {
        BlockCacheStats stats = new BlockCacheStats(10, 5, 1024, 512, 3, 300, 2, 200, 0, 4096, 8192, 512);
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String json = builder.toString();
        assertTrue(json.contains("\"block_cache\""));
        assertTrue(json.contains("\"over_all_stats\""));
        assertTrue(json.contains("\"block_file_stats\""));
        assertTrue(json.contains("\"full_file_stats\""));
        assertTrue(json.contains("\"pinned_file_stats\""));
        assertTrue(json.contains("\"hit_count\":10"));
        assertTrue(json.contains("\"miss_count\":5"));
        assertTrue(json.contains("\"used_in_bytes\":4096"));
        assertFalse("Non-tiered stats should not contain data_cache_stats", json.contains("\"data_cache_stats\""));
    }

    public void testToXContentWithTieredStats() throws IOException {
        BlockCacheTieredStats tiered = new BlockCacheTieredStats(
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
        BlockCacheStats stats = new BlockCacheStats(150, 15, 7500, 750, 3, 300, 0, 0, 0, 12000, 15000, 96, tiered);
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String json = builder.toString();
        assertTrue(json.contains("\"data_cache_stats\""));
        assertTrue(json.contains("\"metadata_cache_stats\""));
        assertTrue(json.contains("\"capacity_in_bytes\":10000"));
        assertTrue(json.contains("\"capacity_in_bytes\":5000"));
    }

    public void testNonTieredConstructorHasNullTieredStats() {
        BlockCacheStats stats = new BlockCacheStats(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
        assertNull(stats.tieredStats());
    }

    private BlockCacheTieredStats randomTieredStats() {
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
}
