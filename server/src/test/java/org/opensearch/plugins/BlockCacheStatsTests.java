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
            randomNonNegativeLong()
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
    }
}
