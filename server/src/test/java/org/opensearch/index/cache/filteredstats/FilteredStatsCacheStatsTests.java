/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.cache.filteredstats;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

/** Wire and JSON form of {@link FilteredStatsCacheStats}. */
public class FilteredStatsCacheStatsTests extends OpenSearchTestCase {

    public void testRoundTrip() throws IOException {
        FilteredStatsCacheStats stats = new FilteredStatsCacheStats(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            stats.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertEquals(stats, new FilteredStatsCacheStats(in));
            }
        }
    }

    public void testRendersEveryCounter() throws IOException {
        FilteredStatsCacheStats stats = new FilteredStatsCacheStats(2048, 3, 40, 5, 1, 2);
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        assertEquals(
            "{\"filtered_stats_cache\":{\"memory_size_in_bytes\":2048,\"entries\":3,\"hit_count\":40,\"miss_count\":5,"
                + "\"evictions\":1,\"refusals\":2}}",
            builder.toString()
        );
    }
}
