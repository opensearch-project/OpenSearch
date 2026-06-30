/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.test.OpenSearchTestCase;

public class ImmutableCacheStatsTests extends OpenSearchTestCase {
    public void testSerialization() throws Exception {
        ImmutableCacheStats immutableCacheStats = new ImmutableCacheStats(1, 2, 3, 4, 5);
        BytesStreamOutput os = new BytesStreamOutput();
        immutableCacheStats.writeTo(os);
        BytesStreamInput is = new BytesStreamInput(BytesReference.toBytes(os.bytes()));
        ImmutableCacheStats deserialized = new ImmutableCacheStats(is);

        assertEquals(immutableCacheStats, deserialized);
    }

    public void testAddSnapshots() throws Exception {
        ImmutableCacheStats ics1 = new ImmutableCacheStats(1, 2, 3, 4, 5);
        ImmutableCacheStats ics2 = new ImmutableCacheStats(6, 7, 8, 9, 10);
        ImmutableCacheStats expected = new ImmutableCacheStats(7, 9, 11, 13, 15);
        assertEquals(expected, ImmutableCacheStats.addSnapshots(ics1, ics2));
    }

    public void testEqualsAndHash() throws Exception {
        ImmutableCacheStats ics1 = new ImmutableCacheStats(1, 2, 3, 4, 5);
        ImmutableCacheStats ics2 = new ImmutableCacheStats(1, 2, 3, 4, 5);
        ImmutableCacheStats ics3 = new ImmutableCacheStats(0, 2, 3, 4, 5);

        assertEquals(ics1, ics2);
        assertNotEquals(ics1, ics3);
        assertNotEquals(ics1, null);
        assertNotEquals(ics1, "string");

        assertEquals(ics1.hashCode(), ics2.hashCode());
        assertNotEquals(ics1.hashCode(), ics3.hashCode());
    }
}
