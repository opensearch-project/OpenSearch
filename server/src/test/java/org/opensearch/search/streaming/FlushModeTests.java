/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.streaming;

import org.opensearch.test.OpenSearchTestCase;

public class FlushModeTests extends OpenSearchTestCase {

    public void testFlushModeValues() {
        assertEquals(3, FlushMode.values().length);
        assertEquals(FlushMode.PER_SEGMENT, FlushMode.valueOf("PER_SEGMENT"));
        assertEquals(FlushMode.PER_SLICE, FlushMode.valueOf("PER_SLICE"));
        assertEquals(FlushMode.PER_SHARD, FlushMode.valueOf("PER_SHARD"));
    }

    public void testFlushModeOrdinals() {
        assertEquals(0, FlushMode.PER_SEGMENT.ordinal());
        assertEquals(1, FlushMode.PER_SLICE.ordinal());
        assertEquals(2, FlushMode.PER_SHARD.ordinal());
    }
}
