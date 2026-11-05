/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.rescore;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

/**
 * Test fixing the ordinals and names in {@link RescoreReorderMode}. These should not be changed since we
 * use the names in the parser and the ordinals in serialization.
 */
public class RescoreReorderModeTests extends OpenSearchTestCase {

    /**
     * Test {@link RescoreReorderMode} enum ordinals and names, since serialization relies on them
     */
    public void testRescoreReorderMode() {
        assertEquals(0, RescoreReorderMode.LEGACY.ordinal());
        assertEquals("legacy", RescoreReorderMode.LEGACY.toString());

        assertEquals(1, RescoreReorderMode.ALL_HITS.ordinal());
        assertEquals("all_hits", RescoreReorderMode.ALL_HITS.toString());

        assertEquals(2, RescoreReorderMode.WINDOW_ONLY.ordinal());
        assertEquals("window_only", RescoreReorderMode.WINDOW_ONLY.toString());
    }

    public void testFromString() {
        assertEquals(RescoreReorderMode.LEGACY, RescoreReorderMode.fromString("legacy"));
        assertEquals(RescoreReorderMode.ALL_HITS, RescoreReorderMode.fromString("all_hits"));
        assertEquals(RescoreReorderMode.WINDOW_ONLY, RescoreReorderMode.fromString("window_only"));
        // case insensitive, like score_mode
        assertEquals(RescoreReorderMode.ALL_HITS, RescoreReorderMode.fromString("ALL_HITS"));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> RescoreReorderMode.fromString("bogus"));
        assertEquals("illegal reorder_mode [bogus]", e.getMessage());
    }

    public void testStreamRoundTrip() throws IOException {
        for (RescoreReorderMode mode : RescoreReorderMode.values()) {
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                mode.writeTo(out);
                try (StreamInput in = out.bytes().streamInput()) {
                    assertEquals(mode, RescoreReorderMode.readFromStream(in));
                }
            }
        }
    }
}
