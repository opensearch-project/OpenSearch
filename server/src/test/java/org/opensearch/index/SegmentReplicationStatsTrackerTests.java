/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.indices.IndicesService;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;

public class SegmentReplicationStatsTrackerTests extends OpenSearchTestCase {

    private IndicesService indicesService = mock(IndicesService.class);

    public void testRejectedCountWhenEmpty() {
        SegmentReplicationStatsTracker segmentReplicationStatsTracker = new SegmentReplicationStatsTracker(indicesService);

        // Verify that total rejection count is 0 on an empty rejectionCount map in statsTracker.
        assertTrue(segmentReplicationStatsTracker.getRejectionCount().isEmpty());
        assertEquals(segmentReplicationStatsTracker.getTotalRejectionStats().getTotalRejectionCount(), 0L);
    }

}
