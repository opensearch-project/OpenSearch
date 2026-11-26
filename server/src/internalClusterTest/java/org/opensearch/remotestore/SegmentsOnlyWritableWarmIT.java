/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Before;

/**
 * A warm index reads the segments it has evicted back from the remote store, so it needs remote segments and never a
 * remote translog. Nothing in the inherited tests should then depend on the mode, which is the claim being made here.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, supportsDedicatedMasters = false)
public class SegmentsOnlyWritableWarmIT extends WritableWarmIT {

    @Before
    public void startNodesInSegmentsOnlyMode() {
        segmentsOnly = true;
    }
}
