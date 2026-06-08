/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.opensearch.analytics.spi.ExchangeSinkContext.ChildInput;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Validation coverage for {@link ChildInput}. The number-of-input-partitions field
 * is what the coordinator-reduce backend uses to size the input parallelism (one
 * {@code StreamingTable} partition per lane); pinning these contracts in a
 * dedicated unit avoids regressions in callers that construct it from a
 * resolved target list.
 */
public class ExchangeSinkContextChildInputTests extends OpenSearchTestCase {

    public void testTwoArgConstructorDefaultsNumInputPartitionsToOne() {
        ChildInput input = new ChildInput(7, new byte[] { 1, 2, 3 });
        assertEquals(7, input.childStageId());
        assertEquals(1, input.numInputPartitions());
    }

    public void testThreeArgConstructorAcceptsValidLaneCount() {
        ChildInput input = new ChildInput(0, new byte[] { 9 }, 4);
        assertEquals(0, input.childStageId());
        assertEquals(4, input.numInputPartitions());
    }

    public void testZeroLanesRejected() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new ChildInput(0, new byte[0], 0));
        assertTrue("error must mention numInputPartitions: " + e.getMessage(), e.getMessage().contains("numInputPartitions"));
    }

    public void testNegativeLanesRejected() {
        expectThrows(IllegalArgumentException.class, () -> new ChildInput(0, new byte[0], -1));
        expectThrows(IllegalArgumentException.class, () -> new ChildInput(0, new byte[0], Integer.MIN_VALUE));
    }

    public void testLargeLaneCountAccepted() {
        // No explicit upper bound on the SPI; the backend may impose its own cap.
        ChildInput input = new ChildInput(0, new byte[0], 1024);
        assertEquals(1024, input.numInputPartitions());
    }
}
