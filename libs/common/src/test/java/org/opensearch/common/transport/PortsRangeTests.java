/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.transport;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;

public class PortsRangeTests extends OpenSearchTestCase {

    public void testDescendingRangeHasCorrectDiagnostic() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> new PortsRange("9302-9300").ports());

        assertEquals("Start port [9302] must be less than or equal to end port [9300]", exception.getMessage());
    }

    public void testValidRangeReturnsAllPorts() {
        assertArrayEquals(new int[] { 9300, 9301, 9302 }, new PortsRange("9300-9302").ports());
    }

    public void testEqualStartAndEndPortIsValid() {
        assertArrayEquals(new int[] { 9300 }, new PortsRange("9300-9300").ports());
    }

    public void testIterationStopsWhenCallbackSucceeds() {
        List<Integer> visitedPorts = new ArrayList<>();

        boolean stopped = new PortsRange("9300-9305").iterate(port -> {
            visitedPorts.add(port);
            return port == 9301;
        });

        assertTrue(stopped);
        assertEquals(List.of(9300, 9301), visitedPorts);
    }
}
