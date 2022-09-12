/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.stats;

import org.opensearch.common.collect.MapBuilder;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class CancellationStatsTests extends AbstractWireSerializingTestCase<CancellationStats> {
    @Override
    protected Writeable.Reader<CancellationStats> instanceReader() {
        return CancellationStats::new;
    }

    @Override
    protected CancellationStats createTestInstance() {
        return randomInstance();
    }

    public void testLastCancelledTaskStatsIsNull() throws IOException {
        CancellationStats expected = new CancellationStats(
            randomNonNegativeLong(),
            new MapBuilder<String, Long>().put("foo", randomNonNegativeLong()).put("bar", randomNonNegativeLong()).immutableMap(),
            randomNonNegativeLong(),
            null
        );

        CancellationStats actual = copyInstance(expected);
        assertEquals(expected, actual);
    }

    public static CancellationStats randomInstance() {
        return new CancellationStats(
            randomNonNegativeLong(),
            new MapBuilder<String, Long>().put("foo", randomNonNegativeLong()).put("bar", randomNonNegativeLong()).immutableMap(),
            randomNonNegativeLong(),
            CancelledTaskStatsTests.randomInstance()
        );
    }
}
