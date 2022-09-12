/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.stats;

import org.opensearch.common.io.stream.Writeable;
import org.opensearch.test.AbstractWireSerializingTestCase;

public class CancelledTaskStatsTests extends AbstractWireSerializingTestCase<CancelledTaskStats> {
    @Override
    protected Writeable.Reader<CancelledTaskStats> instanceReader() {
        return CancelledTaskStats::new;
    }

    @Override
    protected CancelledTaskStats createTestInstance() {
        return randomInstance();
    }

    public static CancelledTaskStats randomInstance() {
        return new CancelledTaskStats(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong());
    }
}
