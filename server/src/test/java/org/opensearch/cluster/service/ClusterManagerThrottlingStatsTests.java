/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service;

import org.opensearch.common.io.stream.Writeable;
import org.opensearch.test.AbstractWireSerializingTestCase;

public class ClusterManagerThrottlingStatsTests extends AbstractWireSerializingTestCase<ClusterManagerThrottlingStats> {
    @Override
    protected Writeable.Reader<ClusterManagerThrottlingStats> instanceReader() {
        return ClusterManagerThrottlingStats::new;
    }

    @Override
    protected ClusterManagerThrottlingStats createTestInstance() {
        return randomInstance();
    }

    public static ClusterManagerThrottlingStats randomInstance() {
        ClusterManagerThrottlingStats randomStats = new ClusterManagerThrottlingStats();
        randomStats.onThrottle(randomAlphaOfLengthBetween(3, 10), randomInt());
        return randomStats;
    }
}
