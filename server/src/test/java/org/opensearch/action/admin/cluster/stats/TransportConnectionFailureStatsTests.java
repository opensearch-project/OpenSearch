/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.stats;

import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.opensearch.transport.TransportConnectionFailureStats;

public class TransportConnectionFailureStatsTests extends AbstractWireSerializingTestCase<TransportConnectionFailureStats> {

    @Override
    protected Writeable.Reader<TransportConnectionFailureStats> instanceReader() {
        return TransportConnectionFailureStats::new;
    }

    @Override
    protected TransportConnectionFailureStats createTestInstance() {
        return randomInstance();
    }

    public static TransportConnectionFailureStats randomInstance() {
        TransportConnectionFailureStats randomStats = new TransportConnectionFailureStats();
        if (randomBoolean()) {
            randomStats.updateConnectionFailureCount("node1");
            randomStats.updateConnectionFailureCount("node2");
            randomStats.updateConnectionFailureCount("node1");
        }
        return randomStats;
    }
}
