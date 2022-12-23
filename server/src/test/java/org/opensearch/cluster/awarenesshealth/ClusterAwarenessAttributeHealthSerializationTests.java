/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.awarenesshealth;

import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ClusterAwarenessAttributeHealthSerializationTests extends AbstractSerializingTestCase<ClusterAwarenessAttributeHealth> {

    @Override
    protected ClusterAwarenessAttributeHealth doParseInstance(XContentParser parser) throws IOException {
        return ClusterAwarenessAttributeHealth.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<ClusterAwarenessAttributeHealth> instanceReader() {
        return ClusterAwarenessAttributeHealth::new;
    }

    @Override
    protected ClusterAwarenessAttributeHealth createTestInstance() {
        Map<String, ClusterAwarenessAttributeValueHealth> clusterAwarenessAttributeValueHealthMap = new HashMap<>();
        ClusterAwarenessAttributeValueHealth clusterAwarenessAttributeValueHealth1 = new ClusterAwarenessAttributeValueHealth(
            "zone-1",
            2,
            0,
            0,
            2,
            2,
            1.0
        );

        ClusterAwarenessAttributeValueHealth clusterAwarenessAttributeValueHealth2 = new ClusterAwarenessAttributeValueHealth(
            "zone-2",
            2,
            0,
            0,
            2,
            2,
            1.0
        );

        ClusterAwarenessAttributeValueHealth clusterAwarenessAttributeValueHealth3 = new ClusterAwarenessAttributeValueHealth(
            "zone-3",
            2,
            0,
            0,
            2,
            2,
            0.0
        );

        clusterAwarenessAttributeValueHealthMap.put(clusterAwarenessAttributeValueHealth1.getName(), clusterAwarenessAttributeValueHealth1);
        clusterAwarenessAttributeValueHealthMap.put(clusterAwarenessAttributeValueHealth2.getName(), clusterAwarenessAttributeValueHealth2);
        clusterAwarenessAttributeValueHealthMap.put(clusterAwarenessAttributeValueHealth3.getName(), clusterAwarenessAttributeValueHealth3);

        return new ClusterAwarenessAttributeHealth("zone", clusterAwarenessAttributeValueHealthMap);
    }
}
