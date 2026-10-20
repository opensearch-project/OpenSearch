/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.blockcache;

import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.test.AbstractWireSerializingTestCase;

import java.util.Collections;

public class NodePruneBlockCacheResponseTests extends AbstractWireSerializingTestCase<NodePruneBlockCacheResponse> {

    @Override
    protected NodePruneBlockCacheResponse createTestInstance() {
        DiscoveryNode node = new DiscoveryNode(
            randomAlphaOfLength(8),
            randomAlphaOfLength(8),
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.emptySet(),
            Version.CURRENT
        );
        return new NodePruneBlockCacheResponse(node, randomBoolean());
    }

    @Override
    protected Writeable.Reader<NodePruneBlockCacheResponse> instanceReader() {
        return NodePruneBlockCacheResponse::new;
    }

    @Override
    protected NodePruneBlockCacheResponse mutateInstance(NodePruneBlockCacheResponse instance) {
        return new NodePruneBlockCacheResponse(instance.getNode(), !instance.isCleared());
    }
}
