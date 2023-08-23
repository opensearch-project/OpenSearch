/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.Version;
import org.opensearch.action.FailedNodeException;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

public class GetAllPitNodesResponseTests extends OpenSearchTestCase {
    protected void assertEqualInstances(GetAllPitNodesResponse expected, GetAllPitNodesResponse actual) {
        assertNotSame(expected, actual);
        Set<ListPitInfo> expectedPitInfos = new HashSet<>(expected.getPitInfos());
        Set<ListPitInfo> actualPitInfos = new HashSet<>(actual.getPitInfos());
        assertEquals(expectedPitInfos, actualPitInfos);

        List<GetAllPitNodeResponse> expectedResponses = expected.getNodes();
        List<GetAllPitNodeResponse> actualResponses = actual.getNodes();
        assertEquals(expectedResponses.size(), actualResponses.size());
        for (int i = 0; i < expectedResponses.size(); i++) {
            assertEquals(expectedResponses.get(i).getNode(), actualResponses.get(i).getNode());
            Set<ListPitInfo> expectedNodePitInfos = new HashSet<>(expectedResponses.get(i).getPitInfos());
            Set<ListPitInfo> actualNodePitInfos = new HashSet<>(actualResponses.get(i).getPitInfos());
            assertEquals(expectedNodePitInfos, actualNodePitInfos);
        }

        List<FailedNodeException> expectedFailures = expected.failures();
        List<FailedNodeException> actualFailures = actual.failures();
        assertEquals(expectedFailures.size(), actualFailures.size());
        for (int i = 0; i < expectedFailures.size(); i++) {
            assertEquals(expectedFailures.get(i).nodeId(), actualFailures.get(i).nodeId());
            assertEquals(expectedFailures.get(i).getMessage(), actualFailures.get(i).getMessage());
            assertEquals(expectedFailures.get(i).getCause().getClass(), actualFailures.get(i).getCause().getClass());
        }
    }

    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Collections.emptyList());
    }

    public void testSerialization() throws IOException {
        GetAllPitNodesResponse response = createTestItem();
        GetAllPitNodesResponse deserialized = copyWriteable(response, getNamedWriteableRegistry(), GetAllPitNodesResponse::new);
        assertEqualInstances(response, deserialized);
    }

    private GetAllPitNodesResponse createTestItem() {
        int numNodes = randomIntBetween(1, 10);
        int numPits = randomInt(10);
        List<ListPitInfo> candidatePitInfos = new ArrayList<>(numPits);
        for (int i = 0; i < numNodes; i++) {
            candidatePitInfos.add(new ListPitInfo(randomAlphaOfLength(10), randomLong(), randomLong()));
        }

        List<GetAllPitNodeResponse> responses = new ArrayList<>();
        List<FailedNodeException> failures = new ArrayList<>();
        for (int i = 0; i < numNodes; i++) {
            DiscoveryNode node = new DiscoveryNode(
                randomAlphaOfLength(10),
                buildNewFakeTransportAddress(),
                emptyMap(),
                emptySet(),
                Version.CURRENT
            );
            if (randomBoolean()) {
                List<ListPitInfo> nodePitInfos = new ArrayList<>();
                for (int j = 0; j < randomInt(numPits); j++) {
                    nodePitInfos.add(randomFrom(candidatePitInfos));
                }
                responses.add(new GetAllPitNodeResponse(node, nodePitInfos));
            } else {
                failures.add(
                    new FailedNodeException(node.getId(), randomAlphaOfLength(10), new TransportException(randomAlphaOfLength(10)))
                );
            }
        }
        return new GetAllPitNodesResponse(new ClusterName("test"), responses, failures);
    }
}
