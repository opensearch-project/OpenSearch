///*
// * SPDX-License-Identifier: Apache-2.0
// *
// * The OpenSearch Contributors require contributions made to
// * this file be licensed under the Apache-2.0 license or a
// * compatible open source license.
// */
//
//package org.opensearch.search.pit;
//
//import org.apache.lucene.util.SetOnce;
//import org.opensearch.Version;
//import org.opensearch.cluster.ClusterName;
//import org.opensearch.cluster.ClusterState;
//import org.opensearch.cluster.node.DiscoveryNode;
//import org.opensearch.cluster.node.DiscoveryNodes;
//import org.opensearch.rest.RestRequest;
//import org.opensearch.rest.action.search.RestGetAllPitsAction;
//import org.opensearch.test.OpenSearchTestCase;
//import org.opensearch.test.rest.FakeRestRequest;
//
//import java.util.HashMap;
//import java.util.Map;
//
//import static java.util.Collections.emptyMap;
//import static java.util.Collections.emptySet;
//import static org.hamcrest.Matchers.*;
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.when;
//
//public class RestGetAllPitsActionTests extends OpenSearchTestCase {
//
//    public void testGetAllPits() throws Exception {
//        ClusterName clusterName = new ClusterName("cluster-1");
//        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
//
//        builder.add(new DiscoveryNode("node-1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT));
//        DiscoveryNodes discoveryNodes = builder.build();
//        ClusterState clusterState = mock(ClusterState.class);
//        when(clusterState.nodes()).thenReturn(discoveryNodes);
//
//        SetOnce<Boolean> getAllPitsCalled = new SetOnce<>();
//        RestGetAllPitsAction action = new RestGetAllPitsAction();
//        Map<String, String> params = new HashMap<>();
//        RestRequest restRequest = buildRestRequest(params);
//        // NodesInfoRequest actual = action.handleRequest(restRequest);
//
//    }
//
//    private FakeRestRequest buildRestRequest(Map<String, String> params) {
//        return new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
//            .withPath("/_search/point_in_time/all")
//            .withParams(params)
//            .build();
//    }
//
//}
