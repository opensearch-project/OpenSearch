/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.transport.netty4;

import org.opensearch.OpenSearchNetty4IntegTestCase;
import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.common.network.NetworkAddress;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.BoundTransportAddress;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;
import org.opensearch.test.junit.annotations.Network;
import org.opensearch.transport.TransportInfo;

import java.util.Locale;

import static org.opensearch.action.admin.cluster.node.info.NodesInfoRequest.Metric.TRANSPORT;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@ClusterScope(scope = Scope.SUITE, supportsDedicatedMasters = false, numDataNodes = 1, numClientNodes = 0)
public class Netty4TransportMultiPortIntegrationIT extends OpenSearchNetty4IntegTestCase {

    private static int randomPort = -1;
    private static String randomPortRange;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        if (randomPort == -1) {
            randomPort = randomIntBetween(49152, 65525);
            randomPortRange = String.format(Locale.ROOT, "%s-%s", randomPort, randomPort + 10);
        }
        Settings.Builder builder = Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("network.host", "127.0.0.1")
            .put("transport.profiles.client1.port", randomPortRange)
            .put("transport.profiles.client1.publish_host", "127.0.0.7")
            .put("transport.profiles.client1.publish_port", "4321")
            .put("transport.profiles.client1.reuse_address", true);
        return builder.build();
    }

    @Network
    public void testThatInfosAreExposed() throws Exception {
        NodesInfoResponse response = client().admin().cluster().prepareNodesInfo().clear().addMetric(TRANSPORT.metricName()).get();
        for (NodeInfo nodeInfo : response.getNodes()) {
            assertThat(nodeInfo.getInfo(TransportInfo.class).getProfileAddresses().keySet(), hasSize(1));
            assertThat(nodeInfo.getInfo(TransportInfo.class).getProfileAddresses(), hasKey("client1"));
            BoundTransportAddress boundTransportAddress = nodeInfo.getInfo(TransportInfo.class).getProfileAddresses().get("client1");
            for (TransportAddress transportAddress : boundTransportAddress.boundAddresses()) {
                assertThat(transportAddress, instanceOf(TransportAddress.class));
            }

            // bound addresses
            for (TransportAddress transportAddress : boundTransportAddress.boundAddresses()) {
                assertThat(transportAddress, instanceOf(TransportAddress.class));
                assertThat(
                    transportAddress.address().getPort(),
                    is(allOf(greaterThanOrEqualTo(randomPort), lessThanOrEqualTo(randomPort + 10)))
                );
            }

            // publish address
            assertThat(boundTransportAddress.publishAddress(), instanceOf(TransportAddress.class));
            TransportAddress publishAddress = boundTransportAddress.publishAddress();
            assertThat(NetworkAddress.format(publishAddress.address().getAddress()), is("127.0.0.7"));
            assertThat(publishAddress.address().getPort(), is(4321));
        }
    }
}
