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

package org.opensearch.http.nio;

import org.opensearch.NioIntegTestCase;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;

import java.util.Collection;
import java.util.Locale;

import io.netty.handler.codec.http.FullHttpResponse;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@ClusterScope(scope = Scope.TEST, supportsDedicatedMasters = false, numDataNodes = 1)
public class NioPipeliningIT extends NioIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    public void testThatNioHttpServerSupportsPipelining() throws Exception {
        String[] requests = new String[] { "/", "/_nodes/stats", "/", "/_cluster/state", "/" };

        HttpServerTransport httpServerTransport = internalCluster().getInstance(HttpServerTransport.class);
        TransportAddress[] boundAddresses = httpServerTransport.boundAddress().boundAddresses();
        TransportAddress transportAddress = randomFrom(boundAddresses);

        try (NioHttpClient client = NioHttpClient.http()) {
            Collection<FullHttpResponse> responses = client.get(transportAddress.address(), requests);
            assertThat(responses, hasSize(5));

            Collection<String> opaqueIds = NioHttpClient.returnOpaqueIds(responses);
            assertOpaqueIdsInOrder(opaqueIds);
        }
    }

    private void assertOpaqueIdsInOrder(Collection<String> opaqueIds) {
        // check if opaque ids are monotonically increasing
        int i = 0;
        String msg = String.format(Locale.ROOT, "Expected list of opaque ids to be monotonically increasing, got [%s]", opaqueIds);
        for (String opaqueId : opaqueIds) {
            assertThat(msg, opaqueId, is(String.valueOf(i++)));
        }
    }

}
