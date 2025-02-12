/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.http.reactor.netty4;

import org.opensearch.OpenSearchReactorNetty4IntegTestCase;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;

import java.util.Collection;
import java.util.Locale;

import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.util.ReferenceCounted;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@ClusterScope(scope = Scope.TEST, supportsDedicatedMasters = false, numDataNodes = 1)
public class ReactorNetty4PipeliningIT extends OpenSearchReactorNetty4IntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    public void testThatNettyHttpServerSupportsPipelining() throws Exception {
        String[] requests = new String[] { "/", "/_nodes/stats", "/", "/_cluster/state", "/" };

        HttpServerTransport httpServerTransport = internalCluster().getInstance(HttpServerTransport.class);
        TransportAddress[] boundAddresses = httpServerTransport.boundAddress().boundAddresses();
        TransportAddress transportAddress = randomFrom(boundAddresses);

        try (ReactorHttpClient client = ReactorHttpClient.create()) {
            Collection<FullHttpResponse> responses = client.get(transportAddress.address(), true, requests);
            try {
                assertThat(responses, hasSize(5));

                Collection<String> opaqueIds = ReactorHttpClient.returnOpaqueIds(responses);
                assertOpaqueIdsInOrder(opaqueIds);
            } finally {
                responses.forEach(ReferenceCounted::release);
            }
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
