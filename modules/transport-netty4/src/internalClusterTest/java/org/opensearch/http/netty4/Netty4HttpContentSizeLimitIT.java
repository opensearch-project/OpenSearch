/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.netty4;

import org.opensearch.OpenSearchNetty4IntegTestCase;
import org.opensearch.common.settings.Settings;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;

import java.net.Socket;
import java.nio.charset.StandardCharsets;

import static org.opensearch.http.HttpTransportSettings.SETTING_HTTP_MAX_CONTENT_LENGTH;
import static org.opensearch.http.netty4.Netty4HttpRequestSizeTestClient.assertResponse;
import static org.opensearch.http.netty4.Netty4HttpRequestSizeTestClient.bulkBody;
import static org.opensearch.http.netty4.Netty4HttpRequestSizeTestClient.connect;
import static org.opensearch.http.netty4.Netty4HttpRequestSizeTestClient.encode;
import static org.opensearch.http.netty4.Netty4HttpRequestSizeTestClient.request;
import static org.opensearch.http.netty4.Netty4HttpRequestSizeTestClient.send;
import static org.hamcrest.Matchers.containsString;

/**
 * Sends bulk requests to a complete node and verifies response framing, clean EOF and which documents were indexed.
 */
@ClusterScope(scope = Scope.TEST, supportsDedicatedMasters = false, numClientNodes = 0, numDataNodes = 1)
public class Netty4HttpContentSizeLimitIT extends OpenSearchNetty4IntegTestCase {
    private static final int LIMIT = 1024;

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(SETTING_HTTP_MAX_CONTENT_LENGTH.getKey(), LIMIT + "b").build();
    }

    public void testBulkRequestsRespectDecompressedLimitAndReturn413BeforeEof() throws Exception {
        createIndex("oversized");
        ensureGreen();
        HttpServerTransport transport = internalCluster().getInstance(HttpServerTransport.class);
        int accepted = 0;
        for (int size : new int[] { LIMIT, LIMIT + 1, 32 * LIMIT }) {
            for (String encoding : new String[] { "identity", "gzip", "deflate" }) {
                for (boolean chunked : new boolean[] { false, true }) {
                    for (int fragmentSize : new int[] { 1, 17, 1024, Integer.MAX_VALUE }) {
                        String scenario = "size=" + size + ", encoding=" + encoding + ", chunked=" + chunked + ", fragment=" + fragmentSize;
                        try (Socket socket = connect(transport)) {
                            send(
                                socket.getOutputStream(),
                                request(encode(bulkBody(size), encoding), encoding, chunked, true),
                                fragmentSize
                            );
                            byte[] response = assertResponse(socket.getInputStream(), size <= LIMIT ? 200 : 413, scenario);
                            assertEquals("Complete response must precede FIN: " + scenario, -1, socket.getInputStream().read());
                            if (size <= LIMIT) {
                                assertThat(new String(response, StandardCharsets.UTF_8), containsString("\"errors\":false"));
                                accepted++;
                            }
                        }
                    }
                }
            }
        }
        refresh("oversized");
        assertEquals(
            "Only accepted bulk requests may index documents",
            accepted,
            client().prepareSearch("oversized").setSize(0).get().getHits().getTotalHits().value()
        );
    }
}
