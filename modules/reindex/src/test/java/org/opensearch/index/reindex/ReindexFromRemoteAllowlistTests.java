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

package org.opensearch.index.reindex;

import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.test.OpenSearchTestCase;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.opensearch.index.reindex.ReindexValidator.buildRemoteAllowlist;
import static org.opensearch.index.reindex.ReindexValidator.checkRemoteAllowlist;

/**
 * Tests the reindex-from-remote allowlist of remotes.
 */
public class ReindexFromRemoteAllowlistTests extends OpenSearchTestCase {

    private final BytesReference query = new BytesArray("{ \"foo\" : \"bar\" }");

    public void testLocalRequestWithoutAllowlist() {
        checkRemoteAllowlist(buildRemoteAllowlist(emptyList()), null);
    }

    public void testLocalRequestWithAllowlist() {
        checkRemoteAllowlist(buildRemoteAllowlist(randomAllowlist()), null);
    }

    /**
     * Build a {@link RemoteInfo}, defaulting values that we don't care about in this test to values that don't hurt anything.
     */
    private RemoteInfo newRemoteInfo(String host, int port) {
        return new RemoteInfo(
            randomAlphaOfLength(5),
            host,
            port,
            null,
            query,
            null,
            null,
            emptyMap(),
            RemoteInfo.DEFAULT_SOCKET_TIMEOUT,
            RemoteInfo.DEFAULT_CONNECT_TIMEOUT
        );
    }

    public void testAllowlistedRemote() {
        List<String> allowlist = randomAllowlist();
        String[] inList = allowlist.iterator().next().split(":");
        String host = inList[0];
        int port = Integer.valueOf(inList[1]);
        checkRemoteAllowlist(buildRemoteAllowlist(allowlist), newRemoteInfo(host, port));
    }

    public void testAllowlistedByPrefix() {
        checkRemoteAllowlist(
            buildRemoteAllowlist(singletonList("*.example.com:9200")),
            new RemoteInfo(
                randomAlphaOfLength(5),
                "es.example.com",
                9200,
                null,
                query,
                null,
                null,
                emptyMap(),
                RemoteInfo.DEFAULT_SOCKET_TIMEOUT,
                RemoteInfo.DEFAULT_CONNECT_TIMEOUT
            )
        );
        checkRemoteAllowlist(
            buildRemoteAllowlist(singletonList("*.example.com:9200")),
            newRemoteInfo("6e134134a1.us-east-1.aws.example.com", 9200)
        );
    }

    public void testAllowlistedBySuffix() {
        checkRemoteAllowlist(buildRemoteAllowlist(singletonList("es.example.com:*")), newRemoteInfo("es.example.com", 9200));
    }

    public void testAllowlistedByInfix() {
        checkRemoteAllowlist(buildRemoteAllowlist(singletonList("es*.example.com:9200")), newRemoteInfo("es1.example.com", 9200));
    }

    public void testLoopbackInAllowlistRemote() throws UnknownHostException {
        List<String> allowlist = randomAllowlist();
        allowlist.add("127.0.0.1:*");
        checkRemoteAllowlist(buildRemoteAllowlist(allowlist), newRemoteInfo("127.0.0.1", 9200));
    }

    public void testUnallowlistedRemote() {
        int port = between(1, Integer.MAX_VALUE);
        List<String> allowlist = randomBoolean() ? randomAllowlist() : emptyList();
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> checkRemoteAllowlist(buildRemoteAllowlist(allowlist), newRemoteInfo("not in list", port))
        );
        assertEquals("[not in list:" + port + "] not allowlisted in reindex.remote.allowlist", e.getMessage());
    }

    public void testRejectMatchAll() {
        assertMatchesTooMuch(singletonList("*"));
        assertMatchesTooMuch(singletonList("**"));
        assertMatchesTooMuch(singletonList("***"));
        assertMatchesTooMuch(Arrays.asList("realstuff", "*"));
        assertMatchesTooMuch(Arrays.asList("*", "realstuff"));
        List<String> random = randomAllowlist();
        random.add("*");
        assertMatchesTooMuch(random);
    }

    public void testIPv6Address() {
        List<String> allowlist = randomAllowlist();
        allowlist.add("[::1]:*");
        checkRemoteAllowlist(buildRemoteAllowlist(allowlist), newRemoteInfo("[::1]", 9200));
    }

    private void assertMatchesTooMuch(List<String> allowlist) {
        Exception e = expectThrows(IllegalArgumentException.class, () -> buildRemoteAllowlist(allowlist));
        assertEquals(
            "Refusing to start because allowlist "
                + allowlist
                + " accepts all addresses. "
                + "This would allow users to reindex-from-remote any URL they like effectively having OpenSearch make HTTP GETs "
                + "for them.",
            e.getMessage()
        );
    }

    private List<String> randomAllowlist() {
        int size = between(1, 100);
        List<String> allowlist = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            allowlist.add(randomAlphaOfLength(5) + ':' + between(1, Integer.MAX_VALUE));
        }
        return allowlist;
    }
}
