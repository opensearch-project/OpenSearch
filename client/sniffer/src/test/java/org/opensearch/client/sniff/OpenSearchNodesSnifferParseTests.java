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

package org.opensearch.client.sniff;

import com.fasterxml.jackson.core.JsonFactory;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.opensearch.client.Node;
import org.opensearch.client.Node.Roles;
import org.opensearch.client.RestClientTestCase;
import org.opensearch.client.sniff.OpenSearchNodesSniffer.Scheme;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Test parsing the response from the {@code /_nodes/http} API from fixed
 * versions of OpenSearch.
 */
public class OpenSearchNodesSnifferParseTests extends RestClientTestCase {

    private void checkFile(String file, Node... expected) throws IOException {
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(file);
        if (in == null) {
            throw new IllegalArgumentException("Couldn't find [" + file + "]");
        }
        try {
            HttpEntity entity = new InputStreamEntity(in, ContentType.APPLICATION_JSON);
            List<Node> nodes = OpenSearchNodesSniffer.readHosts(entity, Scheme.HTTP, new JsonFactory());
            /*
             * Use these assertions because the error messages are nicer
             * than hasItems and we know the results are in order because
             * that is how we generated the file.
             */
            assertThat(nodes, hasSize(expected.length));
            for (int i = 0; i < expected.length; i++) {
                assertEquals(expected[i], nodes.get(i));
            }
        } finally {
            in.close();
        }
    }

    public void test1x() throws IOException {
        checkFile(
            "1.0.0_nodes_http.json",
            node(9200, "m1", "1.0.0", "master", "ingest"),
            node(9201, "m2", "1.0.0", "master", "data", "ingest"),
            node(9202, "m3", "1.0.0", "master", "ingest"),
            node(9203, "d1", "1.0.0", "data", "ingest"),
            node(9204, "d2", "1.0.0", "data", "ingest"),
            node(9205, "d3", "1.0.0", "data", "ingest"),
            node(9206, "c1", "1.0.0", "ingest"),
            node(9207, "c2", "1.0.0", "ingest")
        );
    }

    public void test2x() throws IOException {
        checkFile(
            "2.0.0_nodes_http.json",
            node(9200, "m1", "2.0.0", "cluster_manager", "ingest"),
            node(9201, "m2", "2.0.0", "cluster_manager", "data", "ingest"),
            node(9202, "m3", "2.0.0", "cluster_manager", "ingest"),
            node(9203, "d1", "2.0.0", "data", "ingest"),
            node(9204, "d2", "2.0.0", "data", "ingest"),
            node(9205, "d3", "2.0.0", "data", "ingest"),
            node(9206, "c1", "2.0.0", "ingest"),
            node(9207, "c2", "2.0.0", "ingest")
        );
    }

    public void testParsingPublishAddressWithPreES7Format() throws IOException {
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("es6_nodes_publication_address_format.json");

        HttpEntity entity = new InputStreamEntity(in, ContentType.APPLICATION_JSON);
        List<Node> nodes = OpenSearchNodesSniffer.readHosts(entity, Scheme.HTTP, new JsonFactory());

        assertEquals("127.0.0.1", nodes.get(0).getHost().getHostName());
        assertEquals(9200, nodes.get(0).getHost().getPort());
        assertEquals("http", nodes.get(0).getHost().getSchemeName());
    }

    public void testParsingPublishAddressWithES7Format() throws IOException {
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("es7_nodes_publication_address_format.json");

        HttpEntity entity = new InputStreamEntity(in, ContentType.APPLICATION_JSON);
        List<Node> nodes = OpenSearchNodesSniffer.readHosts(entity, Scheme.HTTP, new JsonFactory());

        assertEquals("opensearch.test", nodes.get(0).getHost().getHostName());
        assertEquals(9200, nodes.get(0).getHost().getPort());
        assertEquals("http", nodes.get(0).getHost().getSchemeName());
    }

    private Node node(int port, String name, String version, String... roles) {
        return node(port, name, version, new TreeSet<>(Arrays.asList(roles)));
    }

    private Node node(int port, String name, String version, Set<String> roles) {
        HttpHost host = new HttpHost("127.0.0.1", port);
        Set<HttpHost> boundHosts = new HashSet<>(2);
        boundHosts.add(new HttpHost("[::1]", port));
        boundHosts.add(host);
        Map<String, List<String>> attributes = new LinkedHashMap<>(); // LinkedHashMap to preserve insertion order
        attributes.put("dummy", singletonList("everyone_has_me"));
        attributes.put("number", singletonList(name.substring(1)));
        if (!version.startsWith("1.0") && !version.startsWith("1.1")) {
            // Shard Indexing Pressure feature is added in version 1.2.0
            attributes.put("shard_indexing_pressure_enabled", singletonList(Boolean.TRUE.toString()));
        }
        attributes.put("array", Arrays.asList(name.substring(0, 1), name.substring(1)));
        return new Node(host, boundHosts, name, version, new Roles(new TreeSet<>(roles)), attributes);
    }

}
