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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.client;

import org.apache.http.client.methods.HttpGet;
import org.opensearch.action.main.TransportMainAction;
import org.opensearch.client.core.MainResponse;

import java.io.IOException;
import java.util.Map;

public class PingAndInfoIT extends OpenSearchRestHighLevelClientTestCase {

    public void testPing() throws IOException {
        assertTrue(highLevelClient().ping(RequestOptions.DEFAULT));
    }

    public void testInfo() throws IOException {
        MainResponse info = highLevelClient().info(RequestOptions.DEFAULT);
        // compare with what the low level client outputs
        Map<String, Object> infoAsMap = entityAsMap(adminClient().performRequest(new Request(HttpGet.METHOD_NAME, "/")));
        assertEquals(infoAsMap.get("cluster_name"), info.getClusterName());
        assertEquals(infoAsMap.get("cluster_uuid"), info.getClusterUuid());

        // only check node name existence, might be a different one from what was hit by low level client in multi-node cluster
        assertNotNull(info.getNodeName());
        @SuppressWarnings("unchecked")
        Map<String, Object> versionMap = (Map<String, Object>) infoAsMap.get("version");
        assertEquals(versionMap.get("build_type"), info.getVersion().getBuildType());
        assertEquals(versionMap.get("build_hash"), info.getVersion().getBuildHash());
        assertEquals(versionMap.get("build_date"), info.getVersion().getBuildDate());
        assertEquals(versionMap.get("build_snapshot"), info.getVersion().isSnapshot());
        assertTrue(versionMap.get("number").toString().startsWith(info.getVersion().getNumber()));
        assertEquals(versionMap.get("lucene_version"), info.getVersion().getLuceneVersion());
    }

    public void testInfo_overrideResponseVersion() throws IOException {
        Request overrideResponseVersionRequest = new Request("PUT", "/_cluster/settings");
        overrideResponseVersionRequest.setOptions(expectWarnings(TransportMainAction.OVERRIDE_MAIN_RESPONSE_VERSION_DEPRECATION_MESSAGE));
        overrideResponseVersionRequest.setJsonEntity("{\"persistent\":{\"compatibility\": {\"override_main_response_version\":true}}}");
        client().performRequest(overrideResponseVersionRequest);

        MainResponse info = highLevelClient().info(RequestOptions.DEFAULT);
        assertEquals("7.10.2", info.getVersion().getNumber());

        // Set back to default version.
        Request resetResponseVersionRequest = new Request("PUT", "/_cluster/settings");
        resetResponseVersionRequest.setJsonEntity("{\"persistent\":{\"compatibility\": {\"override_main_response_version\":null}}}");
        client().performRequest(resetResponseVersionRequest);

        Map<String, Object> infoAsMap = entityAsMap(adminClient().performRequest(new Request(HttpGet.METHOD_NAME, "/")));
        @SuppressWarnings("unchecked")
        Map<String, Object> versionMap = (Map<String, Object>) infoAsMap.get("version");
        info = highLevelClient().info(RequestOptions.DEFAULT);
        assertTrue(versionMap.get("number").toString().startsWith(info.getVersion().getNumber()));
    }
}
