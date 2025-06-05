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

package org.opensearch.rest.action.admin.indices;

import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;

public class RestResumeIngestionActionTests extends OpenSearchTestCase {

    private RestResumeIngestionAction action;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        action = new RestResumeIngestionAction();
    }

    public void testPrepareRequest() throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("index", "test-index");
        params.put("cluster_manager_timeout", "30s");
        params.put("timeout", "60s");

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/test-index/ingestion/_resume")
            .withMethod(RestRequest.Method.POST)
            .withParams(params)
            .build();

        NodeClient client = mock(NodeClient.class);
        assertNotNull(action.prepareRequest(request, client));
    }

    public void testRoutes() {
        assertEquals(1, action.routes().size());
        assertEquals("/" + "{index}" + "/ingestion/_resume", action.routes().get(0).getPath());
        assertEquals(RestRequest.Method.POST, action.routes().get(0).getMethod());
    }

    public void testGetName() {
        assertEquals("resume_ingestion_action", action.getName());
    }

}
