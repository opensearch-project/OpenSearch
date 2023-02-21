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

package org.opensearch.rest.action;

import org.opensearch.Build;
import org.opensearch.Version;
import org.opensearch.action.main.MainResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class RestMainActionTests extends OpenSearchTestCase {

    public void testHeadResponse() throws Exception {
        final String nodeName = "node1";
        final ClusterName clusterName = new ClusterName("cluster1");
        final String clusterUUID = randomAlphaOfLengthBetween(10, 20);
        final Version version = Version.CURRENT;
        final Build build = Build.CURRENT;

        final MainResponse mainResponse = new MainResponse(nodeName, version, clusterName, clusterUUID, build);
        XContentBuilder builder = JsonXContent.contentBuilder();
        RestRequest restRequest = new FakeRestRequest() {
            @Override
            public Method method() {
                return Method.HEAD;
            }
        };

        BytesRestResponse response = RestMainAction.convertMainResponse(mainResponse, restRequest, builder);
        assertNotNull(response);
        assertThat(response.status(), equalTo(RestStatus.OK));

        // the empty responses are handled in the HTTP layer so we do
        // not assert on them here
    }

    public void testGetResponse() throws Exception {
        final String nodeName = "node1";
        final ClusterName clusterName = new ClusterName("cluster1");
        final String clusterUUID = randomAlphaOfLengthBetween(10, 20);
        final Version version = Version.CURRENT;
        final Build build = Build.CURRENT;
        final boolean prettyPrint = randomBoolean();

        final MainResponse mainResponse = new MainResponse(nodeName, version, clusterName, clusterUUID, build);
        XContentBuilder builder = JsonXContent.contentBuilder();

        Map<String, String> params = new HashMap<>();
        if (prettyPrint == false) {
            params.put("pretty", String.valueOf(prettyPrint));
        }
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();

        BytesRestResponse response = RestMainAction.convertMainResponse(mainResponse, restRequest, builder);
        assertNotNull(response);
        assertThat(response.status(), equalTo(RestStatus.OK));
        assertThat(response.content().length(), greaterThan(0));

        XContentBuilder responseBuilder = JsonXContent.contentBuilder();
        if (prettyPrint) {
            // do this to mimic what the rest layer does
            responseBuilder.prettyPrint().lfAtEnd();
        }
        mainResponse.toXContent(responseBuilder, ToXContent.EMPTY_PARAMS);
        BytesReference xcontentBytes = BytesReference.bytes(responseBuilder);
        assertEquals(xcontentBytes, response.content());
    }
}
