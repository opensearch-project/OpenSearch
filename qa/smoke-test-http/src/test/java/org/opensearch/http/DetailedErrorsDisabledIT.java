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

package org.opensearch.http;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import java.io.IOException;

import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;

/**
 * Tests that when disabling detailed errors, a request with the error_trace parameter returns an HTTP 400 response.
 */
@ClusterScope(scope = Scope.TEST, supportsDedicatedMasters = false, numDataNodes = 1)
public class DetailedErrorsDisabledIT extends HttpSmokeTestCase {

    // Build our cluster settings
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                       .put(super.nodeSettings(nodeOrdinal))
                       .put(HttpTransportSettings.SETTING_HTTP_DETAILED_ERRORS_ENABLED.getKey(), false)
                       .build();
    }

    public void testThatErrorTraceParamReturns400() throws IOException, ParseException {
        Request request = new Request("DELETE", "/");
        request.addParameter("error_trace", "true");
        ResponseException e = expectThrows(ResponseException.class, () ->
            getRestClient().performRequest(request));

        Response response = e.getResponse();
        assertThat(response.getHeader("Content-Type"), is("application/json; charset=UTF-8"));
        assertThat(EntityUtils.toString(e.getResponse().getEntity()),
            containsString("\"error\":\"error traces in responses are disabled.\""));
        assertThat(response.getStatusLine().getStatusCode(), is(400));
    }

    public void testDetailedStackTracesAreNotIncludedWhenErrorTraceIsDisabledForBulkApis() throws IOException, ParseException {
        MediaType contentType = MediaTypeRegistry.JSON;
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest().add(
            new SearchRequest("missing_index")
                .source(new SearchSourceBuilder().query(new QueryStringQueryBuilder("foo").field("*"))));
        Request request = new Request("POST", "/_msearch");
        byte[] requestBody = MultiSearchRequest.writeMultiLineFormat(multiSearchRequest, contentType.xContent());
        request.setEntity(new ByteArrayEntity(requestBody, ContentType.APPLICATION_JSON));

        Response response = getRestClient().performRequest(request);

        assertThat(EntityUtils.toString(response.getEntity()), not(containsString("stack_trace")));
    }

}
