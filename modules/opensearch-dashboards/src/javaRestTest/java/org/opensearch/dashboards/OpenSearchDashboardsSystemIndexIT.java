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

package org.opensearch.dashboards;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.http.util.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class OpenSearchDashboardsSystemIndexIT extends OpenSearchRestTestCase {

    private final String indexName;

    public OpenSearchDashboardsSystemIndexIT(@Name("indexName") String indexName) {
        this.indexName = indexName;
    }

    @ParametersFactory
    public static Iterable<Object[]> data() {
        return Arrays.asList(
            new Object[] { ".opensearch_dashboards" },
            new Object[] { ".opensearch_dashboards_1" },
            new Object[] { ".reporting-1" },
            new Object[] { ".apm-agent-configuration" },
            new Object[] { ".apm-custom-link" }
        );
    }

    public void testCreateIndex() throws IOException {
        Request request = new Request("PUT", "/_opensearch_dashboards/" + indexName);
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
    }

    public void testAliases() throws IOException {
        assumeFalse("In this test, .opensearch_dashboards is the alias name", ".opensearch_dashboards".equals(indexName));
        Request request = new Request("PUT", "/_opensearch_dashboards/" + indexName);
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        request = new Request("PUT", "/_opensearch_dashboards/" + indexName + "/_alias/.opensearch_dashboards");
        response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        request = new Request("GET", "/_opensearch_dashboards/_aliases");
        response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
        assertThat(EntityUtils.toString(response.getEntity()), containsString(".opensearch_dashboards"));
    }

    public void testBulkToOpenSearchDashboardsIndex() throws IOException {
        Request request = new Request("POST", "/_opensearch_dashboards/_bulk");
        request.setJsonEntity("{ \"index\" : { \"_index\" : \"" + indexName + "\", \"_id\" : \"1\" } }\n{ \"foo\" : \"bar\" }\n");
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
    }

    public void testRefresh() throws IOException {
        Request request = new Request("POST", "/_opensearch_dashboards/_bulk");
        request.setJsonEntity("{ \"index\" : { \"_index\" : \"" + indexName + "\", \"_id\" : \"1\" } }\n{ \"foo\" : \"bar\" }\n");
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        request = new Request("GET", "/_opensearch_dashboards/" + indexName + "/_refresh");
        response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        Request getRequest = new Request("GET", "/_opensearch_dashboards/" + indexName + "/_doc/1");
        Response getResponse = client().performRequest(getRequest);
        assertThat(getResponse.getStatusLine().getStatusCode(), is(200));
        String responseBody = EntityUtils.toString(getResponse.getEntity());
        assertThat(responseBody, containsString("foo"));
        assertThat(responseBody, containsString("bar"));
    }

    public void testGetFromOpenSearchDashboardsIndex() throws IOException {
        Request request = new Request("POST", "/_opensearch_dashboards/_bulk");
        request.setJsonEntity("{ \"index\" : { \"_index\" : \"" + indexName + "\", \"_id\" : \"1\" } }\n{ \"foo\" : \"bar\" }\n");
        request.addParameter("refresh", "true");

        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        Request getRequest = new Request("GET", "/_opensearch_dashboards/" + indexName + "/_doc/1");
        Response getResponse = client().performRequest(getRequest);
        assertThat(getResponse.getStatusLine().getStatusCode(), is(200));
        String responseBody = EntityUtils.toString(getResponse.getEntity());
        assertThat(responseBody, containsString("foo"));
        assertThat(responseBody, containsString("bar"));
    }

    public void testMultiGetFromOpenSearchDashboardsIndex() throws IOException {
        Request request = new Request("POST", "/_opensearch_dashboards/_bulk");
        request.setJsonEntity(
            "{ \"index\" : { \"_index\" : \""
                + indexName
                + "\", \"_id\" : \"1\" } }\n{ \"foo\" : \"bar\" }\n"
                + "{ \"index\" : { \"_index\" : \""
                + indexName
                + "\", \"_id\" : \"2\" } }\n{ \"baz\" : \"tag\" }\n"
        );
        request.addParameter("refresh", "true");

        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        Request getRequest = new Request("GET", "/_opensearch_dashboards/_mget");
        getRequest.setJsonEntity(
            "{ \"docs\" : [ { \"_index\" : \""
                + indexName
                + "\", \"_id\" : \"1\" }, "
                + "{ \"_index\" : \""
                + indexName
                + "\", \"_id\" : \"2\" } ] }\n"
        );
        Response getResponse = client().performRequest(getRequest);
        assertThat(getResponse.getStatusLine().getStatusCode(), is(200));
        String responseBody = EntityUtils.toString(getResponse.getEntity());
        assertThat(responseBody, containsString("foo"));
        assertThat(responseBody, containsString("bar"));
        assertThat(responseBody, containsString("baz"));
        assertThat(responseBody, containsString("tag"));
    }

    public void testSearchFromOpenSearchDashboardsIndex() throws IOException {
        Request request = new Request("POST", "/_opensearch_dashboards/_bulk");
        request.setJsonEntity(
            "{ \"index\" : { \"_index\" : \""
                + indexName
                + "\", \"_id\" : \"1\" } }\n{ \"foo\" : \"bar\" }\n"
                + "{ \"index\" : { \"_index\" : \""
                + indexName
                + "\", \"_id\" : \"2\" } }\n{ \"baz\" : \"tag\" }\n"
        );
        request.addParameter("refresh", "true");

        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        Request searchRequest = new Request("GET", "/_opensearch_dashboards/" + indexName + "/_search");
        searchRequest.setJsonEntity("{ \"query\" : { \"match_all\" : {} } }\n");
        Response getResponse = client().performRequest(searchRequest);
        assertThat(getResponse.getStatusLine().getStatusCode(), is(200));
        String responseBody = EntityUtils.toString(getResponse.getEntity());
        assertThat(responseBody, containsString("foo"));
        assertThat(responseBody, containsString("bar"));
        assertThat(responseBody, containsString("baz"));
        assertThat(responseBody, containsString("tag"));
    }

    public void testDeleteFromOpenSearchDashboardsIndex() throws IOException {
        Request request = new Request("POST", "/_opensearch_dashboards/_bulk");
        request.setJsonEntity(
            "{ \"index\" : { \"_index\" : \""
                + indexName
                + "\", \"_id\" : \"1\" } }\n{ \"foo\" : \"bar\" }\n"
                + "{ \"index\" : { \"_index\" : \""
                + indexName
                + "\", \"_id\" : \"2\" } }\n{ \"baz\" : \"tag\" }\n"
        );
        request.addParameter("refresh", "true");

        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        Request deleteRequest = new Request("DELETE", "/_opensearch_dashboards/" + indexName + "/_doc/1");
        Response deleteResponse = client().performRequest(deleteRequest);
        assertThat(deleteResponse.getStatusLine().getStatusCode(), is(200));
    }

    public void testDeleteByQueryFromOpenSearchDashboardsIndex() throws IOException {
        Request request = new Request("POST", "/_opensearch_dashboards/_bulk");
        request.setJsonEntity(
            "{ \"index\" : { \"_index\" : \""
                + indexName
                + "\", \"_id\" : \"1\" } }\n{ \"foo\" : \"bar\" }\n"
                + "{ \"index\" : { \"_index\" : \""
                + indexName
                + "\", \"_id\" : \"2\" } }\n{ \"baz\" : \"tag\" }\n"
        );
        request.addParameter("refresh", "true");

        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        Request dbqRequest = new Request("POST", "/_opensearch_dashboards/" + indexName + "/_delete_by_query");
        dbqRequest.setJsonEntity("{ \"query\" : { \"match_all\" : {} } }\n");
        Response dbqResponse = client().performRequest(dbqRequest);
        assertThat(dbqResponse.getStatusLine().getStatusCode(), is(200));
    }

    public void testUpdateIndexSettings() throws IOException {
        Request request = new Request("PUT", "/_opensearch_dashboards/" + indexName);
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        request = new Request("PUT", "/_opensearch_dashboards/" + indexName + "/_settings");
        request.setJsonEntity("{ \"index.blocks.read_only\" : false }");
        response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
    }

    public void testGetIndex() throws IOException {
        Request request = new Request("PUT", "/_opensearch_dashboards/" + indexName);
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        request = new Request("GET", "/_opensearch_dashboards/" + indexName);
        response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
        assertThat(EntityUtils.toString(response.getEntity()), containsString(indexName));
    }

    public void testIndexingAndUpdatingDocs() throws IOException {
        Request request = new Request("PUT", "/_opensearch_dashboards/" + indexName + "/_doc/1");
        request.setJsonEntity("{ \"foo\" : \"bar\" }");
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(201));

        request = new Request("PUT", "/_opensearch_dashboards/" + indexName + "/_create/2");
        request.setJsonEntity("{ \"foo\" : \"bar\" }");
        response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(201));

        request = new Request("POST", "/_opensearch_dashboards/" + indexName + "/_doc");
        request.setJsonEntity("{ \"foo\" : \"bar\" }");
        response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(201));

        request = new Request("GET", "/_opensearch_dashboards/" + indexName + "/_refresh");
        response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        request = new Request("POST", "/_opensearch_dashboards/" + indexName + "/_update/1");
        request.setJsonEntity("{ \"doc\" : { \"foo\" : \"baz\" } }");
        response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
    }

    public void testScrollingDocs() throws IOException {
        Request request = new Request("POST", "/_opensearch_dashboards/_bulk");
        request.setJsonEntity(
            "{ \"index\" : { \"_index\" : \""
                + indexName
                + "\", \"_id\" : \"1\" } }\n{ \"foo\" : \"bar\" }\n"
                + "{ \"index\" : { \"_index\" : \""
                + indexName
                + "\", \"_id\" : \"2\" } }\n{ \"baz\" : \"tag\" }\n"
                + "{ \"index\" : { \"_index\" : \""
                + indexName
                + "\", \"_id\" : \"3\" } }\n{ \"baz\" : \"tag\" }\n"
        );
        request.addParameter("refresh", "true");
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        Request searchRequest = new Request("GET", "/_opensearch_dashboards/" + indexName + "/_search");
        searchRequest.setJsonEntity("{ \"size\" : 1,\n\"query\" : { \"match_all\" : {} } }\n");
        searchRequest.addParameter("scroll", "1m");
        response = client().performRequest(searchRequest);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
        Map<String, Object> map = XContentHelper.convertToMap(JsonXContent.jsonXContent, EntityUtils.toString(response.getEntity()), false);
        assertNotNull(map.get("_scroll_id"));
        String scrollId = (String) map.get("_scroll_id");

        Request scrollRequest = new Request("POST", "/_opensearch_dashboards/_search/scroll");
        scrollRequest.addParameter("scroll_id", scrollId);
        scrollRequest.addParameter("scroll", "1m");
        response = client().performRequest(scrollRequest);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
        map = XContentHelper.convertToMap(JsonXContent.jsonXContent, EntityUtils.toString(response.getEntity()), false);
        assertNotNull(map.get("_scroll_id"));
        scrollId = (String) map.get("_scroll_id");

        Request clearScrollRequest = new Request("DELETE", "/_opensearch_dashboards/_search/scroll");
        clearScrollRequest.addParameter("scroll_id", scrollId);
        response = client().performRequest(clearScrollRequest);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
    }
}
