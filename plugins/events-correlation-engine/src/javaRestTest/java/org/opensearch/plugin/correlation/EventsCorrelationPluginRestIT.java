/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation;

import org.junit.Assert;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Rest Action tests for events-correlation-plugin
 */
public class EventsCorrelationPluginRestIT extends OpenSearchRestTestCase {

    /**
     * test events-correlation-plugin is installed
     * @throws IOException IOException
     */
    @SuppressWarnings("unchecked")
    public void testPluginsAreInstalled() throws IOException {
        Request request = new Request("GET", "/_cat/plugins?s=component&h=name,component,version,description&format=json");
        Response response = client().performRequest(request);
        List<Object> pluginsList = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            response.getEntity().getContent()
        ).list();
        Assert.assertTrue(
            pluginsList.stream()
                .map(o -> (Map<String, Object>) o)
                .anyMatch(plugin -> plugin.get("component").equals("events-correlation-engine"))
        );
    }

    /**
     * test creating a correlation rule
     * @throws IOException IOException
     */
    public void testCreatingACorrelationRule() throws IOException {
        Request request = new Request("POST", "/_correlation/rules");
        request.setJsonEntity(sampleCorrelationRule());
        Response response = client().performRequest(request);

        Assert.assertEquals(201, response.getStatusLine().getStatusCode());

        Map<String, Object> responseMap = entityAsMap(response);
        String id = responseMap.get("_id").toString();

        request = new Request("POST", "/.opensearch-correlation-rules-config/_search");
        request.setJsonEntity(matchIdQuery(id));
        response = client().performRequest(request);

        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        SearchResponse searchResponse = SearchResponse.fromXContent(
            createParser(JsonXContent.jsonXContent, response.getEntity().getContent())
        );
        Assert.assertEquals(1L, searchResponse.getHits().getTotalHits().value);
    }

    /**
     * test creating a correlation rule with no timestamp field
     * @throws IOException IOException
     */
    @SuppressWarnings("unchecked")
    public void testCreatingACorrelationRuleWithNoTimestampField() throws IOException {
        Request request = new Request("POST", "/_correlation/rules");
        request.setJsonEntity(sampleCorrelationRuleWithNoTimestamp());
        Response response = client().performRequest(request);

        Assert.assertEquals(201, response.getStatusLine().getStatusCode());

        Map<String, Object> responseMap = entityAsMap(response);
        String id = responseMap.get("_id").toString();

        request = new Request("POST", "/.opensearch-correlation-rules-config/_search");
        request.setJsonEntity(matchIdQuery(id));
        response = client().performRequest(request);

        Assert.assertEquals(200, response.getStatusLine().getStatusCode());
        SearchResponse searchResponse = SearchResponse.fromXContent(
            createParser(JsonXContent.jsonXContent, response.getEntity().getContent())
        );
        Assert.assertEquals(1L, searchResponse.getHits().getTotalHits().value);
        Assert.assertEquals(
            "_timestamp",
            ((List<Map<String, Object>>) (searchResponse.getHits().getHits()[0].getSourceAsMap().get("correlate"))).get(0)
                .get("timestampField")
        );
    }

    private String sampleCorrelationRule() {
        return "{\n"
            + "  \"name\": \"s3 to app logs\",\n"
            + "  \"correlate\": [\n"
            + "    {\n"
            + "      \"index\": \"s3_access_logs\",\n"
            + "      \"query\": \"aws.cloudtrail.eventName:ReplicateObject\",\n"
            + "      \"timestampField\": \"@timestamp\",\n"
            + "      \"tags\": [\n"
            + "        \"s3\"\n"
            + "      ]\n"
            + "    },\n"
            + "    {\n"
            + "      \"index\": \"app_logs\",\n"
            + "      \"query\": \"keywords:PermissionDenied\",\n"
            + "      \"timestampField\": \"@timestamp\",\n"
            + "      \"tags\": [\n"
            + "        \"others_application\"\n"
            + "      ]\n"
            + "    }\n"
            + "  ]\n"
            + "}";
    }

    private String sampleCorrelationRuleWithNoTimestamp() {
        return "{\n"
            + "  \"name\": \"s3 to app logs\",\n"
            + "  \"correlate\": [\n"
            + "    {\n"
            + "      \"index\": \"s3_access_logs\",\n"
            + "      \"query\": \"aws.cloudtrail.eventName:ReplicateObject\",\n"
            + "      \"tags\": [\n"
            + "        \"s3\"\n"
            + "      ]\n"
            + "    },\n"
            + "    {\n"
            + "      \"index\": \"app_logs\",\n"
            + "      \"query\": \"keywords:PermissionDenied\",\n"
            + "      \"tags\": [\n"
            + "        \"others_application\"\n"
            + "      ]\n"
            + "    }\n"
            + "  ]\n"
            + "}";
    }

    private String matchIdQuery(String id) {
        return "{\n" + "   \"query\" : {\n" + "     \"match\":{\n" + "        \"_id\": \"" + id + "\"\n" + "     }\n" + "   }\n" + "}";
    }
}
