/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.test.rest.OpenSearchRestTestCase;
import org.junit.Assert;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
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

    @SuppressWarnings("unchecked")
    public void testCorrelationWithSingleRule() throws IOException {
        String windowsIndex = "windows";
        Request request = new Request("PUT", "/" + windowsIndex);
        request.setJsonEntity(windowsMappings());
        client().performRequest(request);

        String appLogsIndex = "app_logs";
        request = new Request("PUT", "/" + appLogsIndex);
        request.setJsonEntity(appLogMappings());
        client().performRequest(request);

        String correlationRule = windowsToAppLogsCorrelationRule();
        request = new Request("POST", "/_correlation/rules");
        request.setJsonEntity(correlationRule);
        client().performRequest(request);

        request = new Request("POST", String.format(Locale.ROOT, "/%s/_doc?refresh", windowsIndex));
        request.setJsonEntity(sampleWindowsEvent());
        client().performRequest(request);

        request = new Request("POST", String.format(Locale.ROOT, "/%s/_doc?refresh", appLogsIndex));
        request.setJsonEntity(sampleAppLogsEvent());
        Response response = client().performRequest(request);
        String appLogsId = responseAsMap(response).get("_id").toString();

        request = new Request("POST", "/_correlation/events");
        request.setJsonEntity(prepareCorrelateEventRequest(appLogsIndex, appLogsId));
        response = client().performRequest(request);
        Map<String, Object> responseAsMap = responseAsMap(response);
        Assert.assertEquals(1, ((Map<String, Object>) responseAsMap.get("neighbor_events")).size());
    }

    @SuppressWarnings("unchecked")
    public void testSearchCorrelationsWithSingleRule() throws IOException {
        String windowsIndex = "windows";
        Request request = new Request("PUT", "/" + windowsIndex);
        request.setJsonEntity(windowsMappings());
        client().performRequest(request);

        String appLogsIndex = "app_logs";
        request = new Request("PUT", "/" + appLogsIndex);
        request.setJsonEntity(appLogMappings());
        client().performRequest(request);

        String correlationRule = windowsToAppLogsCorrelationRule();
        request = new Request("POST", "/_correlation/rules");
        request.setJsonEntity(correlationRule);
        client().performRequest(request);

        request = new Request("POST", String.format(Locale.ROOT, "/%s/_doc?refresh", windowsIndex));
        request.setJsonEntity(sampleWindowsEvent());
        Response response = client().performRequest(request);
        String windowsId = responseAsMap(response).get("_id").toString();

        request = new Request("POST", String.format(Locale.ROOT, "/%s/_doc?refresh", appLogsIndex));
        request.setJsonEntity(sampleAppLogsEvent());
        response = client().performRequest(request);
        String appLogsId = responseAsMap(response).get("_id").toString();

        request = new Request("POST", "/_correlation/events");
        request.setJsonEntity(prepareCorrelateEventRequest(windowsIndex, windowsId, true));
        client().performRequest(request);

        request = new Request("POST", "/_correlation/events");
        request.setJsonEntity(prepareCorrelateEventRequest(appLogsIndex, appLogsId, true));
        response = client().performRequest(request);
        Map<String, Object> responseAsMap = responseAsMap(response);
        Assert.assertEquals(1, ((Map<String, Object>) responseAsMap.get("neighbor_events")).size());

        request = new Request("GET", prepareSearchCorrelationsRequest(windowsIndex, windowsId, "winlog.timestamp", 300000L, 5));
        response = client().performRequest(request);
        responseAsMap = responseAsMap(response);
        Assert.assertEquals(1, ((List<Object>) responseAsMap.get("events")).size());
    }

    private String prepareCorrelateEventRequest(String index, String event) {
        return "{\n" + "  \"index\": \"" + index + "\",\n" + "  \"event\": \"" + event + "\",\n" + "  \"store\": false\n" + "}";
    }

    private String prepareCorrelateEventRequest(String index, String event, Boolean store) {
        return "{\n" + "  \"index\": \"" + index + "\",\n" + "  \"event\": \"" + event + "\",\n" + "  \"store\": " + store + "\n" + "}";
    }

    private String prepareSearchCorrelationsRequest(String index, String event, String timestampField, Long timeWindow, int neighbors) {
        return String.format(
            Locale.ROOT,
            "%s?index=%s&event=%s&timestamp_field=%s&time_window=%s&nearby_events=%s",
            "/_correlation/events",
            index,
            event,
            timestampField,
            timeWindow,
            neighbors
        );
    }

    private String windowsToAppLogsCorrelationRule() {
        return "{\n"
            + "  \"name\": \"windows to app logs\",\n"
            + "  \"correlate\": [\n"
            + "    {\n"
            + "      \"index\": \"windows\",\n"
            + "      \"query\": \"host.hostname:EC2AMAZ*\",\n"
            + "      \"timestampField\": \"winlog.timestamp\",\n"
            + "      \"tags\": [\n"
            + "        \"windows\"\n"
            + "      ]\n"
            + "    },\n"
            + "    {\n"
            + "      \"index\": \"app_logs\",\n"
            + "      \"query\": \"endpoint:\\\\/customer_records.txt\",\n"
            + "      \"timestampField\": \"timestamp\",\n"
            + "      \"tags\": [\n"
            + "        \"others_application\"\n"
            + "      ]\n"
            + "    }\n"
            + "  ]\n"
            + "}";
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

    private String windowsMappings() {
        return "{"
            + "   \"settings\": {"
            + "       \"number_of_shards\": 1"
            + "   },"
            + "   \"mappings\": {"
            + "    \"properties\": {\n"
            + "      \"server.user.hash\": {\n"
            + "        \"type\": \"text\"\n"
            + "      },\n"
            + "      \"winlog.event_id\": {\n"
            + "        \"type\": \"integer\"\n"
            + "      },\n"
            + "      \"host.hostname\": {\n"
            + "        \"type\": \"text\"\n"
            + "      },\n"
            + "      \"windows.message\": {\n"
            + "        \"type\": \"text\"\n"
            + "      },\n"
            + "      \"winlog.provider_name\": {\n"
            + "        \"type\": \"text\"\n"
            + "      },\n"
            + "      \"winlog.event_data.ServiceName\": {\n"
            + "        \"type\": \"text\"\n"
            + "      },\n"
            + "      \"winlog.timestamp\": {\n"
            + "        \"type\": \"long\"\n"
            + "      }\n"
            + "    }\n"
            + " }\n"
            + "}";
    }

    private String appLogMappings() {
        return "{"
            + "   \"settings\": {"
            + "       \"number_of_shards\": 1"
            + "   },"
            + "   \"mappings\": {"
            + "    \"properties\": {\n"
            + "      \"http_method\": {\n"
            + "        \"type\": \"text\"\n"
            + "      },\n"
            + "      \"endpoint\": {\n"
            + "        \"type\": \"text\",\n"
            + "        \"analyzer\": \"whitespace\""
            + "      },\n"
            + "      \"keywords\": {\n"
            + "        \"type\": \"text\"\n"
            + "      },\n"
            + "      \"timestamp\": {\n"
            + "        \"type\": \"long\"\n"
            + "      }\n"
            + "    }\n"
            + " }\n"
            + "}";
    }

    private String sampleWindowsEvent() {
        return "{\n"
            + "  \"EventTime\": \"2020-02-04T14:59:39.343541+00:00\",\n"
            + "  \"host.hostname\": \"EC2AMAZEPO7HKA\",\n"
            + "  \"Keywords\": \"9223372036854775808\",\n"
            + "  \"SeverityValue\": 2,\n"
            + "  \"Severity\": \"INFO\",\n"
            + "  \"winlog.event_id\": 22,\n"
            + "  \"SourceName\": \"Microsoft-Windows-Sysmon\",\n"
            + "  \"ProviderGuid\": \"{5770385F-C22A-43E0-BF4C-06F5698FFBD9}\",\n"
            + "  \"Version\": 5,\n"
            + "  \"TaskValue\": 22,\n"
            + "  \"OpcodeValue\": 0,\n"
            + "  \"RecordNumber\": 9532,\n"
            + "  \"ExecutionProcessID\": 1996,\n"
            + "  \"ExecutionThreadID\": 2616,\n"
            + "  \"Channel\": \"Microsoft-Windows-Sysmon/Operational\",\n"
            + "  \"winlog.event_data.SubjectDomainName\": \"NTAUTHORITY\",\n"
            + "  \"AccountName\": \"SYSTEM\",\n"
            + "  \"UserID\": \"S-1-5-18\",\n"
            + "  \"AccountType\": \"User\",\n"
            + "  \"windows.message\": \"Dns query:\\r\\nRuleName: \\r\\nUtcTime: 2020-02-04 14:59:38.349\\r\\nProcessGuid: {b3c285a4-3cda-5dc0-0000-001077270b00}\\r\\nProcessId: 1904\\r\\nQueryName: EC2AMAZ-EPO7HKA\\r\\nQueryStatus: 0\\r\\nQueryResults: 172.31.46.38;\\r\\nImage: C:\\\\Program Files\\\\nxlog\\\\nxlog.exe\",\n"
            + "  \"Category\": \"Dns query (rule: DnsQuery)\",\n"
            + "  \"Opcode\": \"Info\",\n"
            + "  \"UtcTime\": \"2020-02-04 14:59:38.349\",\n"
            + "  \"ProcessGuid\": \"{b3c285a4-3cda-5dc0-0000-001077270b00}\",\n"
            + "  \"ProcessId\": \"1904\",\n"
            + "  \"QueryName\": \"EC2AMAZ-EPO7HKA\",\n"
            + "  \"QueryStatus\": \"0\",\n"
            + "  \"QueryResults\": \"172.31.46.38;\",\n"
            + "  \"Image\": \"C:\\\\Program Files\\\\nxlog\\\\regsvr32.exe\",\n"
            + "  \"EventReceivedTime\": \"2020-02-04T14:59:40.780905+00:00\",\n"
            + "  \"SourceModuleName\": \"in\",\n"
            + "  \"SourceModuleType\": \"im_msvistalog\",\n"
            + "  \"CommandLine\": \"eachtest\",\n"
            + "  \"Initiated\": \"true\",\n"
            + " \"winlog.timestamp\": "
            + System.currentTimeMillis()
            + "\n"
            + "}";
    }

    private String sampleAppLogsEvent() {
        return "{\n"
            + "  \"endpoint\": \"/customer_records.txt\",\n"
            + "  \"http_method\": \"POST\",\n"
            + "  \"keywords\": \"PermissionDenied\",\n"
            + "  \"timestamp\": "
            + System.currentTimeMillis()
            + "\n"
            + "}";
    }
}
