/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation;

import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.action.admin.cluster.node.info.PluginsAndModules;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugin.correlation.events.action.IndexCorrelationAction;
import org.opensearch.plugin.correlation.events.action.IndexCorrelationRequest;
import org.opensearch.plugin.correlation.events.action.IndexCorrelationResponse;
import org.opensearch.plugin.correlation.events.action.SearchCorrelatedEventsAction;
import org.opensearch.plugin.correlation.events.action.SearchCorrelatedEventsRequest;
import org.opensearch.plugin.correlation.events.action.SearchCorrelatedEventsResponse;
import org.opensearch.plugin.correlation.events.model.Correlation;
import org.opensearch.plugin.correlation.rules.action.IndexCorrelationRuleAction;
import org.opensearch.plugin.correlation.rules.action.IndexCorrelationRuleRequest;
import org.opensearch.plugin.correlation.rules.action.IndexCorrelationRuleResponse;
import org.opensearch.plugin.correlation.rules.model.CorrelationQuery;
import org.opensearch.plugin.correlation.rules.model.CorrelationRule;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Assert;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Transport Action tests for events-correlation-plugin
 */
public class EventsCorrelationPluginTransportIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(EventsCorrelationPlugin.class);
    }

    /**
     * test events-correlation-plugin is installed
     */
    public void testPluginsAreInstalled() {
        NodesInfoRequest nodesInfoRequest = new NodesInfoRequest();
        nodesInfoRequest.addMetric(NodesInfoRequest.Metric.PLUGINS.metricName());
        NodesInfoResponse nodesInfoResponse = OpenSearchIntegTestCase.client().admin().cluster().nodesInfo(nodesInfoRequest).actionGet();
        List<PluginInfo> pluginInfos = nodesInfoResponse.getNodes()
            .stream()
            .flatMap(
                (Function<NodeInfo, Stream<PluginInfo>>) nodeInfo -> nodeInfo.getInfo(PluginsAndModules.class).getPluginInfos().stream()
            )
            .collect(Collectors.toList());
        Assert.assertTrue(
            pluginInfos.stream()
                .anyMatch(pluginInfo -> pluginInfo.getName().equals("org.opensearch.plugin.correlation.EventsCorrelationPlugin"))
        );
    }

    /**
     * test creating a correlation rule
     * @throws Exception Exception
     */
    public void testCreatingACorrelationRule() throws Exception {
        List<CorrelationQuery> correlationQueries = Arrays.asList(
            new CorrelationQuery("s3_access_logs", "aws.cloudtrail.eventName:ReplicateObject", "@timestamp", List.of("s3")),
            new CorrelationQuery("app_logs", "keywords:PermissionDenied", "@timestamp", List.of("others_application"))
        );
        CorrelationRule correlationRule = new CorrelationRule("s3 to app logs", correlationQueries);
        IndexCorrelationRuleRequest request = new IndexCorrelationRuleRequest(correlationRule, RestRequest.Method.POST);

        IndexCorrelationRuleResponse response = client().execute(IndexCorrelationRuleAction.INSTANCE, request).get();
        Assert.assertEquals(RestStatus.CREATED, response.getStatus());

        NestedQueryBuilder queryBuilder = QueryBuilders.nestedQuery(
            "correlate",
            QueryBuilders.matchQuery("correlate.index", "s3_access_logs"),
            ScoreMode.None
        );
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(queryBuilder);
        searchSourceBuilder.fetchSource(true);

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(CorrelationRule.CORRELATION_RULE_INDEX);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client().search(searchRequest).get();
        Assert.assertEquals(1L, searchResponse.getHits().getTotalHits().value);
    }

    /**
     * test filtering correlation rules
     * @throws Exception Exception
     */
    public void testFilteringCorrelationRules() throws Exception {
        List<CorrelationQuery> correlationQueries1 = Arrays.asList(
            new CorrelationQuery("s3_access_logs", "aws.cloudtrail.eventName:ReplicateObject", "@timestamp", List.of("s3")),
            new CorrelationQuery("app_logs", "keywords:PermissionDenied", "@timestamp", List.of("others_application"))
        );
        CorrelationRule correlationRule1 = new CorrelationRule("s3 to app logs", correlationQueries1);
        IndexCorrelationRuleRequest request1 = new IndexCorrelationRuleRequest(correlationRule1, RestRequest.Method.POST);
        client().execute(IndexCorrelationRuleAction.INSTANCE, request1).get();

        List<CorrelationQuery> correlationQueries2 = Arrays.asList(
            new CorrelationQuery("windows", "host.hostname:EC2AMAZ*", "@timestamp", List.of("windows")),
            new CorrelationQuery("app_logs", "endpoint:/customer_records.txt", "@timestamp", List.of("others_application"))
        );
        CorrelationRule correlationRule2 = new CorrelationRule("windows to app logs", correlationQueries2);
        IndexCorrelationRuleRequest request2 = new IndexCorrelationRuleRequest(correlationRule2, RestRequest.Method.POST);
        client().execute(IndexCorrelationRuleAction.INSTANCE, request2).get();

        NestedQueryBuilder queryBuilder = QueryBuilders.nestedQuery(
            "correlate",
            QueryBuilders.matchQuery("correlate.index", "s3_access_logs"),
            ScoreMode.None
        );
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(queryBuilder);
        searchSourceBuilder.fetchSource(true);

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(CorrelationRule.CORRELATION_RULE_INDEX);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client().search(searchRequest).get();
        Assert.assertEquals(1L, searchResponse.getHits().getTotalHits().value);
    }

    /**
     * test creating a correlation rule with no timestamp field
     * @throws Exception Exception
     */
    @SuppressWarnings("unchecked")
    public void testCreatingACorrelationRuleWithNoTimestampField() throws Exception {
        List<CorrelationQuery> correlationQueries = Arrays.asList(
            new CorrelationQuery("s3_access_logs", "aws.cloudtrail.eventName:ReplicateObject", null, List.of("s3")),
            new CorrelationQuery("app_logs", "keywords:PermissionDenied", null, List.of("others_application"))
        );
        CorrelationRule correlationRule = new CorrelationRule("s3 to app logs", correlationQueries);
        IndexCorrelationRuleRequest request = new IndexCorrelationRuleRequest(correlationRule, RestRequest.Method.POST);

        IndexCorrelationRuleResponse response = client().execute(IndexCorrelationRuleAction.INSTANCE, request).get();
        Assert.assertEquals(RestStatus.CREATED, response.getStatus());

        NestedQueryBuilder queryBuilder = QueryBuilders.nestedQuery(
            "correlate",
            QueryBuilders.matchQuery("correlate.index", "s3_access_logs"),
            ScoreMode.None
        );
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(queryBuilder);
        searchSourceBuilder.fetchSource(true);

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(CorrelationRule.CORRELATION_RULE_INDEX);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client().search(searchRequest).get();
        Assert.assertEquals(1L, searchResponse.getHits().getTotalHits().value);
        Assert.assertEquals(
            "_timestamp",
            ((List<Map<String, Object>>) (searchResponse.getHits().getHits()[0].getSourceAsMap().get("correlate"))).get(0)
                .get("timestampField")
        );
    }

    public void testEventOnIndexWithNoRules() throws ExecutionException, InterruptedException {
        String windowsIndex = "windows";
        CreateIndexRequest windowsRequest = new CreateIndexRequest(windowsIndex).mapping(windowsMappings()).settings(Settings.EMPTY);

        client().admin().indices().create(windowsRequest).get();

        String appLogsIndex = "app_logs";
        CreateIndexRequest appLogsRequest = new CreateIndexRequest(appLogsIndex).mapping(appLogsMappings()).settings(Settings.EMPTY);

        client().admin().indices().create(appLogsRequest).get();

        List<CorrelationQuery> correlationQueries = List.of(
            new CorrelationQuery("app_logs", "endpoint:\\/customer_records.txt", "timestamp", List.of())
        );
        CorrelationRule correlationRule = new CorrelationRule("windows to app logs", correlationQueries);
        IndexCorrelationRuleRequest request = new IndexCorrelationRuleRequest(correlationRule, RestRequest.Method.POST);
        client().execute(IndexCorrelationRuleAction.INSTANCE, request).get();

        IndexRequest indexRequestWindows = new IndexRequest("windows").source(sampleWindowsEvent(), XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        IndexResponse response = client().index(indexRequestWindows).get();
        String eventId = response.getId();

        IndexCorrelationRequest correlationRequest = new IndexCorrelationRequest("windows", eventId, false);
        IndexCorrelationResponse correlationResponse = client().execute(IndexCorrelationAction.INSTANCE, correlationRequest).get();

        Assert.assertEquals(200, correlationResponse.getStatus().getStatus());
        Assert.assertTrue(correlationResponse.getOrphan());
        Assert.assertEquals(0, correlationResponse.getNeighborEvents().size());
    }

    public void testEventOnIndexWithNoMatchingRules() throws ExecutionException, InterruptedException {
        String windowsIndex = "windows";
        CreateIndexRequest windowsRequest = new CreateIndexRequest(windowsIndex).mapping(windowsMappings()).settings(Settings.EMPTY);

        client().admin().indices().create(windowsRequest).get();

        String appLogsIndex = "app_logs";
        CreateIndexRequest appLogsRequest = new CreateIndexRequest(appLogsIndex).mapping(appLogsMappings()).settings(Settings.EMPTY);

        client().admin().indices().create(appLogsRequest).get();

        List<CorrelationQuery> correlationQueries = Arrays.asList(
            new CorrelationQuery("windows", "host.hostname:EC2BMAZ*", "winlog.timestamp", List.of())
        );
        CorrelationRule correlationRule = new CorrelationRule("windows to app logs", correlationQueries);
        IndexCorrelationRuleRequest request = new IndexCorrelationRuleRequest(correlationRule, RestRequest.Method.POST);
        client().execute(IndexCorrelationRuleAction.INSTANCE, request).get();

        IndexRequest indexRequestWindows = new IndexRequest("windows").source(sampleWindowsEvent(), XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        IndexResponse response = client().index(indexRequestWindows).get();
        String eventId = response.getId();

        IndexCorrelationRequest correlationRequest = new IndexCorrelationRequest("windows", eventId, false);
        IndexCorrelationResponse correlationResponse = client().execute(IndexCorrelationAction.INSTANCE, correlationRequest).get();

        Assert.assertEquals(200, correlationResponse.getStatus().getStatus());
        Assert.assertTrue(correlationResponse.getOrphan());
        Assert.assertEquals(0, correlationResponse.getNeighborEvents().size());
    }

    public void testCorrelationWithSingleRule() throws ExecutionException, InterruptedException {
        String windowsIndex = "windows";
        CreateIndexRequest windowsRequest = new CreateIndexRequest(windowsIndex).mapping(windowsMappings()).settings(Settings.EMPTY);

        client().admin().indices().create(windowsRequest).get();

        String appLogsIndex = "app_logs";
        CreateIndexRequest appLogsRequest = new CreateIndexRequest(appLogsIndex).mapping(appLogsMappings()).settings(Settings.EMPTY);

        client().admin().indices().create(appLogsRequest).get();

        List<CorrelationQuery> correlationQueries = Arrays.asList(
            new CorrelationQuery("windows", "host.hostname:EC2AMAZ*", "winlog.timestamp", List.of()),
            new CorrelationQuery("app_logs", "endpoint:\\/customer_records.txt", "timestamp", List.of())
        );
        CorrelationRule correlationRule = new CorrelationRule("windows to app logs", correlationQueries);
        IndexCorrelationRuleRequest request = new IndexCorrelationRuleRequest(correlationRule, RestRequest.Method.POST);
        client().execute(IndexCorrelationRuleAction.INSTANCE, request).get();

        IndexRequest indexRequestWindows = new IndexRequest("windows").source(sampleWindowsEvent(), XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        client().index(indexRequestWindows).get();

        IndexRequest indexRequestAppLogs = new IndexRequest("app_logs").source(sampleAppLogsEvent(), XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        IndexResponse response = client().index(indexRequestAppLogs).get();
        String eventId = response.getId();

        IndexCorrelationRequest correlationRequest = new IndexCorrelationRequest("app_logs", eventId, false);
        IndexCorrelationResponse correlationResponse = client().execute(IndexCorrelationAction.INSTANCE, correlationRequest).get();

        Assert.assertEquals(200, correlationResponse.getStatus().getStatus());
        Assert.assertEquals(1, correlationResponse.getNeighborEvents().size());
        Assert.assertFalse(correlationResponse.getOrphan());
    }

    public void testSearchCorrelationWithSingleRule() throws ExecutionException, InterruptedException, IOException {
        String windowsIndex = "windows";
        CreateIndexRequest windowsRequest = new CreateIndexRequest(windowsIndex).mapping(windowsMappings()).settings(Settings.EMPTY);

        client().admin().indices().create(windowsRequest).get();

        String appLogsIndex = "app_logs";
        CreateIndexRequest appLogsRequest = new CreateIndexRequest(appLogsIndex).mapping(appLogsMappings()).settings(Settings.EMPTY);

        client().admin().indices().create(appLogsRequest).get();

        List<CorrelationQuery> correlationQueries = Arrays.asList(
            new CorrelationQuery("windows", "host.hostname:EC2AMAZ*", "winlog.timestamp", List.of()),
            new CorrelationQuery("app_logs", "endpoint:\\/customer_records.txt", "timestamp", List.of())
        );
        CorrelationRule correlationRule = new CorrelationRule("windows to app logs", correlationQueries);
        IndexCorrelationRuleRequest request = new IndexCorrelationRuleRequest(correlationRule, RestRequest.Method.POST);
        client().execute(IndexCorrelationRuleAction.INSTANCE, request).get();

        IndexRequest indexRequestWindows = new IndexRequest("windows").source(sampleWindowsEvent(), XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        IndexResponse response = client().index(indexRequestWindows).get();
        String windowsEventId = response.getId();

        IndexRequest indexRequestAppLogs = new IndexRequest("app_logs").source(sampleAppLogsEvent(), XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        response = client().index(indexRequestAppLogs).get();
        String eventId = response.getId();

        IndexCorrelationRequest correlationRequest = new IndexCorrelationRequest("app_logs", eventId, true);
        IndexCorrelationResponse correlationResponse = client().execute(IndexCorrelationAction.INSTANCE, correlationRequest).get();

        Assert.assertEquals(200, correlationResponse.getStatus().getStatus());
        Assert.assertEquals(1, correlationResponse.getNeighborEvents().size());
        Assert.assertFalse(correlationResponse.getOrphan());

        correlationRequest = new IndexCorrelationRequest("windows", windowsEventId, true);
        client().execute(IndexCorrelationAction.INSTANCE, correlationRequest).get();

        SearchCorrelatedEventsRequest searchCorrelatedEventsRequest = new SearchCorrelatedEventsRequest(
            "windows",
            windowsEventId,
            "winlog.timestamp",
            300000L,
            5
        );
        SearchCorrelatedEventsResponse correlatedEventsResponse = client().execute(
            SearchCorrelatedEventsAction.INSTANCE,
            searchCorrelatedEventsRequest
        ).get();
        Assert.assertEquals(1, correlatedEventsResponse.getEvents().size());
    }

    public void testCorrelationWithMultipleRule() throws ExecutionException, InterruptedException {
        String windowsIndex = "windows";
        CreateIndexRequest windowsRequest = new CreateIndexRequest(windowsIndex).mapping(windowsMappings()).settings(Settings.EMPTY);

        client().admin().indices().create(windowsRequest).get();

        String appLogsIndex = "app_logs";
        CreateIndexRequest appLogsRequest = new CreateIndexRequest(appLogsIndex).mapping(appLogsMappings()).settings(Settings.EMPTY);

        client().admin().indices().create(appLogsRequest).get();

        List<CorrelationQuery> correlationQueries1 = Arrays.asList(
            new CorrelationQuery("windows", "host.hostname:EC2AMAZ*", "winlog.timestamp", List.of()),
            new CorrelationQuery("app_logs", "endpoint:\\/customer_records.txt", "timestamp", List.of())
        );
        CorrelationRule correlationRule1 = new CorrelationRule("windows to app logs", correlationQueries1);
        IndexCorrelationRuleRequest request1 = new IndexCorrelationRuleRequest(correlationRule1, RestRequest.Method.POST);
        client().execute(IndexCorrelationRuleAction.INSTANCE, request1).get();

        List<CorrelationQuery> correlationQueries2 = Arrays.asList(
            new CorrelationQuery("windows", "host.hostname:EC2BMAZ*", "winlog.timestamp", List.of()),
            new CorrelationQuery("app_logs", "endpoint:\\/customer_records1.txt", "timestamp", List.of())
        );
        CorrelationRule correlationRule2 = new CorrelationRule("windows to app logs", correlationQueries2);
        IndexCorrelationRuleRequest request2 = new IndexCorrelationRuleRequest(correlationRule2, RestRequest.Method.POST);
        client().execute(IndexCorrelationRuleAction.INSTANCE, request2).get();

        IndexRequest indexRequestWindows = new IndexRequest("windows").source(sampleWindowsEvent(), XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        client().index(indexRequestWindows).get();

        IndexRequest indexRequestAppLogs = new IndexRequest("app_logs").source(sampleAppLogsEvent(), XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        IndexResponse response = client().index(indexRequestAppLogs).get();
        String eventId = response.getId();

        IndexCorrelationRequest correlationRequest = new IndexCorrelationRequest("app_logs", eventId, false);
        IndexCorrelationResponse correlationResponse = client().execute(IndexCorrelationAction.INSTANCE, correlationRequest).get();

        Assert.assertEquals(200, correlationResponse.getStatus().getStatus());
        Assert.assertFalse(correlationResponse.getOrphan());
        Assert.assertEquals(1, correlationResponse.getNeighborEvents().size());
    }

    public void testStoringCorrelationWithMultipleRule() throws ExecutionException, InterruptedException {
        String networkIndex = "vpc_flow";
        CreateIndexRequest networkRequest = new CreateIndexRequest(networkIndex).mapping(networkMappings()).settings(Settings.EMPTY);

        client().admin().indices().create(networkRequest).get();

        String adLdapIndex = "ad_logs";
        CreateIndexRequest adLdapRequest = new CreateIndexRequest(adLdapIndex).mapping(adLdapMappings()).settings(Settings.EMPTY);

        client().admin().indices().create(adLdapRequest).get();

        String windowsIndex = "windows";
        CreateIndexRequest windowsRequest = new CreateIndexRequest(windowsIndex).mapping(windowsMappings()).settings(Settings.EMPTY);

        client().admin().indices().create(windowsRequest).get();

        String appLogsIndex = "app_logs";
        CreateIndexRequest appLogsRequest = new CreateIndexRequest(appLogsIndex).mapping(appLogsMappings()).settings(Settings.EMPTY);

        client().admin().indices().create(appLogsRequest).get();

        String s3AccessLogsIndex = "s3_access_logs";
        CreateIndexRequest s3AccessLogsRequest = new CreateIndexRequest(s3AccessLogsIndex).mapping(s3AccessLogsMapping())
            .settings(Settings.EMPTY);

        client().admin().indices().create(s3AccessLogsRequest).get();

        List<CorrelationQuery> windowsAppLogsQuery = Arrays.asList(
            new CorrelationQuery("windows", "host.hostname:EC2AMAZ*", "winlog.timestamp", List.of()),
            new CorrelationQuery("app_logs", "endpoint:\\/customer_records.txt", "timestamp", List.of())
        );
        CorrelationRule windowsAppLogsRule = new CorrelationRule("windows to app logs", windowsAppLogsQuery);
        IndexCorrelationRuleRequest windowsAppLogsRequest = new IndexCorrelationRuleRequest(windowsAppLogsRule, RestRequest.Method.POST);
        client().execute(IndexCorrelationRuleAction.INSTANCE, windowsAppLogsRequest).get();

        List<CorrelationQuery> networkWindowsAdLdapQuery = Arrays.asList(
            new CorrelationQuery("vpc_flow", "dstaddr:4.5.6.7 or dstaddr:4.5.6.6", "timestamp", List.of()),
            new CorrelationQuery("windows", "winlog.event_data.SubjectDomainName:NTAUTHORI*", "winlog.timestamp", List.of()),
            new CorrelationQuery("ad_logs", "ResultType:50126", "timestamp", List.of())
        );
        CorrelationRule networkWindowsAdLdapRule = new CorrelationRule("netowrk to windows to ad/ldap", networkWindowsAdLdapQuery);
        IndexCorrelationRuleRequest networkWindowsAdLdapRequest = new IndexCorrelationRuleRequest(
            networkWindowsAdLdapRule,
            RestRequest.Method.POST
        );
        client().execute(IndexCorrelationRuleAction.INSTANCE, networkWindowsAdLdapRequest).get();

        List<CorrelationQuery> s3AppLogsQuery = Arrays.asList(
            new CorrelationQuery("s3_access_logs", "aws.cloudtrail.eventName:ReplicateObject", "timestamp", List.of()),
            new CorrelationQuery("app_logs", "keywords:PermissionDenied", "timestamp", List.of())
        );
        CorrelationRule s3AppLogsRule = new CorrelationRule("s3 to app logs", s3AppLogsQuery);
        IndexCorrelationRuleRequest s3AppLogsRequest = new IndexCorrelationRuleRequest(s3AppLogsRule, RestRequest.Method.POST);
        client().execute(IndexCorrelationRuleAction.INSTANCE, s3AppLogsRequest).get();

        IndexRequest indexRequestNetwork = new IndexRequest("vpc_flow").source(sampleNetworkEvent(), XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        IndexResponse indexResponseNetwork = client().index(indexRequestNetwork).get();

        String networkEventId = indexResponseNetwork.getId();
        IndexCorrelationRequest networkCorrelationRequest = new IndexCorrelationRequest("vpc_flow", networkEventId, true);
        IndexCorrelationResponse networkCorrelationResponse = client().execute(IndexCorrelationAction.INSTANCE, networkCorrelationRequest)
            .get();
        Assert.assertEquals(200, networkCorrelationResponse.getStatus().getStatus());
        Assert.assertEquals(true, networkCorrelationResponse.getOrphan());

        IndexRequest indexRequestAdLdap = new IndexRequest("ad_logs").source(sampleAdLdapEvent(), XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        IndexResponse indexResponseAdLdap = client().index(indexRequestAdLdap).get();

        String adLdapEventId = indexResponseAdLdap.getId();
        IndexCorrelationRequest adLdapCorrelationRequest = new IndexCorrelationRequest("ad_logs", adLdapEventId, true);
        IndexCorrelationResponse adLdapCorrelationResponse = client().execute(IndexCorrelationAction.INSTANCE, adLdapCorrelationRequest)
            .get();
        Assert.assertEquals(200, adLdapCorrelationResponse.getStatus().getStatus());
        Assert.assertEquals(false, adLdapCorrelationResponse.getOrphan());
        Assert.assertEquals(1, adLdapCorrelationResponse.getNeighborEvents().size());

        IndexRequest indexRequestWindows = new IndexRequest("windows").source(sampleWindowsEvent(), XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        IndexResponse indexResponseWindows = client().index(indexRequestWindows).get();

        String windowsEventId = indexResponseWindows.getId();
        IndexCorrelationRequest windowsCorrelationRequest = new IndexCorrelationRequest("windows", windowsEventId, true);
        IndexCorrelationResponse windowsCorrelationResponse = client().execute(IndexCorrelationAction.INSTANCE, windowsCorrelationRequest)
            .get();
        Assert.assertEquals(200, windowsCorrelationResponse.getStatus().getStatus());
        Assert.assertEquals(false, windowsCorrelationResponse.getOrphan());
        Assert.assertEquals(2, windowsCorrelationResponse.getNeighborEvents().size());

        IndexRequest indexRequestAppLogs = new IndexRequest("app_logs").source(sampleAppLogsEvent(), XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        IndexResponse indexResponseAppLogs = client().index(indexRequestAppLogs).get();

        String appLogsEventId = indexResponseAppLogs.getId();
        IndexCorrelationRequest appLogsCorrelationRequest = new IndexCorrelationRequest("app_logs", appLogsEventId, true);
        IndexCorrelationResponse appLogsCorrelationResponse = client().execute(IndexCorrelationAction.INSTANCE, appLogsCorrelationRequest)
            .get();
        Assert.assertEquals(200, appLogsCorrelationResponse.getStatus().getStatus());
        Assert.assertEquals(false, appLogsCorrelationResponse.getOrphan());
        Assert.assertEquals(1, appLogsCorrelationResponse.getNeighborEvents().size());

        IndexRequest indexRequestS3Logs = new IndexRequest("s3_access_logs").source(sampleS3AccessEvent(), XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        IndexResponse indexResponseS3 = client().index(indexRequestS3Logs).get();

        String s3EventId = indexResponseS3.getId();
        IndexCorrelationRequest s3CorrelationRequest = new IndexCorrelationRequest("s3_access_logs", s3EventId, true);
        IndexCorrelationResponse s3CorrelationResponse = client().execute(IndexCorrelationAction.INSTANCE, s3CorrelationRequest).get();
        Assert.assertEquals(200, s3CorrelationResponse.getStatus().getStatus());
        Assert.assertEquals(false, s3CorrelationResponse.getOrphan());
        Assert.assertEquals(1, s3CorrelationResponse.getNeighborEvents().size());

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.fetchSource(true);
        searchSourceBuilder.size(100);

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(Correlation.CORRELATION_HISTORY_INDEX);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client().search(searchRequest).get();
        Assert.assertEquals(12L, searchResponse.getHits().getTotalHits().value);
    }

    public void testStoringCorrelationWithMultipleLevels() throws ExecutionException, InterruptedException {
        String networkIndex = "vpc_flow";
        CreateIndexRequest networkRequest = new CreateIndexRequest(networkIndex).mapping(networkMappings()).settings(Settings.EMPTY);

        client().admin().indices().create(networkRequest).get();

        String adLdapIndex = "ad_logs";
        CreateIndexRequest adLdapRequest = new CreateIndexRequest(adLdapIndex).mapping(adLdapMappings()).settings(Settings.EMPTY);

        client().admin().indices().create(adLdapRequest).get();

        String windowsIndex = "windows";
        CreateIndexRequest windowsRequest = new CreateIndexRequest(windowsIndex).mapping(windowsMappings()).settings(Settings.EMPTY);

        client().admin().indices().create(windowsRequest).get();

        String appLogsIndex = "app_logs";
        CreateIndexRequest appLogsRequest = new CreateIndexRequest(appLogsIndex).mapping(appLogsMappings()).settings(Settings.EMPTY);

        client().admin().indices().create(appLogsRequest).get();

        List<CorrelationQuery> networkWindowsAdLdapQuery = Arrays.asList(
            new CorrelationQuery("vpc_flow", "dstaddr:4.5.6.7 or dstaddr:4.5.6.6", "timestamp", List.of()),
            new CorrelationQuery("windows", "winlog.event_data.SubjectDomainName:NTAUTHORI*", "winlog.timestamp", List.of()),
            new CorrelationQuery("ad_logs", "ResultType:50126", "timestamp", List.of())
        );
        CorrelationRule networkWindowsAdLdapRule = new CorrelationRule("netowrk to windows to ad/ldap", networkWindowsAdLdapQuery);
        IndexCorrelationRuleRequest networkWindowsAdLdapRequest = new IndexCorrelationRuleRequest(
            networkWindowsAdLdapRule,
            RestRequest.Method.POST
        );
        client().execute(IndexCorrelationRuleAction.INSTANCE, networkWindowsAdLdapRequest).get();

        IndexRequest indexRequestNetwork = new IndexRequest("vpc_flow").source(sampleNetworkEvent(), XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        IndexResponse indexResponseNetwork = client().index(indexRequestNetwork).get();

        String networkEventId = indexResponseNetwork.getId();
        IndexCorrelationRequest networkCorrelationRequest = new IndexCorrelationRequest("vpc_flow", networkEventId, true);
        IndexCorrelationResponse networkCorrelationResponse = client().execute(IndexCorrelationAction.INSTANCE, networkCorrelationRequest)
            .get();
        Assert.assertEquals(200, networkCorrelationResponse.getStatus().getStatus());
        Assert.assertEquals(true, networkCorrelationResponse.getOrphan());

        IndexRequest indexRequestAdLdap = new IndexRequest("ad_logs").source(sampleAdLdapEvent(), XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        IndexResponse indexResponseAdLdap = client().index(indexRequestAdLdap).get();

        String adLdapEventId = indexResponseAdLdap.getId();
        IndexCorrelationRequest adLdapCorrelationRequest = new IndexCorrelationRequest("ad_logs", adLdapEventId, true);
        IndexCorrelationResponse adLdapCorrelationResponse = client().execute(IndexCorrelationAction.INSTANCE, adLdapCorrelationRequest)
            .get();
        Assert.assertEquals(200, adLdapCorrelationResponse.getStatus().getStatus());
        Assert.assertEquals(false, adLdapCorrelationResponse.getOrphan());
        Assert.assertEquals(1, adLdapCorrelationResponse.getNeighborEvents().size());

        IndexRequest indexRequestWindows = new IndexRequest("windows").source(sampleWindowsEvent(), XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        IndexResponse indexResponseWindows = client().index(indexRequestWindows).get();

        String windowsEventId = indexResponseWindows.getId();
        IndexCorrelationRequest windowsCorrelationRequest = new IndexCorrelationRequest("windows", windowsEventId, true);
        IndexCorrelationResponse windowsCorrelationResponse = client().execute(IndexCorrelationAction.INSTANCE, windowsCorrelationRequest)
            .get();
        Assert.assertEquals(200, windowsCorrelationResponse.getStatus().getStatus());
        Assert.assertEquals(false, windowsCorrelationResponse.getOrphan());
        Assert.assertEquals(2, windowsCorrelationResponse.getNeighborEvents().size());

        IndexRequest indexRequestAppLogs = new IndexRequest("app_logs").source(sampleAppLogsEvent(), XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        IndexResponse indexResponseAppLogs = client().index(indexRequestAppLogs).get();

        String appLogsEventId = indexResponseAppLogs.getId();
        IndexCorrelationRequest appLogsCorrelationRequest = new IndexCorrelationRequest("app_logs", appLogsEventId, true);
        IndexCorrelationResponse appLogsCorrelationResponse = client().execute(IndexCorrelationAction.INSTANCE, appLogsCorrelationRequest)
            .get();
        Assert.assertEquals(200, appLogsCorrelationResponse.getStatus().getStatus());
        Assert.assertEquals(true, appLogsCorrelationResponse.getOrphan());
        Assert.assertEquals(0, appLogsCorrelationResponse.getNeighborEvents().size());

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("index1", "app_logs"));
        searchSourceBuilder.fetchSource(true);
        searchSourceBuilder.size(100);

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(Correlation.CORRELATION_HISTORY_INDEX);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client().search(searchRequest).get();
        Assert.assertEquals(1L, searchResponse.getHits().getTotalHits().value);
        Assert.assertEquals(100, Objects.requireNonNull(searchResponse.getHits().getHits()[0].getSourceAsMap()).get("level"));
    }

    public void testStoringCorrelationWithMultipleLevelsWithSeparateGroups() throws ExecutionException, InterruptedException {
        String networkIndex = "vpc_flow";
        CreateIndexRequest networkRequest = new CreateIndexRequest(networkIndex).mapping(networkMappings()).settings(Settings.EMPTY);

        client().admin().indices().create(networkRequest).get();

        String adLdapIndex = "ad_logs";
        CreateIndexRequest adLdapRequest = new CreateIndexRequest(adLdapIndex).mapping(adLdapMappings()).settings(Settings.EMPTY);

        client().admin().indices().create(adLdapRequest).get();

        String windowsIndex = "windows";
        CreateIndexRequest windowsRequest = new CreateIndexRequest(windowsIndex).mapping(windowsMappings()).settings(Settings.EMPTY);

        client().admin().indices().create(windowsRequest).get();

        String appLogsIndex = "app_logs";
        CreateIndexRequest appLogsRequest = new CreateIndexRequest(appLogsIndex).mapping(appLogsMappings()).settings(Settings.EMPTY);

        client().admin().indices().create(appLogsRequest).get();

        List<CorrelationQuery> networkWindowsAdLdapQuery = Arrays.asList(
            new CorrelationQuery("vpc_flow", "dstaddr:4.5.6.7 or dstaddr:4.5.6.6", "timestamp", List.of()),
            new CorrelationQuery("windows", "winlog.event_data.SubjectDomainName:NTAUTHORI*", "winlog.timestamp", List.of()),
            new CorrelationQuery("ad_logs", "ResultType:50126", "timestamp", List.of())
        );
        CorrelationRule networkWindowsAdLdapRule = new CorrelationRule("netowrk to windows to ad/ldap", networkWindowsAdLdapQuery);
        IndexCorrelationRuleRequest networkWindowsAdLdapRequest = new IndexCorrelationRuleRequest(
            networkWindowsAdLdapRule,
            RestRequest.Method.POST
        );
        client().execute(IndexCorrelationRuleAction.INSTANCE, networkWindowsAdLdapRequest).get();

        List<CorrelationQuery> s3AppLogsQuery = Arrays.asList(
            new CorrelationQuery("s3_access_logs", "aws.cloudtrail.eventName:ReplicateObject", "timestamp", List.of()),
            new CorrelationQuery("app_logs", "keywords:PermissionDenied", "timestamp", List.of())
        );
        CorrelationRule s3AppLogsRule = new CorrelationRule("s3 to app logs", s3AppLogsQuery);
        IndexCorrelationRuleRequest s3AppLogsRequest = new IndexCorrelationRuleRequest(s3AppLogsRule, RestRequest.Method.POST);
        client().execute(IndexCorrelationRuleAction.INSTANCE, s3AppLogsRequest).get();

        IndexRequest indexRequestNetwork = new IndexRequest("vpc_flow").source(sampleNetworkEvent(), XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        IndexResponse indexResponseNetwork = client().index(indexRequestNetwork).get();

        String networkEventId = indexResponseNetwork.getId();
        IndexCorrelationRequest networkCorrelationRequest = new IndexCorrelationRequest("vpc_flow", networkEventId, true);
        IndexCorrelationResponse networkCorrelationResponse = client().execute(IndexCorrelationAction.INSTANCE, networkCorrelationRequest)
            .get();
        Assert.assertEquals(200, networkCorrelationResponse.getStatus().getStatus());
        Assert.assertEquals(true, networkCorrelationResponse.getOrphan());

        IndexRequest indexRequestAdLdap = new IndexRequest("ad_logs").source(sampleAdLdapEvent(), XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        IndexResponse indexResponseAdLdap = client().index(indexRequestAdLdap).get();

        String adLdapEventId = indexResponseAdLdap.getId();
        IndexCorrelationRequest adLdapCorrelationRequest = new IndexCorrelationRequest("ad_logs", adLdapEventId, true);
        IndexCorrelationResponse adLdapCorrelationResponse = client().execute(IndexCorrelationAction.INSTANCE, adLdapCorrelationRequest)
            .get();
        Assert.assertEquals(200, adLdapCorrelationResponse.getStatus().getStatus());
        Assert.assertEquals(false, adLdapCorrelationResponse.getOrphan());
        Assert.assertEquals(1, adLdapCorrelationResponse.getNeighborEvents().size());

        IndexRequest indexRequestWindows = new IndexRequest("windows").source(sampleWindowsEvent(), XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        IndexResponse indexResponseWindows = client().index(indexRequestWindows).get();

        String windowsEventId = indexResponseWindows.getId();
        IndexCorrelationRequest windowsCorrelationRequest = new IndexCorrelationRequest("windows", windowsEventId, true);
        IndexCorrelationResponse windowsCorrelationResponse = client().execute(IndexCorrelationAction.INSTANCE, windowsCorrelationRequest)
            .get();
        Assert.assertEquals(200, windowsCorrelationResponse.getStatus().getStatus());
        Assert.assertEquals(false, windowsCorrelationResponse.getOrphan());
        Assert.assertEquals(2, windowsCorrelationResponse.getNeighborEvents().size());

        IndexRequest indexRequestAppLogs = new IndexRequest("app_logs").source(sampleAppLogsEvent(), XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        IndexResponse indexResponseAppLogs = client().index(indexRequestAppLogs).get();

        String appLogsEventId = indexResponseAppLogs.getId();
        IndexCorrelationRequest appLogsCorrelationRequest = new IndexCorrelationRequest("app_logs", appLogsEventId, true);
        IndexCorrelationResponse appLogsCorrelationResponse = client().execute(IndexCorrelationAction.INSTANCE, appLogsCorrelationRequest)
            .get();
        Assert.assertEquals(200, appLogsCorrelationResponse.getStatus().getStatus());
        Assert.assertEquals(true, appLogsCorrelationResponse.getOrphan());

        IndexRequest indexRequestS3Logs = new IndexRequest("s3_access_logs").source(sampleS3AccessEvent(), XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        IndexResponse indexResponseS3 = client().index(indexRequestS3Logs).get();

        String s3EventId = indexResponseS3.getId();
        IndexCorrelationRequest s3CorrelationRequest = new IndexCorrelationRequest("s3_access_logs", s3EventId, true);
        IndexCorrelationResponse s3CorrelationResponse = client().execute(IndexCorrelationAction.INSTANCE, s3CorrelationRequest).get();
        Assert.assertEquals(200, s3CorrelationResponse.getStatus().getStatus());
        Assert.assertEquals(false, s3CorrelationResponse.getOrphan());
        Assert.assertEquals(1, s3CorrelationResponse.getNeighborEvents().size());

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("index2", "app_logs"))
                .must(QueryBuilders.matchQuery("index1", "s3_access_logs"))
        );
        searchSourceBuilder.fetchSource(true);
        searchSourceBuilder.size(100);

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(Correlation.CORRELATION_HISTORY_INDEX);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client().search(searchRequest).get();
        Assert.assertEquals(1L, searchResponse.getHits().getTotalHits().value);
        Assert.assertEquals(75, Objects.requireNonNull(searchResponse.getHits().getHits()[0].getSourceAsMap()).get("level"));
    }

    private String networkMappings() {
        return "\"properties\": {\n"
            + "      \"version\": {\n"
            + "        \"type\": \"integer\"\n"
            + "      },\n"
            + "      \"account-id\": {\n"
            + "        \"type\": \"text\"\n"
            + "      },\n"
            + "      \"interface-id\": {\n"
            + "        \"type\": \"text\"\n"
            + "      },\n"
            + "      \"srcaddr\": {\n"
            + "        \"type\": \"text\"\n"
            + "      },\n"
            + "      \"dstaddr\": {\n"
            + "        \"type\": \"text\"\n"
            + "      },\n"
            + "      \"srcport\": {\n"
            + "        \"type\": \"integer\"\n"
            + "      },\n"
            + "      \"dstport\": {\n"
            + "        \"type\": \"integer\"\n"
            + "      },\n"
            + "      \"severity_id\": {\n"
            + "        \"type\": \"text\"\n"
            + "      },\n"
            + "      \"class_name\": {\n"
            + "        \"type\": \"text\"\n"
            + "      },\n"
            + "      \"timestamp\": {\n"
            + "        \"type\": \"long\"\n"
            + "      },\n"
            + "    }";
    }

    private String sampleNetworkEvent() {
        return "{\n"
            + "  \"version\": 1,\n"
            + "  \"account-id\": \"A12345\",\n"
            + "  \"interface-id\": \"I12345\",\n"
            + "  \"srcaddr\": \"1.2.3.4\",\n"
            + "  \"dstaddr\": \"4.5.6.7\",\n"
            + "  \"srcport\": 9000,\n"
            + "  \"dstport\": 8000,\n"
            + "  \"severity_id\": \"-1\",\n"
            + "  \"class_name\": \"Network Activity\",\n"
            + "  \"timestamp\": "
            + System.currentTimeMillis()
            + "\n"
            + "}";
    }

    private String adLdapMappings() {
        return "\"properties\": {\n"
            + "      \"ResultType\": {\n"
            + "        \"type\": \"integer\"\n"
            + "      },\n"
            + "      \"ResultDescription\": {\n"
            + "        \"type\": \"text\"\n"
            + "      },\n"
            + "      \"winlog.event_data.TargetUserName\": {\n"
            + "        \"type\": \"text\"\n"
            + "      },\n"
            + "      \"timestamp\": {\n"
            + "        \"type\": \"long\"\n"
            + "      },\n"
            + "    }";
    }

    private String sampleAdLdapEvent() {
        return "{\n"
            + "  \"ResultType\": 50126,\n"
            + "  \"ResultDescription\": \"Invalid username or password or Invalid on-premises username or password.\",\n"
            + "  \"winlog.event_data.TargetUserName\": \"DEYSUBHO\",\n"
            + "  \"timestamp\": "
            + System.currentTimeMillis()
            + "\n"
            + "}";
    }

    private String windowsMappings() {
        return "    \"properties\": {\n"
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
            + "    }\n";
    }

    private String sampleWindowsEvent() {
        return "{\n"
            + "  \"EventTime\": \"2020-02-04T14:59:39.343541+00:00\",\n"
            + "  \"host.hostname\": \"EC2AMAZ-EPO7HKA\",\n"
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

    private String appLogsMappings() {
        return "    \"properties\": {\n"
            + "      \"http_method\": {\n"
            + "        \"type\": \"text\"\n"
            + "      },\n"
            + "      \"endpoint\": {\n"
            + "        \"type\": \"text\"\n"
            + "      },\n"
            + "      \"keywords\": {\n"
            + "        \"type\": \"text\"\n"
            + "      },\n"
            + "      \"timestamp\": {\n"
            + "        \"type\": \"long\"\n"
            + "      }\n"
            + "    }\n";
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

    private String s3AccessLogsMapping() {
        return "\"properties\": {\n"
            + "      \"aws.cloudtrail.eventSource\": {\n"
            + "        \"type\": \"text\"\n"
            + "      },\n"
            + "      \"aws.cloudtrail.eventName\": {\n"
            + "        \"type\": \"text\"\n"
            + "      },\n"
            + "      \"aws.cloudtrail.eventTime\": {\n"
            + "        \"type\": \"integer\"\n"
            + "      },\n"
            + "      \"timestamp\": {\n"
            + "        \"type\": \"long\"\n"
            + "      }\n"
            + "    }";
    }

    private String sampleS3AccessEvent() {
        return "{\n"
            + "  \"aws.cloudtrail.eventSource\": \"s3.amazonaws.com\",\n"
            + "  \"aws.cloudtrail.eventName\": \"ReplicateObject\",\n"
            + "  \"aws.cloudtrail.eventTime\": 1,\n"
            + "  \"timestamp\": "
            + System.currentTimeMillis()
            + "\n"
            + "}";
    }
}
