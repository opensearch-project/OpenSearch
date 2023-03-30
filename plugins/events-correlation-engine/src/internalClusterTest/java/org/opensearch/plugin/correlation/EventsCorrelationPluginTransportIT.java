/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation;

import org.apache.lucene.search.join.ScoreMode;
import org.junit.Assert;
import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.action.admin.cluster.node.info.PluginsAndModules;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugin.correlation.rules.action.IndexCorrelationRuleAction;
import org.opensearch.plugin.correlation.rules.action.IndexCorrelationRuleRequest;
import org.opensearch.plugin.correlation.rules.action.IndexCorrelationRuleResponse;
import org.opensearch.plugin.correlation.rules.model.CorrelationQuery;
import org.opensearch.plugin.correlation.rules.model.CorrelationRule;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
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
}
