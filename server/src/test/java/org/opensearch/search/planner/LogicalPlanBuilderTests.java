/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.planner;

import org.opensearch.index.IndexService;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.planner.nodes.BooleanPlanNode;
import org.opensearch.search.planner.nodes.GenericPlanNode;
import org.opensearch.search.planner.nodes.MatchPlanNode;
import org.opensearch.search.planner.nodes.RangePlanNode;
import org.opensearch.search.planner.nodes.TermPlanNode;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class LogicalPlanBuilderTests extends OpenSearchSingleNodeTestCase {

    private QueryShardContext queryShardContext;
    private LogicalPlanBuilder planBuilder;
    private IndexService indexService;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        // Create index with mappings
        indexService = createIndex(
            "test",
            org.opensearch.common.settings.Settings.EMPTY,
            "properties",
            "status",
            "type=keyword",
            "price",
            "type=long",
            "description",
            "type=text",
            "title",
            "type=text",
            "category",
            "type=keyword",
            "brand",
            "type=keyword",
            "deleted",
            "type=boolean"
        );
        ensureGreen();

        // Get query shard context from the index
        queryShardContext = createSearchContext(indexService).getQueryShardContext();
        planBuilder = new LogicalPlanBuilder(queryShardContext);
    }

    public void testBuildFromTermQueryBuilderProducesTermPlanNode() throws Exception {
        TermQueryBuilder termQuery = QueryBuilders.termQuery("status", "active");

        QueryPlanNode node = planBuilder.build(termQuery);

        assertThat(node, instanceOf(TermPlanNode.class));
        assertThat(node.getType(), equalTo(QueryNodeType.TERM));

        TermPlanNode termNode = (TermPlanNode) node;
        assertThat(termNode.getField(), equalTo("status"));
        assertThat(termNode.getValue(), equalTo("active"));
    }

    public void testBuildFromRangeQueryBuilderProducesRangePlanNode() throws Exception {
        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("price").from(100).to(1000).includeLower(true).includeUpper(false);

        QueryPlanNode node = planBuilder.build(rangeQuery);

        assertThat(node, instanceOf(RangePlanNode.class));
        assertThat(node.getType(), equalTo(QueryNodeType.RANGE));

        RangePlanNode rangeNode = (RangePlanNode) node;
        assertThat(rangeNode.getField(), equalTo("price"));

        // Check profile has the attributes
        var profile = rangeNode.getProfile();
        assertThat(profile.getAttributes().get("field"), equalTo("price"));
        assertThat(profile.getAttributes().get("from"), equalTo("100"));
        assertThat(profile.getAttributes().get("to"), equalTo("1000"));
        assertThat(profile.getAttributes().get("include_from"), equalTo(true));
        assertThat(profile.getAttributes().get("include_to"), equalTo(false));
    }

    public void testBuildFromMatchQueryBuilderProducesMatchPlanNode() throws Exception {
        MatchQueryBuilder matchQuery = QueryBuilders.matchQuery("description", "quick brown fox").analyzer("standard");

        QueryPlanNode node = planBuilder.build(matchQuery);

        assertThat(node, instanceOf(MatchPlanNode.class));
        assertThat(node.getType(), equalTo(QueryNodeType.MATCH));

        MatchPlanNode matchNode = (MatchPlanNode) node;
        assertThat(matchNode.getField(), equalTo("description"));
        assertThat(matchNode.getText(), equalTo("quick brown fox"));

        // Check profile
        var profile = matchNode.getProfile();
        assertThat(profile.getAttributes().get("field"), equalTo("description"));
        assertThat(profile.getAttributes().get("text"), equalTo("quick brown fox"));
        assertThat(profile.getAttributes().get("analyzer"), equalTo("standard"));
        assertTrue((int) profile.getAttributes().get("term_count") >= 1);
    }

    public void testBuildFromBoolQueryBuilderPreservesClausesAndMinimumShouldMatch() throws Exception {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("status", "active"))
            .filter(QueryBuilders.rangeQuery("price").gte(100))
            .should(QueryBuilders.matchQuery("description", "laptop"))
            .should(QueryBuilders.matchQuery("title", "computer"))
            .mustNot(QueryBuilders.termQuery("deleted", true))
            .minimumShouldMatch("1");

        QueryPlanNode node = planBuilder.build(boolQuery);

        assertThat(node, instanceOf(BooleanPlanNode.class));
        assertThat(node.getType(), equalTo(QueryNodeType.BOOLEAN));

        BooleanPlanNode boolNode = (BooleanPlanNode) node;
        assertThat(boolNode.getMustClauses().size(), equalTo(1));
        assertThat(boolNode.getFilterClauses().size(), equalTo(1));
        assertThat(boolNode.getShouldClauses().size(), equalTo(2));
        assertThat(boolNode.getMustNotClauses().size(), equalTo(1));

        // Check child types
        assertThat(boolNode.getMustClauses().get(0), instanceOf(TermPlanNode.class));
        assertThat(boolNode.getFilterClauses().get(0), instanceOf(RangePlanNode.class));
        assertThat(boolNode.getShouldClauses().get(0), instanceOf(MatchPlanNode.class));
        assertThat(boolNode.getMustNotClauses().get(0), instanceOf(TermPlanNode.class));

        // Check minimum should match is preserved
        var profile = boolNode.getProfile();
        assertThat(profile.getAttributes().get("minimum_should_match"), equalTo(1));
    }

    public void testUnknownBuilderFallsBackToGenericNode() throws Exception {
        // Create a custom query builder that's not handled
        QueryBuilder customQuery = QueryBuilders.wildcardQuery("status", "val*");

        QueryPlanNode node = planBuilder.build(customQuery);

        // Should produce GenericPlanNode with WILDCARD type
        assertThat(node, instanceOf(GenericPlanNode.class));
        assertThat(node.getType(), equalTo(QueryNodeType.WILDCARD));
    }

    public void testNestedBoolQueryPreservesStructure() throws Exception {
        BoolQueryBuilder innerBool = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("category", "electronics"))
            .must(QueryBuilders.termQuery("brand", "apple"));

        BoolQueryBuilder outerBool = QueryBuilders.boolQuery().must(innerBool).filter(QueryBuilders.rangeQuery("price").lte(2000));

        QueryPlanNode node = planBuilder.build(outerBool);

        assertThat(node, instanceOf(BooleanPlanNode.class));
        BooleanPlanNode outerNode = (BooleanPlanNode) node;
        assertThat(outerNode.getMustClauses().size(), equalTo(1));
        assertThat(outerNode.getFilterClauses().size(), equalTo(1));

        // Check nested structure
        assertThat(outerNode.getMustClauses().get(0), instanceOf(BooleanPlanNode.class));
        BooleanPlanNode innerNode = (BooleanPlanNode) outerNode.getMustClauses().get(0);
        assertThat(innerNode.getMustClauses().size(), equalTo(2));

        // Both inner clauses should be term queries
        assertThat(innerNode.getMustClauses().get(0), instanceOf(TermPlanNode.class));
        assertThat(innerNode.getMustClauses().get(1), instanceOf(TermPlanNode.class));
    }

    public void testBooleanQueryMinimumShouldMatchPercentage() throws Exception {
        BoolQueryBuilder query = QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery("status", "active"))
            .should(QueryBuilders.termQuery("status", "inactive"))
            .should(QueryBuilders.termQuery("category", "electronics"))
            .should(QueryBuilders.termQuery("category", "clothing"))
            .minimumShouldMatch("50%");

        QueryPlanNode plan = planBuilder.build(query);

        assertNotNull(plan);
        assertTrue(plan instanceof BooleanPlanNode);
        BooleanPlanNode boolNode = (BooleanPlanNode) plan;

        // With 4 should clauses and 50%, minimum should be 2
        QueryPlanProfile profile = plan.getProfile();
        assertEquals(Integer.valueOf(2), profile.getAttributes().get("minimum_should_match"));
    }
}
