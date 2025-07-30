/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline.common;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class AclRoutingSearchProcessorTests extends OpenSearchTestCase {

    public void testProcessRequestWithTermQuery() throws Exception {
        SearchRequest request = createSearchRequest();
        request.source().query(QueryBuilders.termQuery("acl_group", "team-alpha"));

        AclRoutingSearchProcessor processor = new AclRoutingSearchProcessor(null, null, false, "acl_group", true);

        SearchRequest result = processor.processRequest(request);

        assertThat(result.routing(), notNullValue());
    }

    public void testProcessRequestWithTermsQuery() throws Exception {
        SearchRequest request = createSearchRequest();
        request.source().query(QueryBuilders.termsQuery("acl_group", "team-alpha", "team-beta"));

        AclRoutingSearchProcessor processor = new AclRoutingSearchProcessor(null, null, false, "acl_group", true);

        SearchRequest result = processor.processRequest(request);

        assertThat(result.routing(), notNullValue());
    }

    public void testProcessRequestWithBoolQuery() throws Exception {
        SearchRequest request = createSearchRequest();
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("acl_group", "team-alpha"))
            .filter(QueryBuilders.termQuery("acl_group", "team-beta"));
        request.source().query(boolQuery);

        AclRoutingSearchProcessor processor = new AclRoutingSearchProcessor(null, null, false, "acl_group", true);

        SearchRequest result = processor.processRequest(request);

        assertThat(result.routing(), notNullValue());
    }

    public void testProcessRequestNoAclInQuery() throws Exception {
        SearchRequest request = createSearchRequest();
        request.source().query(QueryBuilders.termQuery("other_field", "value"));

        AclRoutingSearchProcessor processor = new AclRoutingSearchProcessor(null, null, false, "acl_group", true);

        SearchRequest result = processor.processRequest(request);

        assertThat(result.routing(), nullValue());
    }

    public void testProcessRequestNoQuery() throws Exception {
        SearchRequest request = createSearchRequest();

        AclRoutingSearchProcessor processor = new AclRoutingSearchProcessor(null, null, false, "acl_group", true);

        SearchRequest result = processor.processRequest(request);

        assertThat(result, equalTo(request));
        assertThat(result.routing(), nullValue());
    }

    public void testProcessRequestNoSource() throws Exception {
        SearchRequest request = new SearchRequest();

        AclRoutingSearchProcessor processor = new AclRoutingSearchProcessor(null, null, false, "acl_group", true);

        SearchRequest result = processor.processRequest(request);

        assertThat(result, equalTo(request));
    }

    public void testProcessRequestExtractDisabled() throws Exception {
        SearchRequest request = createSearchRequest();
        request.source().query(QueryBuilders.termQuery("acl_group", "team-alpha"));

        AclRoutingSearchProcessor processor = new AclRoutingSearchProcessor(null, null, false, "acl_group", false);

        SearchRequest result = processor.processRequest(request);

        assertThat(result.routing(), nullValue());
    }

    public void testConsistentRoutingGeneration() throws Exception {
        String aclValue = "team-alpha";

        SearchRequest request1 = createSearchRequest();
        request1.source().query(QueryBuilders.termQuery("acl_group", aclValue));

        SearchRequest request2 = createSearchRequest();
        request2.source().query(QueryBuilders.termQuery("acl_group", aclValue));

        AclRoutingSearchProcessor processor = new AclRoutingSearchProcessor(null, null, false, "acl_group", true);

        processor.processRequest(request1);
        processor.processRequest(request2);

        assertThat(request1.routing(), equalTo(request2.routing()));
    }

    public void testFactoryCreation() throws Exception {
        AclRoutingSearchProcessor.Factory factory = new AclRoutingSearchProcessor.Factory();

        Map<String, Object> config = new HashMap<>();
        config.put("acl_field", "acl_group");

        AclRoutingSearchProcessor processor = factory.create(null, null, null, false, config, null);
        assertThat(processor.getType(), equalTo(AclRoutingSearchProcessor.TYPE));
    }

    public void testFactoryCreationWithAllParams() throws Exception {
        AclRoutingSearchProcessor.Factory factory = new AclRoutingSearchProcessor.Factory();

        Map<String, Object> config = new HashMap<>();
        config.put("acl_field", "team_id");
        config.put("extract_from_query", false);

        AclRoutingSearchProcessor processor = factory.create(null, null, null, false, config, null);
        assertThat(processor.getType(), equalTo(AclRoutingSearchProcessor.TYPE));
    }

    public void testBoolQueryWithShouldClauses() throws Exception {
        SearchRequest request = createSearchRequest();
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery("acl_group", "team-alpha"))
            .should(QueryBuilders.termQuery("acl_group", "team-beta"))
            .minimumShouldMatch(1);
        request.source().query(boolQuery);

        AclRoutingSearchProcessor processor = new AclRoutingSearchProcessor(null, null, false, "acl_group", true);

        SearchRequest result = processor.processRequest(request);

        assertThat(result.routing(), notNullValue());
    }

    public void testNestedBoolQuery() throws Exception {
        SearchRequest request = createSearchRequest();
        BoolQueryBuilder innerBool = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("acl_group", "team-alpha"));
        BoolQueryBuilder outerBool = QueryBuilders.boolQuery().must(innerBool).filter(QueryBuilders.termQuery("acl_group", "team-beta"));
        request.source().query(outerBool);

        AclRoutingSearchProcessor processor = new AclRoutingSearchProcessor(null, null, false, "acl_group", true);

        SearchRequest result = processor.processRequest(request);

        assertThat(result.routing(), notNullValue());
    }

    public void testFactoryCreationMissingAclField() {
        AclRoutingSearchProcessor.Factory factory = new AclRoutingSearchProcessor.Factory();

        Map<String, Object> config = new HashMap<>();

        Exception e = expectThrows(Exception.class, () -> factory.create(null, null, null, false, config, null));
        assertTrue(e.getMessage().contains("acl_field"));
    }

    public void testGetType() {
        AclRoutingSearchProcessor processor = new AclRoutingSearchProcessor("tag", "description", false, "acl_field", true);
        assertThat(processor.getType(), equalTo("acl_routing_search"));
    }

    public void testBoolQueryWithMustNot() throws Exception {
        SearchRequest request = createSearchRequest();
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("status", "active"))
            .mustNot(QueryBuilders.termQuery("acl_group", "team-alpha"));
        request.source().query(boolQuery);

        AclRoutingSearchProcessor processor = new AclRoutingSearchProcessor(null, null, false, "acl_group", true);

        SearchRequest result = processor.processRequest(request);

        assertThat(result.routing(), notNullValue());
    }

    public void testEmptyTermsQuery() throws Exception {
        SearchRequest request = createSearchRequest();
        request.source().query(QueryBuilders.termsQuery("acl_group", new Object[] {}));

        AclRoutingSearchProcessor processor = new AclRoutingSearchProcessor(null, null, false, "acl_group", true);

        SearchRequest result = processor.processRequest(request);

        assertThat(result.routing(), nullValue());
    }

    public void testHashingConsistency() throws Exception {
        String aclValue = "team-production";

        SearchRequest request1 = createSearchRequest();
        request1.source().query(QueryBuilders.termQuery("acl_group", aclValue));

        SearchRequest request2 = createSearchRequest();
        request2.source().query(QueryBuilders.termQuery("acl_group", aclValue));

        AclRoutingSearchProcessor processor = new AclRoutingSearchProcessor(null, null, false, "acl_group", true);

        processor.processRequest(request1);
        processor.processRequest(request2);

        assertThat(request1.routing(), equalTo(request2.routing()));
    }

    private SearchRequest createSearchRequest() {
        SearchRequest request = new SearchRequest();
        request.source(new SearchSourceBuilder());
        return request;
    }
}
