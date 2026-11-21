/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search;

import org.apache.lucene.util.BytesRef;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.indices.TermsLookup;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class SearchProtoConverterTests extends OpenSearchTestCase {

    public void testIndices_areEmittedInOrder() {
        SearchRequest req = new SearchRequest("a", "b", "c");
        var proto = SearchProtoConverter.toProto(req);
        assertEquals(List.of("a", "b", "c"), proto.getIndexList());
    }

    public void testFromAndSize_round_trip() {
        SearchRequest req = new SearchRequest("idx").source(
            new SearchSourceBuilder().from(10).size(25).query(QueryBuilders.matchAllQuery())
        );
        var body = SearchProtoConverter.toProto(req).getSearchRequestBody();
        assertEquals(10, body.getFrom());
        assertEquals(25, body.getSize());
    }

    public void testMatchAllQuery() {
        var container = queryContainer(QueryBuilders.matchAllQuery());
        assertTrue(container.hasMatchAll());
    }

    public void testMatchAllQuery_boostAndName() {
        MatchAllQueryBuilder q = QueryBuilders.matchAllQuery().boost(2.5f).queryName("named");
        var container = queryContainer(q);
        assertEquals(2.5f, container.getMatchAll().getBoost(), 0.0001f);
        assertEquals("named", container.getMatchAll().getXName());
    }

    public void testMatchNoneQuery() {
        var container = queryContainer(new MatchNoneQueryBuilder());
        assertTrue(container.hasMatchNone());
    }

    public void testTermQuery_string() {
        TermQueryBuilder q = QueryBuilders.termQuery("status", "active");
        var container = queryContainer(q);
        assertTrue(container.hasTerm());
        assertEquals("status", container.getTerm().getField());
        assertEquals("active", container.getTerm().getValue().getString());
    }

    public void testTermQuery_long() {
        var container = queryContainer(QueryBuilders.termQuery("n", 42L));
        assertEquals(42L, container.getTerm().getValue().getGeneralNumber().getInt64Value());
    }

    public void testTermQuery_caseInsensitiveOnlyEmittedWhenTrue() {
        TermQueryBuilder q1 = QueryBuilders.termQuery("f", "v");
        assertFalse(queryContainer(q1).getTerm().getCaseInsensitive());
        TermQueryBuilder q2 = QueryBuilders.termQuery("f", "v").caseInsensitive(true);
        assertTrue(queryContainer(q2).getTerm().getCaseInsensitive());
    }

    public void testTermQuery_bytesRefIsUnwrappedToString() {
        TermQueryBuilder q = QueryBuilders.termQuery("f", new BytesRef("alice"));
        assertEquals("alice", queryContainer(q).getTerm().getValue().getString());
    }

    public void testTermsQuery_inlineValues() {
        TermsQueryBuilder q = new TermsQueryBuilder("color", List.of("red", "green", "blue"));
        var container = queryContainer(q);
        assertTrue(container.hasTerms());
        assertEquals(1, container.getTerms().getTermsCount());
        var field = container.getTerms().getTermsOrThrow("color");
        assertTrue(field.hasValue());
        assertEquals(3, field.getValue().getFieldValueArrayCount());
        assertEquals("red", field.getValue().getFieldValueArray(0).getString());
    }

    public void testTermsQuery_lookup() {
        TermsLookup lookup = new TermsLookup("users", "u1", "tags");
        lookup.routing("r");
        TermsQueryBuilder q = new TermsQueryBuilder("user", lookup);
        var container = queryContainer(q);
        var field = container.getTerms().getTermsOrThrow("user");
        assertTrue(field.hasLookup());
        assertEquals("users", field.getLookup().getIndex());
        assertEquals("u1", field.getLookup().getId());
        assertEquals("tags", field.getLookup().getPath());
        assertEquals("r", field.getLookup().getRouting());
    }

    public void testUnsupportedQueryType_throws() {
        SearchRequest req = new SearchRequest("idx").source(new SearchSourceBuilder().query(new RangeQueryBuilder("n").gte(0)));
        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class, () -> SearchProtoConverter.toProto(req));
        assertTrue(e.getMessage().contains("RangeQueryBuilder"));
    }

    private static QueryContainer queryContainer(org.opensearch.index.query.QueryBuilder q) {
        SearchRequest req = new SearchRequest("idx").source(new SearchSourceBuilder().query(q));
        return SearchProtoConverter.toProto(req).getSearchRequestBody().getQuery();
    }
}
