/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a.java
 * compatible open source license.
 */

package org.opensearch.search.pipeline.common;

import org.apache.lucene.search.TotalHits;
import org.opensearch.OpenSearchParseException;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.common.document.DocumentField;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.ingest.RandomDocumentPicks;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SortResponseProcessorTests extends OpenSearchTestCase {

    private static final List<Object> PI = List.of(3, 1, 4, 1, 5, 9, 2, 6);
    private static final List<Object> E = List.of(2, 7, 1, 8, 2, 8, 1, 8);
    private static final List<Object> X;
    static {
        List<Object> x = new ArrayList<>();
        x.add(1);
        x.add(null);
        x.add(3);
        X = x;
    }

    private SearchRequest createDummyRequest() {
        QueryBuilder query = new TermQueryBuilder("field", "value");
        SearchSourceBuilder source = new SearchSourceBuilder().query(query);
        return new SearchRequest().source(source);
    }

    private SearchResponse createTestResponse() {
        SearchHit[] hits = new SearchHit[2];

        // one response with source
        Map<String, DocumentField> piMap = new HashMap<>();
        piMap.put("digits", new DocumentField("digits", PI));
        hits[0] = new SearchHit(0, "doc 1", piMap, Collections.emptyMap());
        hits[0].sourceRef(new BytesArray("{ \"digits\" : " + PI + " }"));
        hits[0].score((float) Math.PI);

        // one without source
        Map<String, DocumentField> eMap = new HashMap<>();
        eMap.put("digits", new DocumentField("digits", E));
        hits[1] = new SearchHit(1, "doc 2", eMap, Collections.emptyMap());
        hits[1].score((float) Math.E);

        SearchHits searchHits = new SearchHits(hits, new TotalHits(2, TotalHits.Relation.EQUAL_TO), 2);
        SearchResponseSections searchResponseSections = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        return new SearchResponse(searchResponseSections, null, 1, 1, 0, 10, null, null);
    }

    private SearchResponse createTestResponseNullField() {
        SearchHit[] hits = new SearchHit[1];

        Map<String, DocumentField> map = new HashMap<>();
        map.put("digits", null);
        hits[0] = new SearchHit(0, "doc 1", map, Collections.emptyMap());
        hits[0].sourceRef(new BytesArray("{ \"digits\" : null }"));
        hits[0].score((float) Math.PI);

        SearchHits searchHits = new SearchHits(hits, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1);
        SearchResponseSections searchResponseSections = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        return new SearchResponse(searchResponseSections, null, 1, 1, 0, 10, null, null);
    }

    private SearchResponse createTestResponseNullListEntry() {
        SearchHit[] hits = new SearchHit[1];

        Map<String, DocumentField> xMap = new HashMap<>();
        xMap.put("digits", new DocumentField("digits", X));
        hits[0] = new SearchHit(0, "doc 1", xMap, Collections.emptyMap());
        hits[0].sourceRef(new BytesArray("{ \"digits\" : " + X + " }"));
        hits[0].score((float) Math.PI);

        SearchHits searchHits = new SearchHits(hits, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1);
        SearchResponseSections searchResponseSections = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        return new SearchResponse(searchResponseSections, null, 1, 1, 0, 10, null, null);
    }

    private SearchResponse createTestResponseNotComparable() {
        SearchHit[] hits = new SearchHit[1];

        Map<String, DocumentField> piMap = new HashMap<>();
        piMap.put("maps", new DocumentField("maps", List.of(Map.of("foo", "I'm incomparable!"))));
        hits[0] = new SearchHit(0, "doc 1", piMap, Collections.emptyMap());
        hits[0].sourceRef(new BytesArray("{ \"maps\" : [{ \"foo\" : \"I'm incomparable!\"}]] }"));
        hits[0].score((float) Math.PI);

        SearchHits searchHits = new SearchHits(hits, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1);
        SearchResponseSections searchResponseSections = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        return new SearchResponse(searchResponseSections, null, 1, 1, 0, 10, null, null);
    }

    public void testSortResponse() throws Exception {
        SearchRequest request = createDummyRequest();

        SortResponseProcessor sortResponseProcessor = new SortResponseProcessor(
            null,
            null,
            false,
            "digits",
            SortResponseProcessor.SortOrder.ASCENDING,
            "sorted"
        );
        SearchResponse response = createTestResponse();
        SearchResponse sortResponse = sortResponseProcessor.processResponse(request, response);

        assertEquals(response.getHits(), sortResponse.getHits());

        assertEquals(PI, sortResponse.getHits().getHits()[0].field("digits").getValues());
        assertEquals(List.of(1, 1, 2, 3, 4, 5, 6, 9), sortResponse.getHits().getHits()[0].field("sorted").getValues());
        Map<String, Object> map = sortResponse.getHits().getHits()[0].getSourceAsMap();
        assertNotNull(map);
        assertEquals(List.of(1, 1, 2, 3, 4, 5, 6, 9), map.get("sorted"));

        assertEquals(E, sortResponse.getHits().getHits()[1].field("digits").getValues());
        assertEquals(List.of(1, 1, 2, 2, 7, 8, 8, 8), sortResponse.getHits().getHits()[1].field("sorted").getValues());
        assertNull(sortResponse.getHits().getHits()[1].getSourceAsMap());
    }

    public void testSortResponseSameField() throws Exception {
        SearchRequest request = createDummyRequest();

        SortResponseProcessor sortResponseProcessor = new SortResponseProcessor(
            null,
            null,
            false,
            "digits",
            SortResponseProcessor.SortOrder.DESCENDING,
            null
        );
        SearchResponse response = createTestResponse();
        SearchResponse sortResponse = sortResponseProcessor.processResponse(request, response);

        assertEquals(response.getHits(), sortResponse.getHits());
        assertEquals(List.of(9, 6, 5, 4, 3, 2, 1, 1), sortResponse.getHits().getHits()[0].field("digits").getValues());
        assertEquals(List.of(8, 8, 8, 7, 2, 2, 1, 1), sortResponse.getHits().getHits()[1].field("digits").getValues());
    }

    public void testSortResponseNullListEntry() {
        SearchRequest request = createDummyRequest();

        SortResponseProcessor sortResponseProcessor = new SortResponseProcessor(
            null,
            null,
            false,
            "digits",
            SortResponseProcessor.SortOrder.ASCENDING,
            null
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> sortResponseProcessor.processResponse(request, createTestResponseNullListEntry())
        );
    }

    public void testNullField() {
        SearchRequest request = createDummyRequest();

        SortResponseProcessor sortResponseProcessor = new SortResponseProcessor(
            null,
            null,
            false,
            "digits",
            SortResponseProcessor.SortOrder.DESCENDING,
            null
        );

        assertThrows(IllegalArgumentException.class, () -> sortResponseProcessor.processResponse(request, createTestResponseNullField()));
    }

    public void testNotComparableField() {
        SearchRequest request = createDummyRequest();

        SortResponseProcessor sortResponseProcessor = new SortResponseProcessor(
            null,
            null,
            false,
            "maps",
            SortResponseProcessor.SortOrder.ASCENDING,
            null
        );

        assertThrows(
            IllegalArgumentException.class,
            () -> sortResponseProcessor.processResponse(request, createTestResponseNotComparable())
        );
    }

    public void testFactory() {
        String sortField = RandomDocumentPicks.randomFieldName(random());
        String targetField = RandomDocumentPicks.randomFieldName(random());
        Map<String, Object> config = new HashMap<>();
        config.put("field", sortField);
        config.put("order", "desc");
        config.put("target_field", targetField);

        SortResponseProcessor.Factory factory = new SortResponseProcessor.Factory();
        SortResponseProcessor processor = factory.create(Collections.emptyMap(), null, null, false, config, null);
        assertEquals("sort", processor.getType());
        assertEquals(sortField, processor.getSortField());
        assertEquals(targetField, processor.getTargetField());
        assertEquals(SortResponseProcessor.SortOrder.DESCENDING, processor.getSortOrder());

        expectThrows(
            OpenSearchParseException.class,
            () -> factory.create(Collections.emptyMap(), null, null, false, Collections.emptyMap(), null)
        );
    }
}
