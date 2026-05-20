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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SplitResponseProcessorTests extends OpenSearchTestCase {

    private static final String NO_TRAILING = "one,two,three";
    private static final String TRAILING = "alpha,beta,gamma,";
    private static final String REGEX_DELIM = "one1two2three";

    private SearchRequest createDummyRequest() {
        QueryBuilder query = new TermQueryBuilder("field", "value");
        SearchSourceBuilder source = new SearchSourceBuilder().query(query);
        return new SearchRequest().source(source);
    }

    private SearchResponse createTestResponse() {
        SearchHit[] hits = new SearchHit[2];

        // one response with source
        Map<String, DocumentField> csvMap = new HashMap<>();
        csvMap.put("csv", new DocumentField("csv", List.of(NO_TRAILING)));
        hits[0] = new SearchHit(0, "doc 1", csvMap, Collections.emptyMap());
        hits[0].sourceRef(new BytesArray("{ \"csv\" : \"" + NO_TRAILING + "\" }"));
        hits[0].score(1f);

        // one without source
        csvMap = new HashMap<>();
        csvMap.put("csv", new DocumentField("csv", List.of(TRAILING)));
        hits[1] = new SearchHit(1, "doc 2", csvMap, Collections.emptyMap());
        hits[1].score(2f);

        SearchHits searchHits = new SearchHits(hits, new TotalHits(2, TotalHits.Relation.EQUAL_TO), 2);
        SearchResponseSections searchResponseSections = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        return new SearchResponse(searchResponseSections, null, 1, 1, 0, 10, null, null);
    }

    private SearchResponse createTestResponseRegex() {
        SearchHit[] hits = new SearchHit[1];

        Map<String, DocumentField> dsvMap = new HashMap<>();
        dsvMap.put("dsv", new DocumentField("dsv", List.of(REGEX_DELIM)));
        hits[0] = new SearchHit(0, "doc 1", dsvMap, Collections.emptyMap());
        hits[0].sourceRef(new BytesArray("{ \"dsv\" : \"" + REGEX_DELIM + "\" }"));
        hits[0].score(1f);

        SearchHits searchHits = new SearchHits(hits, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1);
        SearchResponseSections searchResponseSections = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        return new SearchResponse(searchResponseSections, null, 1, 1, 0, 10, null, null);
    }

    private SearchResponse createTestResponseNullField() {
        SearchHit[] hits = new SearchHit[1];

        Map<String, DocumentField> map = new HashMap<>();
        map.put("csv", null);
        hits[0] = new SearchHit(0, "doc 1", map, Collections.emptyMap());
        hits[0].sourceRef(new BytesArray("{ \"csv\" : null }"));
        hits[0].score(1f);

        SearchHits searchHits = new SearchHits(hits, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1);
        SearchResponseSections searchResponseSections = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        return new SearchResponse(searchResponseSections, null, 1, 1, 0, 10, null, null);
    }

    private SearchResponse createTestResponseEmptyList() {
        SearchHit[] hits = new SearchHit[1];

        Map<String, DocumentField> map = new HashMap<>();
        map.put("empty", new DocumentField("empty", List.of()));
        hits[0] = new SearchHit(0, "doc 1", map, Collections.emptyMap());
        hits[0].sourceRef(new BytesArray("{ \"empty\" : [] }"));
        hits[0].score(1f);

        SearchHits searchHits = new SearchHits(hits, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1);
        SearchResponseSections searchResponseSections = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        return new SearchResponse(searchResponseSections, null, 1, 1, 0, 10, null, null);
    }

    private SearchResponse createTestResponseNotString() {
        SearchHit[] hits = new SearchHit[1];

        Map<String, DocumentField> piMap = new HashMap<>();
        piMap.put("maps", new DocumentField("maps", List.of(Map.of("foo", "I'm the Map!"))));
        hits[0] = new SearchHit(0, "doc 1", piMap, Collections.emptyMap());
        hits[0].sourceRef(new BytesArray("{ \"maps\" : [{ \"foo\" : \"I'm the Map!\"}]] }"));
        hits[0].score(1f);

        SearchHits searchHits = new SearchHits(hits, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1);
        SearchResponseSections searchResponseSections = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        return new SearchResponse(searchResponseSections, null, 1, 1, 0, 10, null, null);
    }

    public void testSplitResponse() throws Exception {
        SearchRequest request = createDummyRequest();

        SplitResponseProcessor splitResponseProcessor = new SplitResponseProcessor(null, null, false, "csv", ",", false, "split");
        SearchResponse response = createTestResponse();
        SearchResponse splitResponse = splitResponseProcessor.processResponse(request, response);

        assertEquals(response.getHits(), splitResponse.getHits());

        assertEquals(NO_TRAILING, splitResponse.getHits().getHits()[0].field("csv").getValue());
        assertEquals(List.of("one", "two", "three"), splitResponse.getHits().getHits()[0].field("split").getValues());
        Map<String, Object> map = splitResponse.getHits().getHits()[0].getSourceAsMap();
        assertNotNull(map);
        assertEquals(List.of("one", "two", "three"), map.get("split"));

        assertEquals(TRAILING, splitResponse.getHits().getHits()[1].field("csv").getValue());
        assertEquals(List.of("alpha", "beta", "gamma"), splitResponse.getHits().getHits()[1].field("split").getValues());
        assertNull(splitResponse.getHits().getHits()[1].getSourceAsMap());
    }

    public void testSplitResponseRegex() throws Exception {
        SearchRequest request = createDummyRequest();

        SplitResponseProcessor splitResponseProcessor = new SplitResponseProcessor(null, null, false, "dsv", "\\d", false, "split");
        SearchResponse response = createTestResponseRegex();
        SearchResponse splitResponse = splitResponseProcessor.processResponse(request, response);

        assertEquals(response.getHits(), splitResponse.getHits());

        assertEquals(REGEX_DELIM, splitResponse.getHits().getHits()[0].field("dsv").getValue());
        assertEquals(List.of("one", "two", "three"), splitResponse.getHits().getHits()[0].field("split").getValues());
        Map<String, Object> map = splitResponse.getHits().getHits()[0].getSourceAsMap();
        assertNotNull(map);
        assertEquals(List.of("one", "two", "three"), map.get("split"));
    }

    public void testSplitResponseSameField() throws Exception {
        SearchRequest request = createDummyRequest();

        SplitResponseProcessor splitResponseProcessor = new SplitResponseProcessor(null, null, false, "csv", ",", true, null);
        SearchResponse response = createTestResponse();
        SearchResponse splitResponse = splitResponseProcessor.processResponse(request, response);

        assertEquals(response.getHits(), splitResponse.getHits());
        assertEquals(List.of("one", "two", "three"), splitResponse.getHits().getHits()[0].field("csv").getValues());
        assertEquals(List.of("alpha", "beta", "gamma", ""), splitResponse.getHits().getHits()[1].field("csv").getValues());
    }

    public void testSplitResponseEmptyList() {
        SearchRequest request = createDummyRequest();

        SplitResponseProcessor splitResponseProcessor = new SplitResponseProcessor(null, null, false, "empty", ",", false, null);
        assertThrows(IllegalArgumentException.class, () -> splitResponseProcessor.processResponse(request, createTestResponseEmptyList()));
    }

    public void testNullField() {
        SearchRequest request = createDummyRequest();

        SplitResponseProcessor splitResponseProcessor = new SplitResponseProcessor(null, null, false, "csv", ",", false, null);

        assertThrows(IllegalArgumentException.class, () -> splitResponseProcessor.processResponse(request, createTestResponseNullField()));
    }

    public void testNotStringField() {
        SearchRequest request = createDummyRequest();

        SplitResponseProcessor splitResponseProcessor = new SplitResponseProcessor(null, null, false, "maps", ",", false, null);

        assertThrows(IllegalArgumentException.class, () -> splitResponseProcessor.processResponse(request, createTestResponseNotString()));
    }

    public void testFactory() {
        String splitField = RandomDocumentPicks.randomFieldName(random());
        String targetField = RandomDocumentPicks.randomFieldName(random());
        Map<String, Object> config = new HashMap<>();
        config.put("field", splitField);
        config.put("separator", ",");
        config.put("preserve_trailing", true);
        config.put("target_field", targetField);

        SplitResponseProcessor.Factory factory = new SplitResponseProcessor.Factory();
        SplitResponseProcessor processor = factory.create(Collections.emptyMap(), null, null, false, config, null);
        assertEquals("split", processor.getType());
        assertEquals(splitField, processor.getSplitField());
        assertEquals(",", processor.getSeparator());
        assertTrue(processor.isPreserveTrailing());
        assertEquals(targetField, processor.getTargetField());

        expectThrows(
            OpenSearchParseException.class,
            () -> factory.create(Collections.emptyMap(), null, null, false, Collections.emptyMap(), null)
        );
    }
}
