/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.search.pipeline.common.helpers;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.AbstractBuilderTestCase;

public class SearchRequestMapTests extends AbstractBuilderTestCase {

    public void testEmptyMap() {
        SearchRequest searchRequest = new SearchRequest();
        SearchRequestMap map = new SearchRequestMap(searchRequest);

        assertTrue(map.isEmpty());
    }

    public void testGet() {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.from(10);
        source.size(20);
        source.explain(true);
        source.version(true);
        source.seqNoAndPrimaryTerm(true);
        source.trackScores(true);
        source.trackTotalHitsUpTo(3);
        source.minScore(1.0f);
        source.terminateAfter(5);
        searchRequest.source(source);

        SearchRequestMap map = new SearchRequestMap(searchRequest);

        assertEquals(10, map.get("from"));
        assertEquals(20, map.get("size"));
        assertEquals(true, map.get("explain"));
        assertEquals(true, map.get("version"));
        assertEquals(true, map.get("seq_no_primary_term"));
        assertEquals(true, map.get("track_scores"));
        assertEquals(3, map.get("track_total_hits"));
        assertEquals(1.0f, map.get("min_score"));
        assertEquals(5, map.get("terminate_after"));
    }

    public void testPut() {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder();
        searchRequest.source(source);

        SearchRequestMap map = new SearchRequestMap(searchRequest);

        assertEquals(-1, map.put("from", 10));
        assertEquals(10, map.get("from"));

        assertEquals(-1, map.put("size", 20));
        assertEquals(20, map.get("size"));

        assertNull(map.put("explain", true));
        assertEquals(true, map.get("explain"));

        assertNull(map.put("version", true));
        assertEquals(true, map.get("version"));

        assertNull(map.put("seq_no_primary_term", true));
        assertEquals(true, map.get("seq_no_primary_term"));

        assertEquals(false, map.put("track_scores", true));
        assertEquals(true, map.get("track_scores"));

        assertNull(map.put("track_total_hits", 3));
        assertEquals(3, map.get("track_total_hits"));

        assertNull(map.put("min_score", 1.0f));
        assertEquals(1.0f, map.get("min_score"));

        assertEquals(0, map.put("terminate_after", 5));
        assertEquals(5, map.get("terminate_after"));
    }

    public void testUnsupportedOperationException() {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder();
        searchRequest.source(source);

        SearchRequestMap map = new SearchRequestMap(searchRequest);

        assertThrows(UnsupportedOperationException.class, () -> map.size());
        assertThrows(UnsupportedOperationException.class, () -> map.containsValue(null));
        assertThrows(UnsupportedOperationException.class, () -> map.remove(null));
        assertThrows(UnsupportedOperationException.class, () -> map.putAll(null));
        assertThrows(UnsupportedOperationException.class, map::clear);
        assertThrows(UnsupportedOperationException.class, map::keySet);
        assertThrows(UnsupportedOperationException.class, map::values);
        assertThrows(UnsupportedOperationException.class, map::entrySet);
        assertThrows(UnsupportedOperationException.class, () -> map.getOrDefault(null, null));
        assertThrows(UnsupportedOperationException.class, () -> map.forEach(null));
        assertThrows(UnsupportedOperationException.class, () -> map.replaceAll(null));
        assertThrows(UnsupportedOperationException.class, () -> map.putIfAbsent(null, null));
        assertThrows(UnsupportedOperationException.class, () -> map.remove(null, null));
        assertThrows(UnsupportedOperationException.class, () -> map.replace(null, null, null));
        assertThrows(UnsupportedOperationException.class, () -> map.replace(null, null));
        assertThrows(UnsupportedOperationException.class, () -> map.computeIfAbsent(null, null));
        assertThrows(UnsupportedOperationException.class, () -> map.computeIfPresent(null, null));
        assertThrows(UnsupportedOperationException.class, () -> map.compute(null, null));
        assertThrows(UnsupportedOperationException.class, () -> map.merge(null, null, null));
    }

    public void testIllegalArgumentException() {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder();
        searchRequest.source(source);

        SearchRequestMap map = new SearchRequestMap(searchRequest);

        try {
            map.get(1);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    public void testSearchRequestMapProcessingException() {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder();
        searchRequest.source(source);

        SearchRequestMap map = new SearchRequestMap(searchRequest);

        try {
            map.get("unsupported_key");
            fail("Expected SearchRequestMapProcessingException");
        } catch (SearchRequestMapProcessingException e) {
            // expected
        }

        try {
            map.put("unsupported_key", 10);
            fail("Expected SearchRequestMapProcessingException");
        } catch (SearchRequestMapProcessingException e) {
            // expected
        }
    }
}
