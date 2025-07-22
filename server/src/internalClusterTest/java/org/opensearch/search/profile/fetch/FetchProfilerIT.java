/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile.fetch;

import org.apache.lucene.tests.util.English;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchType;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.index.query.InnerHitBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.opensearch.search.profile.ProfileResult;
import org.opensearch.search.profile.ProfileShardResult;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class FetchProfilerIT extends OpenSearchIntegTestCase {

    @Override
    protected int numberOfShards() {
        return 1; // Use a single shard to ensure all documents are in one shard
    }

    /**
     * This test verifies that the fetch profiler returns reasonable results for a simple match_all query
     */
    public void testRootProfile() throws Exception {
        createIndex("test");

        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex("test").setId(String.valueOf(i)).setSource("field1", English.intToEnglish(i), "field2", i);
        }

        indexRandom(true, docs);
        ensureGreen();

        QueryBuilder q = QueryBuilders.matchAllQuery();

        SearchResponse resp = client().prepareSearch("test")
            .setQuery(q)
            .setProfile(true)
            .setSize(numDocs)
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .get();

        Map<String, ProfileShardResult> profileResults = resp.getProfileResults();
        assertNotNull(profileResults);
        assertFalse("Profile response should not be an empty array", profileResults.isEmpty());

        for (Map.Entry<String, ProfileShardResult> shardResult : profileResults.entrySet()) {
            FetchProfileShardResult fetchProfileResult = shardResult.getValue().getFetchProfileResult();
            assertNotNull("Fetch profile result should not be null", fetchProfileResult);

            List<ProfileResult> fetchProfileResults = fetchProfileResult.getFetchProfileResults();
            assertNotNull("Fetch profile results should not be null", fetchProfileResults);
            assertFalse("Should have at least one fetch profile result", fetchProfileResults.isEmpty());

            for (ProfileResult fetchResult : fetchProfileResults) {
                Map<String, Long> breakdown = fetchResult.getTimeBreakdown();
                assertNotNull("Time breakdown should not be null", breakdown);

                assertTrue(
                    "CREATE_STORED_FIELDS_VISITOR timing should be present",
                    breakdown.containsKey(FetchTimingType.CREATE_STORED_FIELDS_VISITOR.toString())
                );
                assertTrue(
                    "BUILD_SUB_PHASE_PROCESSORS timing should be present",
                    breakdown.containsKey(FetchTimingType.BUILD_SUB_PHASE_PROCESSORS.toString())
                );
                assertTrue("NEXT_READER timing should be present", breakdown.containsKey(FetchTimingType.NEXT_READER.toString()));
                assertTrue(
                    "LOAD_STORED_FIELDS timing should be present",
                    breakdown.containsKey(FetchTimingType.LOAD_STORED_FIELDS.toString())
                );
                assertTrue("LOAD_SOURCE timing should be present", breakdown.containsKey(FetchTimingType.LOAD_SOURCE.toString()));
                assertTrue(
                    "BUILD_SEARCH_HITS timing should be present",
                    breakdown.containsKey(FetchTimingType.BUILD_SEARCH_HITS.toString())
                );
            }
        }
        assertFetchPhase(resp, "FetchSourcePhase", 1);
    }

    public void testExplainProfile() throws Exception {
        createIndex("test");

        int numDocs = randomIntBetween(10, 20);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex("test").setId(String.valueOf(i)).setSource("field1", English.intToEnglish(i), "field2", i);
        }

        indexRandom(true, docs);
        ensureGreen();

        SearchResponse resp = client().prepareSearch("test")
            .setQuery(QueryBuilders.matchAllQuery())
            .setProfile(true)
            .setExplain(true)
            .setSize(numDocs)
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .get();

        assertNotNull("Response should include explanations", resp.getHits().getAt(0).getExplanation());
        assertFetchPhase(resp, "ExplainPhase", 2);
    }

    public void testDocValuesPhaseProfile() throws Exception {
        createIndex("test");
        int numDocs = randomIntBetween(10, 20);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex("test").setId(String.valueOf(i)).setSource("field1", English.intToEnglish(i), "field2", i);
        }
        indexRandom(true, docs);

        SearchResponse resp = client().prepareSearch("test")
            .setQuery(QueryBuilders.matchAllQuery())
            .setProfile(true)
            .addDocValueField("field2")
            .setSize(numDocs)
            .get();

        assertNotNull(resp.getHits().getAt(0).field("field2"));
        assertFetchPhase(resp, "FetchDocValuesPhase", 2);
    }

    public void testFieldsPhaseProfile() throws Exception {
        client().admin()
            .indices()
            .prepareCreate("test")
            .setMapping(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject("field1")
                    .field("type", "text")
                    .field("store", true)
                    .endObject()
                    .startObject("field2")
                    .field("type", "integer")
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .get();
        ensureGreen("test");

        int numDocs = randomIntBetween(10, 20);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex("test").setId(String.valueOf(i)).setSource("field1", English.intToEnglish(i), "field2", i);
        }
        indexRandom(true, docs);

        SearchResponse resp = client().prepareSearch("test")
            .setQuery(QueryBuilders.matchAllQuery())
            .setProfile(true)
            .addFetchField("field1")
            .setSize(numDocs)
            .get();

        assertNotNull(resp.getHits().getAt(0).field("field1"));
        assertFetchPhase(resp, "FetchFieldsPhase", 2);
    }

    public void testVersionPhaseProfile() throws Exception {
        createIndex("test");
        int numDocs = randomIntBetween(10, 20);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex("test").setId(String.valueOf(i)).setSource("field1", English.intToEnglish(i));
        }
        indexRandom(true, docs);

        SearchResponse resp = client().prepareSearch("test")
            .setQuery(QueryBuilders.matchAllQuery())
            .setProfile(true)
            .setVersion(true)
            .setSize(numDocs)
            .get();

        assertEquals(1L, resp.getHits().getAt(0).getVersion());
        assertFetchPhase(resp, "FetchVersionPhase", 2);
    }

    public void testSeqNoPrimaryTermPhaseProfile() throws Exception {
        createIndex("test");
        int numDocs = randomIntBetween(10, 20);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex("test").setId(String.valueOf(i)).setSource("field1", English.intToEnglish(i));
        }
        indexRandom(true, docs);

        SearchResponse resp = client().prepareSearch("test")
            .setQuery(QueryBuilders.matchAllQuery())
            .setProfile(true)
            .seqNoAndPrimaryTerm(true)
            .setSize(numDocs)
            .get();

        assertTrue(resp.getHits().getAt(0).getSeqNo() > -1L);
        assertTrue(resp.getHits().getAt(0).getPrimaryTerm() > 0L);
        assertFetchPhase(resp, "SeqNoPrimaryTermPhase", 2);
    }

    public void testMatchedQueriesPhaseProfile() throws Exception {
        createIndex("test");
        client().prepareIndex("test").setId("1").setSource("field1", "The quick brown fox").get();
        refresh();

        QueryBuilder q = QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery("field1", "quick").queryName("first_query"))
            .should(QueryBuilders.termQuery("field1", "fox").queryName("second_query"));

        SearchResponse resp = client().prepareSearch("test").setQuery(q).setProfile(true).get();

        List<String> matchedQueries = Arrays.asList(resp.getHits().getAt(0).getMatchedQueries());
        assertTrue(matchedQueries.contains("first_query"));
        assertTrue(matchedQueries.contains("second_query"));
        assertFetchPhase(resp, "MatchedQueriesPhase", 2);
    }

    public void testHighlightPhaseProfile() throws Exception {
        createIndex("test");
        client().prepareIndex("test").setId("1").setSource("field1", "The quick brown fox jumps over the lazy dog").get();
        refresh();

        QueryBuilder q = QueryBuilders.matchQuery("field1", "quick fox");
        HighlightBuilder highlighter = new HighlightBuilder().field("field1");

        SearchResponse resp = client().prepareSearch("test").setQuery(q).setProfile(true).highlighter(highlighter).get();

        assertNotNull(resp.getHits().getAt(0).getHighlightFields().get("field1"));
        assertFetchPhase(resp, "HighlightPhase", 2);
    }

    public void testFetchScorePhaseProfile() throws Exception {
        createIndex("test");
        client().prepareIndex("test").setId("1").setSource("field1", "The quick brown fox", "field2", 42).get();
        refresh();

        SearchResponse resp = client().prepareSearch("test")
            .setQuery(QueryBuilders.matchQuery("field1", "quick"))
            .setProfile(true)
            .setTrackScores(true)
            .addSort("field2", SortOrder.ASC) // Sort by a field other than _score
            .get();

        assertTrue(resp.getHits().getAt(0).getScore() > 0.0f);

        assertFetchPhase(resp, "FetchScorePhase", 2);
    }

    public void testInnerHitsPhaseProfile() throws Exception {
        client().admin()
            .indices()
            .prepareCreate("test")
            .setMapping(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject("nested_field")
                    .field("type", "nested")
                    .startObject("properties")
                    .startObject("nested_text")
                    .field("type", "text")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .get();
        ensureGreen("test");

        // Index a document with nested fields
        client().prepareIndex("test")
            .setId("1")
            .setSource(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startArray("nested_field")
                    .startObject()
                    .field("nested_text", "first nested value")
                    .endObject()
                    .startObject()
                    .field("nested_text", "second nested value")
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();
        refresh();

        SearchResponse resp = client().prepareSearch("test")
            .setQuery(
                QueryBuilders.nestedQuery("nested_field", QueryBuilders.matchAllQuery(), org.apache.lucene.search.join.ScoreMode.None)
                    .innerHit(new InnerHitBuilder())
            )
            .setProfile(true)
            .get();

        assertFalse(resp.getHits().getAt(0).getInnerHits().isEmpty());

        assertFetchPhase(resp, "InnerHitsPhase", 2);

        Map<String, ProfileShardResult> profileResults = resp.getProfileResults();
        for (ProfileShardResult shardResult : profileResults.values()) {
            FetchProfileShardResult fetchProfileResult = shardResult.getFetchProfileResult();

            for (ProfileResult fetchResult : fetchProfileResult.getFetchProfileResults()) {
                for (ProfileResult phase : fetchResult.getProfiledChildren()) {
                    if ("InnerHitsPhase".equals(phase.getQueryName())) {

                        assertFalse("InnerHitsPhase should have children", phase.getProfiledChildren().isEmpty());

                        for (ProfileResult innerHitsChild : phase.getProfiledChildren()) {
                            assertEquals("fetch_inner_hits", innerHitsChild.getQueryName());

                            assertEquals("Should have 1 profiled child", 1, innerHitsChild.getProfiledChildren().size());

                            assertEquals("FetchSourcePhase", innerHitsChild.getProfiledChildren().getFirst().getQueryName());
                        }
                    }
                }
            }
        }
    }

    private void assertFetchPhase(SearchResponse resp, String phaseName, int expectedChildren) {
        Map<String, ProfileShardResult> profileResults = resp.getProfileResults();
        assertNotNull(profileResults);
        assertFalse(profileResults.isEmpty());

        boolean foundPhase = false;
        for (Map.Entry<String, ProfileShardResult> shardResult : profileResults.entrySet()) {
            FetchProfileShardResult fetchProfileResult = shardResult.getValue().getFetchProfileResult();
            assertNotNull(fetchProfileResult);

            for (ProfileResult fetchResult : fetchProfileResult.getFetchProfileResults()) {
                assertEquals(
                    "Should have " + expectedChildren + " profiled children",
                    expectedChildren,
                    fetchResult.getProfiledChildren().size()
                );
                for (ProfileResult child : fetchResult.getProfiledChildren()) {
                    if (phaseName.equals(child.getQueryName())) {
                        foundPhase = true;
                        Map<String, Long> breakdown = child.getTimeBreakdown();
                        assertTrue(
                            phaseName + " should have PROCESS timing type",
                            breakdown.containsKey(FetchTimingType.PROCESS.toString())
                        );
                        assertTrue(
                            phaseName + " should have NEXT_READER timing type",
                            breakdown.containsKey(FetchTimingType.NEXT_READER.toString())
                        );
                    }
                }
                if (foundPhase) break;
            }
            if (foundPhase) break;
        }
        assertTrue(phaseName + " should be present in the profile", foundPhase);
    }
}
