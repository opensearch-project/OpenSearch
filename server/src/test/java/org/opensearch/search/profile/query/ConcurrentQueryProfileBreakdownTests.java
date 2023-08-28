/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.profile.query;

import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

public class ConcurrentQueryProfileBreakdownTests extends OpenSearchTestCase {
    private static final String CREATE_WEIGHT = QueryTimingType.CREATE_WEIGHT.toString();
    private static final String BUILD_SCORER = QueryTimingType.BUILD_SCORER.toString();
    private static final String NEXT_DOC = QueryTimingType.NEXT_DOC.toString();
    private static final String ADVANCE = QueryTimingType.ADVANCE.toString();
    private static final String MATCH = QueryTimingType.MATCH.toString();
    private static final String SCORE = QueryTimingType.SCORE.toString();
    private static final String SHALLOW_ADVANCE = QueryTimingType.SHALLOW_ADVANCE.toString();
    private static final String COMPUTE_MAX_SCORE = QueryTimingType.COMPUTE_MAX_SCORE.toString();
    private static final String SET_MIN_COMPETITIVE_SCORE = QueryTimingType.SET_MIN_COMPETITIVE_SCORE.toString();
    private static final String COUNT_SUFFIX = "_count";
    private static final String START_TIME_SUFFIX = "_start_time";
    private static final String MAX_END_TIME_SUFFIX = "_max_end_time";
    private static final String MIN_START_TIME_SUFFIX = "_min_start_time";

    public void testBuildFinalBreakdownMap() {
        Map<String, Long> testMap = new HashMap<>();
        testMap.put(CREATE_WEIGHT, 370343L);
        testMap.put(CREATE_WEIGHT + COUNT_SUFFIX, 1L);
        testMap.put(CREATE_WEIGHT + START_TIME_SUFFIX, 1598347679188617L);
        testMap.put(CREATE_WEIGHT + MAX_END_TIME_SUFFIX, 1598347679558960L);
        testMap.put(CREATE_WEIGHT + MIN_START_TIME_SUFFIX, 1598347679188617L);
        testMap.put(BUILD_SCORER, 0L);
        testMap.put(BUILD_SCORER + COUNT_SUFFIX, 6L);
        testMap.put(BUILD_SCORER + START_TIME_SUFFIX, 0L);
        testMap.put(BUILD_SCORER + MAX_END_TIME_SUFFIX, 1598347688270123L);
        testMap.put(BUILD_SCORER + MIN_START_TIME_SUFFIX, 1598347686448701L);
        testMap.put(NEXT_DOC, 0L);
        testMap.put(NEXT_DOC + COUNT_SUFFIX, 5L);
        testMap.put(NEXT_DOC + START_TIME_SUFFIX, 0L);
        testMap.put(NEXT_DOC + MAX_END_TIME_SUFFIX, 1598347688298671L);
        testMap.put(NEXT_DOC + MIN_START_TIME_SUFFIX, 1598347688110288L);
        testMap.put(ADVANCE, 0L);
        testMap.put(ADVANCE + COUNT_SUFFIX, 3L);
        testMap.put(ADVANCE + START_TIME_SUFFIX, 0L);
        testMap.put(ADVANCE + MAX_END_TIME_SUFFIX, 1598347688280739L);
        testMap.put(ADVANCE + MIN_START_TIME_SUFFIX, 1598347687991984L);
        testMap.put(MATCH, 0L);
        testMap.put(MATCH + COUNT_SUFFIX, 0L);
        testMap.put(MATCH + START_TIME_SUFFIX, 0L);
        testMap.put(MATCH + MAX_END_TIME_SUFFIX, 0L);
        testMap.put(MATCH + MIN_START_TIME_SUFFIX, 0L);
        testMap.put(SCORE, 0L);
        testMap.put(SCORE + COUNT_SUFFIX, 5L);
        testMap.put(SCORE + START_TIME_SUFFIX, 0L);
        testMap.put(SCORE + MAX_END_TIME_SUFFIX, 1598347688282743L);
        testMap.put(SCORE + MIN_START_TIME_SUFFIX, 1598347688018500L);
        testMap.put(SHALLOW_ADVANCE, 0L);
        testMap.put(SHALLOW_ADVANCE + COUNT_SUFFIX, 0L);
        testMap.put(SHALLOW_ADVANCE + START_TIME_SUFFIX, 0L);
        testMap.put(SHALLOW_ADVANCE + MAX_END_TIME_SUFFIX, 0L);
        testMap.put(SHALLOW_ADVANCE + MIN_START_TIME_SUFFIX, 0L);
        testMap.put(COMPUTE_MAX_SCORE, 0L);
        testMap.put(COMPUTE_MAX_SCORE + COUNT_SUFFIX, 0L);
        testMap.put(COMPUTE_MAX_SCORE + START_TIME_SUFFIX, 0L);
        testMap.put(COMPUTE_MAX_SCORE + MAX_END_TIME_SUFFIX, 0L);
        testMap.put(COMPUTE_MAX_SCORE + MIN_START_TIME_SUFFIX, 0L);
        testMap.put(SET_MIN_COMPETITIVE_SCORE, 0L);
        testMap.put(SET_MIN_COMPETITIVE_SCORE + COUNT_SUFFIX, 0L);
        testMap.put(SET_MIN_COMPETITIVE_SCORE + START_TIME_SUFFIX, 0L);
        testMap.put(SET_MIN_COMPETITIVE_SCORE + MAX_END_TIME_SUFFIX, 0L);
        testMap.put(SET_MIN_COMPETITIVE_SCORE + MIN_START_TIME_SUFFIX, 0L);
        ConcurrentQueryProfileBreakdown profileBreakdown = new ConcurrentQueryProfileBreakdown();
        Map<String, Map<String, Long>> sliceLevelBreakdown = new HashMap<>();
        sliceLevelBreakdown.put("testCollector", testMap);
        Map<String, Long> breakdownMap = profileBreakdown.buildFinalBreakdownMap(sliceLevelBreakdown);
        assertEquals(66, breakdownMap.size());
        assertEquals(
            "{max_match=0, set_min_competitive_score_count=0, match_count=0, avg_score_count=5, shallow_advance_count=0, next_doc=188383, min_build_scorer=0, score_count=5, compute_max_score_count=0, advance=288755, min_advance=0, min_set_min_competitive_score=0, score=264243, avg_set_min_competitive_score_count=0, min_match_count=0, avg_score=0, max_next_doc_count=5, avg_shallow_advance=0, max_compute_max_score_count=0, max_shallow_advance_count=0, set_min_competitive_score=0, min_build_scorer_count=6, next_doc_count=5, avg_next_doc=0, min_match=0, compute_max_score=0, max_build_scorer=0, min_set_min_competitive_score_count=0, avg_match_count=0, avg_advance=0, build_scorer_count=6, avg_build_scorer_count=6, min_next_doc_count=5, avg_match=0, max_score_count=5, min_shallow_advance_count=0, avg_compute_max_score=0, max_advance=0, avg_shallow_advance_count=0, avg_set_min_competitive_score=0, avg_compute_max_score_count=0, avg_build_scorer=0, max_set_min_competitive_score_count=0, advance_count=3, max_build_scorer_count=6, shallow_advance=0, max_match_count=0, min_compute_max_score=0, create_weight_count=1, build_scorer=1821422, max_compute_max_score=0, max_set_min_competitive_score=0, min_shallow_advance=0, match=0, min_next_doc=0, avg_advance_count=3, max_shallow_advance=0, max_advance_count=3, min_score=0, max_next_doc=0, create_weight=370343, avg_next_doc_count=5, max_score=0, min_compute_max_score_count=0, min_score_count=5, min_advance_count=3}",
            breakdownMap.toString()
        );
    }
}
