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

    public void testBuildQueryProfileBreakdownMap() {
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
        Map<String, Long> breakdownMap = profileBreakdown.buildQueryProfileBreakdownMap(testMap);
        assertEquals(18, breakdownMap.size());
        assertEquals(
            "{set_min_competitive_score_count=0, match_count=0, shallow_advance_count=0, set_min_competitive_score=0, next_doc=188383, match=0, next_doc_count=5, score_count=5, compute_max_score_count=0, compute_max_score=0, advance=288755, advance_count=3, score=264243, build_scorer_count=6, create_weight=370343, shallow_advance=0, create_weight_count=1, build_scorer=1821422}",
            breakdownMap.toString()
        );
    }

    public void testAddMaxEndTimeAndMinStartTime() {
        Map<String, Long> map = new HashMap<>();
        map.put(CREATE_WEIGHT, 201692L);
        map.put(CREATE_WEIGHT + COUNT_SUFFIX, 1L);
        map.put(CREATE_WEIGHT + START_TIME_SUFFIX, 1629732014278990L);
        map.put(BUILD_SCORER, 0L);
        map.put(BUILD_SCORER + COUNT_SUFFIX, 2L);
        map.put(BUILD_SCORER + START_TIME_SUFFIX, 0L);
        map.put(NEXT_DOC, 0L);
        map.put(NEXT_DOC + COUNT_SUFFIX, 2L);
        map.put(NEXT_DOC + START_TIME_SUFFIX, 0L);
        map.put(ADVANCE, 0L);
        map.put(ADVANCE + COUNT_SUFFIX, 1L);
        map.put(ADVANCE + START_TIME_SUFFIX, 0L);
        map.put(MATCH, 0L);
        map.put(MATCH + COUNT_SUFFIX, 0L);
        map.put(MATCH + START_TIME_SUFFIX, 0L);
        map.put(SCORE, 0L);
        map.put(SCORE + COUNT_SUFFIX, 2L);
        map.put(SCORE + START_TIME_SUFFIX, 0L);
        map.put(SHALLOW_ADVANCE, 0L);
        map.put(SHALLOW_ADVANCE + COUNT_SUFFIX, 0L);
        map.put(SHALLOW_ADVANCE + START_TIME_SUFFIX, 0L);
        map.put(COMPUTE_MAX_SCORE, 0L);
        map.put(COMPUTE_MAX_SCORE + COUNT_SUFFIX, 0L);
        map.put(COMPUTE_MAX_SCORE + START_TIME_SUFFIX, 0L);
        map.put(SET_MIN_COMPETITIVE_SCORE, 0L);
        map.put(SET_MIN_COMPETITIVE_SCORE + COUNT_SUFFIX, 0L);
        map.put(SET_MIN_COMPETITIVE_SCORE + START_TIME_SUFFIX, 0L);

        Map<String, Long> breakdown = new HashMap<>();
        breakdown.put(CREATE_WEIGHT, 0L);
        breakdown.put(CREATE_WEIGHT + COUNT_SUFFIX, 0L);
        breakdown.put(CREATE_WEIGHT + START_TIME_SUFFIX, 0L);
        breakdown.put(BUILD_SCORER, 9649L);
        breakdown.put(BUILD_SCORER + COUNT_SUFFIX, 2L);
        breakdown.put(BUILD_SCORER + START_TIME_SUFFIX, 1629732030749745L);
        breakdown.put(NEXT_DOC, 1150L);
        breakdown.put(NEXT_DOC + COUNT_SUFFIX, 2L);
        breakdown.put(NEXT_DOC + START_TIME_SUFFIX, 1629732030806446L);
        breakdown.put(ADVANCE, 920L);
        breakdown.put(ADVANCE + COUNT_SUFFIX, 1L);
        breakdown.put(ADVANCE + START_TIME_SUFFIX, 1629732030776129L);
        breakdown.put(MATCH, 0L);
        breakdown.put(MATCH + COUNT_SUFFIX, 0L);
        breakdown.put(MATCH + START_TIME_SUFFIX, 0L);
        breakdown.put(SCORE, 1050L);
        breakdown.put(SCORE + COUNT_SUFFIX, 2L);
        breakdown.put(SCORE + START_TIME_SUFFIX, 1629732030778977L);
        breakdown.put(SHALLOW_ADVANCE, 0L);
        breakdown.put(SHALLOW_ADVANCE + COUNT_SUFFIX, 0L);
        breakdown.put(SHALLOW_ADVANCE + START_TIME_SUFFIX, 0L);
        breakdown.put(COMPUTE_MAX_SCORE, 0L);
        breakdown.put(COMPUTE_MAX_SCORE + COUNT_SUFFIX, 0L);
        breakdown.put(COMPUTE_MAX_SCORE + START_TIME_SUFFIX, 0L);
        breakdown.put(SET_MIN_COMPETITIVE_SCORE, 0L);
        breakdown.put(SET_MIN_COMPETITIVE_SCORE + COUNT_SUFFIX, 0L);
        breakdown.put(SET_MIN_COMPETITIVE_SCORE + START_TIME_SUFFIX, 0L);
        ConcurrentQueryProfileBreakdown profileBreakdown = new ConcurrentQueryProfileBreakdown();
        profileBreakdown.addMaxEndTimeAndMinStartTime(map, breakdown);
        assertEquals(45, map.size());
        assertEquals(
            "{set_min_competitive_score_count=0, match_count=0, score_start_time=0, shallow_advance_count=0, create_weight_start_time=1629732014278990, next_doc=0, compute_max_score_start_time=0, shallow_advance_min_start_time=0, score_count=2, compute_max_score_count=0, advance_start_time=0, advance=0, advance_count=1, compute_max_score_min_start_time=0, score=0, next_doc_max_end_time=1629732030807596, advance_max_end_time=1629732030777049, next_doc_start_time=0, shallow_advance=0, build_scorer_max_end_time=1629732030759394, create_weight_count=1, create_weight_max_end_time=1629732014480682, match_min_start_time=0, build_scorer=0, compute_max_score_max_end_time=0, next_doc_min_start_time=1629732030806446, set_min_competitive_score=0, set_min_competitive_score_start_time=0, match=0, set_min_competitive_score_max_end_time=0, match_start_time=0, shallow_advance_max_end_time=0, build_scorer_start_time=0, next_doc_count=2, shallow_advance_start_time=0, set_min_competitive_score_min_start_time=0, compute_max_score=0, create_weight_min_start_time=1629732014278990, build_scorer_count=2, create_weight=201692, score_min_start_time=1629732030778977, match_max_end_time=0, advance_min_start_time=1629732030776129, score_max_end_time=1629732030780027, build_scorer_min_start_time=1629732030749745}",
            map.toString()
        );
    }
}
