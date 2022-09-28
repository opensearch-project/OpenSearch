/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.Version;
import org.opensearch.common.util.concurrent.AtomicArray;
import org.opensearch.index.query.IdsQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.shard.ShardId;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.search.internal.ShardSearchContextId;

import java.util.HashMap;
import java.util.Map;

import static org.opensearch.test.OpenSearchTestCase.between;
import static org.opensearch.test.OpenSearchTestCase.randomAlphaOfLength;
import static org.opensearch.test.OpenSearchTestCase.randomBoolean;

/**
 * Helper class for common pit tests functions
 */
public class PitTestsUtil {
    private PitTestsUtil() {}

    public static QueryBuilder randomQueryBuilder() {
        if (randomBoolean()) {
            return new TermQueryBuilder(randomAlphaOfLength(10), randomAlphaOfLength(10));
        } else if (randomBoolean()) {
            return new MatchAllQueryBuilder();
        } else {
            return new IdsQueryBuilder().addIds(randomAlphaOfLength(10));
        }
    }

    public static String getPitId() {
        AtomicArray<SearchPhaseResult> array = new AtomicArray<>(3);
        SearchAsyncActionTests.TestSearchPhaseResult testSearchPhaseResult1 = new SearchAsyncActionTests.TestSearchPhaseResult(
            new ShardSearchContextId("a", 1),
            null
        );
        testSearchPhaseResult1.setSearchShardTarget(new SearchShardTarget("node_1", new ShardId("idx", "uuid1", 2), null, null));
        SearchAsyncActionTests.TestSearchPhaseResult testSearchPhaseResult2 = new SearchAsyncActionTests.TestSearchPhaseResult(
            new ShardSearchContextId("b", 12),
            null
        );
        testSearchPhaseResult2.setSearchShardTarget(new SearchShardTarget("node_2", new ShardId("idy", "uuid2", 42), null, null));
        SearchAsyncActionTests.TestSearchPhaseResult testSearchPhaseResult3 = new SearchAsyncActionTests.TestSearchPhaseResult(
            new ShardSearchContextId("c", 42),
            null
        );
        testSearchPhaseResult3.setSearchShardTarget(new SearchShardTarget("node_3", new ShardId("idy", "uuid2", 43), null, null));
        array.setOnce(0, testSearchPhaseResult1);
        array.setOnce(1, testSearchPhaseResult2);
        array.setOnce(2, testSearchPhaseResult3);

        final Version version = Version.CURRENT;
        final Map<String, AliasFilter> aliasFilters = new HashMap<>();
        for (SearchPhaseResult result : array.asList()) {
            final AliasFilter aliasFilter;
            if (randomBoolean()) {
                aliasFilter = new AliasFilter(randomQueryBuilder());
            } else if (randomBoolean()) {
                aliasFilter = new AliasFilter(randomQueryBuilder(), "alias-" + between(1, 10));
            } else {
                aliasFilter = AliasFilter.EMPTY;
            }
            if (randomBoolean()) {
                aliasFilters.put(result.getSearchShardTarget().getShardId().getIndex().getUUID(), aliasFilter);
            }
        }
        return SearchContextId.encode(array.asList(), aliasFilters, version);
    }
}
