/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.planner.SqlPlannerTestFixture;
import org.opensearch.cluster.ClusterState;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Which indices get their row counts seeded.
 *
 * <p>This runs BEFORE decorrelation, so a subquery is still a {@code RexSubQuery} hanging off a Filter
 * condition or a Project expression rather than an input in the RelNode tree. Collecting scans by walking
 * inputs alone therefore misses any index that appears ONLY inside a subquery, and every such scan then
 * falls back to Calcite's default row count.
 *
 * <p>That default is tiny, so estimates derived from it collapse — a filter and aggregate above such a
 * scan come out at about one row — and plan choices then get made on a number with no relation to the
 * data. The failure is silent: the query still returns correct results, just via a plan chosen on
 * fiction, so nothing but a test like this catches a regression here.
 */
public class IndexRowCountFetcherTests extends OpenSearchTestCase {

    private static final Map<String, Map<String, Object>> FIELDS = Map.of(
        "status",
        Map.of("type", "integer"),
        "size",
        Map.of("type", "integer")
    );

    private static Set<String> namesFor(String sql, String... indices) {
        ClusterState state = SqlPlannerTestFixture.clusterStateWith(List.of(indices), FIELDS);
        RelNode parsed = SqlPlannerTestFixture.parseSql(sql, state);
        return IndexRowCountFetcher.referencedIndexNames(parsed);
    }

    /** The plain case: an index scanned by the outer query is collected. */
    public void testOuterQueryIndexIsCollected() {
        assertEquals(Set.of("outer_index"), namesFor("SELECT * FROM outer_index", "outer_index"));
    }

    /**
     * The regression this class exists for: an index reachable only through an EXISTS subquery must be
     * collected too. Missing it leaves that scan on Calcite's default row count while its sibling is
     * correctly sized, so the two sides of a join are estimated on incomparable numbers.
     */
    public void testIndexReferencedOnlyInsideAnExistsSubqueryIsCollected() {
        Set<String> names = namesFor(
            "SELECT * FROM outer_index WHERE EXISTS (SELECT 1 FROM inner_index WHERE inner_index.status = outer_index.status)",
            "outer_index",
            "inner_index"
        );
        assertTrue(
            "a table reachable only through a subquery must still be seeded, else it keeps Calcite's default "
                + "row count while the outer table is sized correctly; collected "
                + names,
            names.contains("inner_index")
        );
        assertEquals(Set.of("outer_index", "inner_index"), names);
    }

    /** Same requirement for an IN subquery, which lowers through a different rule. */
    public void testIndexReferencedOnlyInsideAnInSubqueryIsCollected() {
        Set<String> names = namesFor(
            "SELECT * FROM outer_index WHERE status IN (SELECT status FROM inner_index)",
            "outer_index",
            "inner_index"
        );
        assertEquals(Set.of("outer_index", "inner_index"), names);
    }

    /** And for a scalar subquery in the WHERE clause. */
    public void testIndexReferencedOnlyInsideAScalarSubqueryIsCollected() {
        Set<String> names = namesFor(
            "SELECT * FROM outer_index WHERE status > (SELECT max(status) FROM inner_index)",
            "outer_index",
            "inner_index"
        );
        assertEquals(Set.of("outer_index", "inner_index"), names);
    }

    /** A subquery over the SAME index must not produce a duplicate or a spurious extra name. */
    public void testSubqueryOverTheSameIndexCollectsItOnce() {
        assertEquals(Set.of("outer_index"), namesFor("SELECT * FROM outer_index WHERE EXISTS (SELECT 1 FROM outer_index)", "outer_index"));
    }
}
