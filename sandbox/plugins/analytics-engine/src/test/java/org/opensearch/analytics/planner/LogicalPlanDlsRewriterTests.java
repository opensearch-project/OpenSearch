/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.opensearch.OpenSearchException;
import org.opensearch.action.support.ReadAccessPolicy;
import org.opensearch.analytics.query.QueryBuilderTranslationService;
import org.opensearch.cluster.ClusterState;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class LogicalPlanDlsRewriterTests extends BasePlannerRulesTests {

    public void testReplacesWildcardWithConcreteIndicesAndAddsSharedRestriction() {
        TableScan scan = stubScan(mockTable("logs-*", "tenant"));
        AtomicReference<QueryBuilder> translated = new AtomicReference<>();
        QueryBuilderTranslationService translationService = new QueryBuilderTranslationService(List.of((query, cluster, table) -> {
            translated.set(query);
            return cluster.getRexBuilder().makeLiteral(true);
        }));
        LogicalPlanDlsRewriter rewriter = new LogicalPlanDlsRewriter(translationService);
        QueryBuilder restriction = QueryBuilders.termQuery("tenant", "blue");
        ReadAccessPolicy policy = policy(restrictedGroup(Set.of("logs-2025", "logs-2026"), restriction));

        RelNode result = rewriter.rewrite(scan, policy, Map.of("logs-*", List.of("logs-2025", "logs-2026")));

        assertTrue(result instanceof LogicalFilter);
        LogicalFilter filter = (LogicalFilter) result;
        assertEquals("logs-2025,logs-2026", tableName((TableScan) filter.getInput()));
        assertSame(restriction, translated.get());
    }

    public void testCreatesUnionForDifferentIndexGroups() {
        TableScan scan = stubScan(mockTable("logs-*", "tenant"));
        List<QueryBuilder> translated = new ArrayList<>();
        QueryBuilderTranslationService translationService = new QueryBuilderTranslationService(List.of((query, cluster, table) -> {
            translated.add(query);
            return cluster.getRexBuilder().makeLiteral(true);
        }));
        QueryBuilder blue = QueryBuilders.termQuery("tenant", "blue");
        QueryBuilder green = QueryBuilders.termQuery("tenant", "green");
        ReadAccessPolicy policy = policy(
            restrictedGroup(List.of("logs-blue-1", "logs-blue-2"), blue),
            restrictedGroup(List.of("logs-green"), green),
            unrestrictedGroup(List.of("logs-public"))
        );

        RelNode result = new LogicalPlanDlsRewriter(translationService).rewrite(
            scan,
            policy,
            Map.of("logs-*", List.of("logs-blue-1", "logs-blue-2", "logs-green", "logs-public"))
        );

        assertTrue(result instanceof LogicalUnion);
        LogicalUnion union = (LogicalUnion) result;
        assertTrue(union.all);
        assertEquals(3, union.getInputs().size());
        assertEquals("logs-blue-1,logs-blue-2", tableName((TableScan) ((LogicalFilter) union.getInput(0)).getInput()));
        assertEquals("logs-green", tableName((TableScan) ((LogicalFilter) union.getInput(1)).getInput()));
        assertEquals("logs-public", tableName((TableScan) union.getInput(2)));
        assertEquals(List.of(blue, green), translated);
    }

    public void testLeavesPlanUnchangedForUnrestrictedPolicy() {
        TableScan scan = stubScan(mockTable("logs", "tenant"));
        QueryBuilderTranslationService translationService = new QueryBuilderTranslationService(List.of((query, cluster, table) -> {
            throw new AssertionError("translator must not be called");
        }));

        RelNode result = new LogicalPlanDlsRewriter(translationService).rewrite(
            scan,
            ReadAccessPolicy.unrestricted(),
            Map.of("logs", List.of("logs"))
        );

        assertSame(scan, result);
    }

    public void testMatchNoneDoesNotRequireTranslatorProvider() {
        TableScan scan = stubScan(mockTable("logs", "tenant"));
        LogicalPlanDlsRewriter rewriter = new LogicalPlanDlsRewriter(new QueryBuilderTranslationService(List.of()));
        ReadAccessPolicy policy = policy(restrictedGroup(List.of("logs"), new MatchNoneQueryBuilder()));

        RelNode result = rewriter.rewrite(scan, policy, Map.of("logs", List.of("logs")));

        assertTrue(result instanceof LogicalFilter);
        LogicalFilter filter = (LogicalFilter) result;
        assertTrue(filter.getCondition().isAlwaysFalse());
    }

    public void testFailsWhenTranslatorProviderIsMissing() {
        // A protected scan must fail rather than execute without its policy predicate.
        TableScan scan = stubScan(mockTable("logs", "tenant"));
        LogicalPlanDlsRewriter rewriter = new LogicalPlanDlsRewriter(new QueryBuilderTranslationService(List.of()));
        ReadAccessPolicy policy = policy(restrictedGroup(List.of("logs"), QueryBuilders.termQuery("tenant", "blue")));

        OpenSearchException exception = expectThrows(
            OpenSearchException.class,
            () -> rewriter.rewrite(scan, policy, Map.of("logs", List.of("logs")))
        );
        assertEquals("No QueryBuilderTranslatorProvider is installed", exception.getMessage());
    }

    public void testFailsWhenPolicyCannotBeAssociatedWithScan() {
        TableScan scan = stubScan(mockTable("logs", "tenant"));
        LogicalPlanDlsRewriter rewriter = new LogicalPlanDlsRewriter(
            new QueryBuilderTranslationService(List.of((query, cluster, table) -> cluster.getRexBuilder().makeLiteral(true)))
        );
        ReadAccessPolicy policy = policy(restrictedGroup(List.of("logs", "other-index"), QueryBuilders.matchAllQuery()));

        OpenSearchException exception = expectThrows(
            OpenSearchException.class,
            () -> rewriter.rewrite(scan, policy, Map.of("logs", List.of("logs")))
        );
        assertTrue(exception.getMessage().contains("other-index"));
    }

    public void testFailsWhenPolicyDoesNotCoverResolvedIndex() {
        TableScan scan = stubScan(mockTable("logs-*", "tenant"));
        LogicalPlanDlsRewriter rewriter = new LogicalPlanDlsRewriter(
            new QueryBuilderTranslationService(List.of((query, cluster, table) -> cluster.getRexBuilder().makeLiteral(true)))
        );
        ReadAccessPolicy policy = policy(restrictedGroup(List.of("logs-blue"), QueryBuilders.matchAllQuery()));

        OpenSearchException exception = expectThrows(
            OpenSearchException.class,
            () -> rewriter.rewrite(scan, policy, Map.of("logs-*", List.of("logs-blue", "logs-green")))
        );
        assertTrue(exception.getMessage().contains("logs-green"));
    }

    public void testRewritesScanInsideRexSubQuery() {
        ClusterState clusterState = SqlPlannerTestFixture.clusterStateWith(List.of("outer_index", "restricted_index"), intFields());
        RelNode plan = SqlPlannerTestFixture.parseSql(
            "SELECT * FROM outer_index WHERE EXISTS (SELECT 1 FROM restricted_index)",
            clusterState
        );
        QueryBuilder restriction = QueryBuilders.termQuery("status", "blue");
        QueryBuilderTranslationService translationService = new QueryBuilderTranslationService(
            List.of((query, cluster, table) -> cluster.getRexBuilder().makeLiteral(true))
        );
        ReadAccessPolicy policy = policy(
            unrestrictedGroup(List.of("outer-concrete")),
            restrictedGroup(List.of("restricted-concrete"), restriction)
        );

        assertEquals(List.of("outer_index", "restricted_index"), RelNodeUtils.extractTableExpressions(plan));

        RelNode result = new LogicalPlanDlsRewriter(translationService).rewrite(
            plan,
            policy,
            Map.of("outer_index", List.of("outer-concrete"), "restricted_index", List.of("restricted-concrete"))
        );

        Set<String> rewrittenTables = new HashSet<>();
        collectTableNames(result, rewrittenTables);
        assertEquals(Set.of("outer-concrete", "restricted-concrete"), rewrittenTables);
        assertTrue(hasFilterDirectlyAboveScan(result, "restricted-concrete"));
    }

    private static void collectTableNames(RelNode node, Set<String> tableNames) {
        if (node instanceof TableScan scan) {
            tableNames.add(tableName(scan));
        }
        for (RelNode input : node.getInputs()) {
            collectTableNames(input, tableNames);
        }
        node.accept(new RexShuttle() {
            @Override
            public RexNode visitSubQuery(RexSubQuery subQuery) {
                collectTableNames(subQuery.rel, tableNames);
                return super.visitSubQuery(subQuery);
            }
        });
    }

    private static boolean hasFilterDirectlyAboveScan(RelNode node, String expectedTableName) {
        if (node instanceof LogicalFilter filter
            && filter.getInput() instanceof TableScan scan
            && expectedTableName.equals(tableName(scan))) {
            return true;
        }
        for (RelNode input : node.getInputs()) {
            if (hasFilterDirectlyAboveScan(input, expectedTableName)) {
                return true;
            }
        }
        boolean[] found = { false };
        node.accept(new RexShuttle() {
            @Override
            public RexNode visitSubQuery(RexSubQuery subQuery) {
                found[0] = hasFilterDirectlyAboveScan(subQuery.rel, expectedTableName);
                return super.visitSubQuery(subQuery);
            }
        });
        return found[0];
    }

    private static String tableName(TableScan scan) {
        return scan.getTable().getQualifiedName().getLast();
    }

    private static ReadAccessPolicy policy(ReadAccessPolicy.IndexGroup... groups) {
        return new TestReadAccessPolicy(List.of(groups));
    }

    private static ReadAccessPolicy.IndexGroup restrictedGroup(Collection<String> concreteIndices, QueryBuilder restrictions) {
        return new TestIndexGroup(concreteIndices, Optional.of(restrictions));
    }

    private static ReadAccessPolicy.IndexGroup unrestrictedGroup(Collection<String> concreteIndices) {
        return new TestIndexGroup(concreteIndices, Optional.empty());
    }

    private static final class TestReadAccessPolicy implements ReadAccessPolicy {
        private final List<IndexGroup> groups;
        private final Set<String> coveredIndices;

        private TestReadAccessPolicy(List<IndexGroup> groups) {
            this.groups = List.copyOf(groups);
            Set<String> covered = new LinkedHashSet<>();
            for (IndexGroup group : groups) {
                covered.addAll(group.concreteIndices());
            }
            this.coveredIndices = Set.copyOf(covered);
        }

        @Override
        public boolean hasRestrictions() {
            return groups.stream().anyMatch(group -> group.restrictions().isPresent());
        }

        @Override
        public Set<String> coveredConcreteIndices() {
            return coveredIndices;
        }

        @Override
        public Optional<QueryBuilder> restrictionsForIndex(String concreteIndex) {
            return groups.stream()
                .filter(group -> group.concreteIndices().contains(concreteIndex))
                .findFirst()
                .flatMap(IndexGroup::restrictions);
        }

        @Override
        public Collection<IndexGroup> indexGroups() {
            return groups;
        }
    }

    private static final class TestIndexGroup implements ReadAccessPolicy.IndexGroup {
        private final Set<String> concreteIndices;
        private final Optional<QueryBuilder> restrictions;

        private TestIndexGroup(Collection<String> concreteIndices, Optional<QueryBuilder> restrictions) {
            this.concreteIndices = Set.copyOf(concreteIndices);
            this.restrictions = restrictions;
        }

        @Override
        public Set<String> concreteIndices() {
            return concreteIndices;
        }

        @Override
        public Optional<QueryBuilder> restrictions() {
            return restrictions;
        }
    }
}
