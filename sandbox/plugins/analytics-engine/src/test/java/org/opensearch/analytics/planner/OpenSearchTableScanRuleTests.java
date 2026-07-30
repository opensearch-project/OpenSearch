/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.analytics.planner.rules.OpenSearchTableScanRule;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Pins the wiring contract between {@link OpenSearchTableScanRule} and {@link PlannerContext}:
 * the rule resolves table names through the resolver supplied by the context. A regression that
 * silently constructs a fresh {@link IndexNameExpressionResolver} inside the rule would bypass
 * security-plugin extensions / system-index access checks attached to the cluster's resolver.
 */
public class OpenSearchTableScanRuleTests extends BasePlannerRulesTests {

    public void testOnMatchRoutesThroughResolverFromPlannerContext() {
        // Resolver supplied via PlannerContext — answers concreteIndexNames so the rule can
        // proceed past resolution. The verify() afterwards is the contract: the rule must have
        // gone through THIS resolver, not constructed its own.
        IndexNameExpressionResolver resolverFromContext = mock(IndexNameExpressionResolver.class);
        when(
            resolverFromContext.concreteIndexNames(
                any(ClusterState.class),
                any(IndicesOptions.class),
                org.mockito.ArgumentMatchers.anyBoolean(),
                any(String[].class)
            )
        ).thenReturn(new String[] { "test_index" });

        PlannerContext context = buildContextWithResolver(resolverFromContext);
        RelOptTable table = mockTable("test_index", "status", "size");

        // Drive the rule via the standard planner-program runner; any rule outcome (transform or
        // no-op) is acceptable here — the assertion is "did the rule call THIS resolver?".
        RelNode ignored = runPlanner(stubScan(table), context);
        assertNotNull(ignored);

        verify(resolverFromContext, atLeastOnce()).concreteIndexNames(
            same(context.getClusterState()),
            any(IndicesOptions.class),
            org.mockito.ArgumentMatchers.anyBoolean(),
            eq(new String[] { "test_index" })
        );
    }

    /**
     * Builds a PlannerContext that wires the given resolver through the 4-arg constructor.
     * Mirrors {@link BasePlannerRulesTests#buildContext(String, java.util.Map)} for everything
     * else (single-shard parquet test_index with intFields).
     */
    private PlannerContext buildContextWithResolver(IndexNameExpressionResolver resolver) {
        // Reuse the standard context to get its mocked ClusterState / CapabilityRegistry, then
        // re-wrap with the resolver-bearing 4-arg ctor.
        PlannerContext baseline = buildContext("parquet", intFields());
        return new PlannerContext(baseline.getCapabilityRegistry(), baseline.getClusterState(), resolver, false);
    }
}
