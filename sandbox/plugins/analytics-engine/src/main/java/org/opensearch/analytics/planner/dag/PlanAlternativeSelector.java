/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;

import java.util.ArrayList;
import java.util.List;

/**
 * Collapses a {@link Stage}'s {@code planAlternatives} down to a single chosen alternative
 * before {@link FragmentConversionDriver} runs. Two consequences:
 *
 * <ul>
 *   <li>Convertor runs once per stage instead of once per alternative — saves coordinator
 *       CPU when {@code PlanForker} produced multiple viable backends.</li>
 *   <li>Each {@code FragmentExecutionRequest} ships exactly one {@code PlanAlternative}, so
 *       the data node skips alternative selection.</li>
 * </ul>
 *
 * <p>Selection rule (today): when {@code analytics.planner.prefer_metadata_driver} is enabled
 * AND a stage has a metadata-only-driving alternative ({@code "lucene"}) AND the resolved
 * fragment is one Lucene can execute end-to-end (Aggregate with empty groupSet — the count
 * fast path), that alternative wins. Non-drivable Lucene alternatives are dropped so the
 * data-node fallback can't pick them.
 *
 * <p>When the setting is disabled, {@code OpenSearchTableScanRule} never admits Lucene as a
 * scan alternative in the first place (the strict value-producing gate drops it because
 * Lucene declares no value-producing scan today), so the selector simply has nothing Lucene
 * to operate on and its decisions reduce to no-ops.
 *
 * <p>TODO: today {@code "lucene"} is the only metadata-only driver and is identified by
 * backend id. When a second metadata-only backend lands (or a backend that declares both
 * Index AND DocValues), replace this hardcoded id with a first-class identifier on
 * {@code BackendCapabilityProvider} so the planner can pick "the metadata-only driver"
 * generically. See {@code OpenSearchTableScanRule} for the matching TODO.
 *
 * @opensearch.internal
 */
public final class PlanAlternativeSelector {

    /** Single metadata-only backend today. See class TODO. */
    private static final String METADATA_ONLY_DRIVER = "lucene";

    private PlanAlternativeSelector() {}

    /**
     * Walks the DAG and collapses each stage's {@code planAlternatives} per the rule above.
     * Stages with zero or one alternative are untouched.
     *
     * @param dag plan-forked DAG; modified in place.
     * @param preferMetadataDriver value of {@code analytics.planner.prefer_metadata_driver}.
     */
    public static void selectAll(QueryDAG dag, boolean preferMetadataDriver) {
        if (preferMetadataDriver == false) return;
        selectStage(dag.rootStage());
    }

    private static void selectStage(Stage stage) {
        for (Stage child : stage.getChildStages()) {
            selectStage(child);
        }
        List<StagePlan> alternatives = stage.getPlanAlternatives();
        if (alternatives.size() < 2) return;

        // Single walk: keep non-Lucene alternatives unconditionally, keep Lucene only when it
        // can drive the stage end-to-end. If a drivable Lucene survives, collapse to it; if
        // we dropped any Lucene alternative, persist the filtered list so the data-node
        // fallback can't silently pick a non-drivable Lucene plan.
        StagePlan drivableLucene = null;
        List<StagePlan> survivors = new ArrayList<>(alternatives.size());
        for (StagePlan plan : alternatives) {
            boolean isLucene = METADATA_ONLY_DRIVER.equals(plan.backendId());
            if (isLucene && canMetadataDriverExecute(plan.resolvedFragment()) == false) continue;
            survivors.add(plan);
            if (isLucene) drivableLucene = plan;
        }
        if (survivors.isEmpty()) {
            throw new IllegalStateException(
                "PlanAlternativeSelector: dropping non-drivable metadata-only alternative left zero alternatives for stage "
                    + stage.getStageId()
            );
        }
        if (drivableLucene != null) {
            stage.setPlanAlternatives(List.of(drivableLucene));
        } else if (survivors.size() != alternatives.size()) {
            stage.setPlanAlternatives(survivors);
        }
    }

    /**
     * Lucene-as-driver only emits a single count value per shard — no grouping, no value
     * materialization, no other aggregate functions. A fragment is drivable iff its top is
     * an {@link OpenSearchAggregate} that:
     * <ul>
     *   <li>has an EMPTY group-set (no {@code BY} keys — Lucene can't materialise per-group
     *       counts via {@code IndexSearcher.count});</li>
     *   <li>contains only {@code COUNT} aggregate calls — {@code SUM}/{@code MIN}/{@code MAX}/
     *       {@code AVG}/etc. need actual column values that Lucene's term dictionary can't
     *       provide. The {@link SqlKind#COUNT COUNT} kind covers both
     *       {@code count(*)} (no field arg) and {@code count(field)} (count of non-nulls).</li>
     * </ul>
     * Anything else (a bare {@code TableScan}, a Project, a Filter without a parent agg,
     * a grouped count, a SUM) needs row values or per-group values Lucene can't produce.
     *
     * <p>Today's filter coverage is also enforced upstream: PlanForker's chain-agreement
     * narrows aggregate alternatives to backends that declare the aggregate function as a
     * capability (prod Lucene declares only COUNT). This predicate is a defense-in-depth
     * guard so a future capability declaration drift doesn't accidentally route a non-COUNT
     * aggregate through the Lucene-driver path.
     *
     * <p>TODO: this is a coordinator-side coupling — the selector decides what Lucene can
     * physically execute by inspecting RelNode shapes that really belong to the backend's
     * own contract. When a second metadata-only backend lands, this should move behind a
     * backend-owned predicate (e.g. {@code FragmentConvertor.canDriveFragment(RelNode)})
     * so each backend declares its own drivability rule and the selector stays generic.
     */
    // Package-private for tests — see PlanAlternativeSelectorTests.
    static boolean canMetadataDriverExecute(RelNode fragment) {
        if (fragment instanceof OpenSearchAggregate == false) return false;
        OpenSearchAggregate agg = (OpenSearchAggregate) fragment;
        if (agg.getGroupSet().isEmpty() == false) return false;
        for (AggregateCall call : agg.getAggCallList()) {
            if (call.getAggregation().getKind() != SqlKind.COUNT) return false;
        }
        return true;
    }
}
