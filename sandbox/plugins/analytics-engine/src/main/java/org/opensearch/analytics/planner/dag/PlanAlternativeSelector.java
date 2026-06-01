/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;

import java.util.ArrayList;
import java.util.List;

/**
 * Collapses a {@link Stage}'s {@code planAlternatives} to a single chosen alternative before
 * {@link FragmentConversionDriver} runs, so the convertor runs once per stage and the wire
 * request carries one {@code PlanAlternative}.
 *
 * <p>Rule: when {@code analytics.planner.prefer_metadata_driver} is on, prefer any
 * {@code isMetadataOnlyDriver()} backend whose convertor's {@code canDriveFragment} accepts
 * the resolved fragment. Non-drivable metadata-only alternatives are dropped — the data
 * node won't see them. When the setting is off, the planner-side gate drops metadata-only
 * backends upstream and this is a no-op.
 *
 * @opensearch.internal
 */
public final class PlanAlternativeSelector {

    private PlanAlternativeSelector() {}

    /**
     * Collapses each stage's alternatives per the class-level rule. Stages with ≤1
     * alternative are untouched.
     *
     * @param dag                  plan-forked DAG; modified in place.
     * @param registry             capability registry for {@code isMetadataOnlyDriver()} and
     *                             per-fragment drivability lookups.
     * @param preferMetadataDriver value of {@code analytics.planner.prefer_metadata_driver}.
     */
    public static void selectAll(QueryDAG dag, CapabilityRegistry registry, boolean preferMetadataDriver) {
        if (preferMetadataDriver == false) return;
        selectStage(dag.rootStage(), registry);
    }

    private static void selectStage(Stage stage, CapabilityRegistry registry) {
        for (Stage child : stage.getChildStages()) {
            selectStage(child, registry);
        }
        List<StagePlan> alternatives = stage.getPlanAlternatives();
        if (alternatives.size() < 2) return;

        // Keep value-producing alternatives unconditionally; keep metadata-only ones only
        // when drivable. If a drivable metadata-only survives, collapse to it. If we dropped
        // any, persist the filtered list so the data-node fallback can't pick a non-drivable.
        StagePlan drivableMetadataOnly = null;
        List<StagePlan> survivors = new ArrayList<>(alternatives.size());
        for (StagePlan plan : alternatives) {
            AnalyticsSearchBackendPlugin backend = registry.getBackend(plan.backendId());
            boolean metadataOnly = backend.getCapabilityProvider().isMetadataOnlyDriver();
            if (metadataOnly && backend.getFragmentConvertor().canDriveFragment(plan.resolvedFragment()) == false) continue;
            survivors.add(plan);
            if (metadataOnly) drivableMetadataOnly = plan;
        }
        if (survivors.isEmpty()) {
            throw new IllegalStateException("Stage " + stage.getStageId() + ": dropped all metadata-only alternatives, no fallback");
        }
        if (drivableMetadataOnly != null) {
            stage.setPlanAlternatives(List.of(drivableMetadataOnly));
        } else if (survivors.size() != alternatives.size()) {
            stage.setPlanAlternatives(survivors);
        }
    }
}
