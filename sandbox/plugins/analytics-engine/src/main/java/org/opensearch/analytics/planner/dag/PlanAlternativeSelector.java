/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

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
 * <p>Selection rule (today): when a stage has a metadata-only-driving alternative
 * ({@code "lucene"}) AND the cluster setting {@code analytics.planner.prefer_metadata_driver}
 * is enabled, that alternative wins. Otherwise the alternative list is left untouched.
 *
 * <p>TODO: today {@code "lucene"} is the only metadata-only driver and is identified by
 * backend id. When a second metadata-only backend lands (or a backend that declares both
 * InvertedIndex AND DocValues), replace this hardcoded id with a first-class identifier on
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
        for (StagePlan plan : alternatives) {
            if (METADATA_ONLY_DRIVER.equals(plan.backendId())) {
                stage.setPlanAlternatives(List.of(plan));
                return;
            }
        }
    }
}
