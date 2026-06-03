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
import org.opensearch.analytics.spi.BackendShardPreference;
import org.opensearch.analytics.spi.ShardPreferenceContext;

import java.util.List;

/**
 * Collapses a {@link Stage}'s {@code planAlternatives} to a single chosen alternative before
 * {@link FragmentConversionDriver} runs, so the convertor runs once per stage and the wire
 * request carries one {@code PlanAlternative}.
 *
 * <p>Selection: the highest-scoring alternative wins, where each backend declares its score
 * via {@link BackendShardPreference#scoreFor}. Alternatives whose backend has no
 * preference (or scores empty) are kept as-is — value-producing backends that don't
 * implement {@link BackendShardPreference} simply pass through.
 *
 * <p>Today's only consumer is Lucene's count-fast-path. The {@link ShardPreferenceContext}
 * surface is intentionally minimal (just the user-facing {@code prefer_metadata_driver} flag);
 * future inputs (deletes, segment count, query-cache warmth) plug into the same scoring path.
 *
 * <p>TODO: this selection runs on the coordinator using only fragment-shape signals. True
 * shard-local routing — where the same fragment routes differently to different shards based
 * on per-shard state — needs the score function to run on the data node with shard-local
 * inputs. The {@link BackendShardPreference} SPI is shape-compatible with that move; the
 * selector just needs to defer scoring instead of running it here.
 *
 * @opensearch.internal
 */
public final class PlanAlternativeSelector {

    private PlanAlternativeSelector() {}

    /**
     * Collapses each stage's alternatives by score. Stages with ≤1 alternative are untouched.
     *
     * @param dag                  plan-forked DAG; modified in place.
     * @param registry             capability registry for backend lookups.
     * @param preferMetadataDriver value of {@code analytics.planner.prefer_metadata_driver},
     *                             passed through to backend scoring functions.
     */
    public static void selectAll(QueryDAG dag, CapabilityRegistry registry, boolean preferMetadataDriver) {
        if (preferMetadataDriver == false) return;
        selectStage(dag.rootStage(), registry, new ShardPreferenceContext(preferMetadataDriver));
    }

    private static void selectStage(Stage stage, CapabilityRegistry registry, ShardPreferenceContext ctx) {
        for (Stage child : stage.getChildStages()) {
            selectStage(child, registry, ctx);
        }
        if (stage.getPlanAlternatives().size() < 2) return;

        // Pick the highest-scoring alternative. Backends without a preference score 0;
        // a positive score wins. Ties go to the first plan in PlanForker order.
        StagePlan winner = stage.getPlanAlternatives().getFirst();
        int winnerScore = scoreOf(winner, registry, ctx);
        for (int i = 1; i < stage.getPlanAlternatives().size(); i++) {
            StagePlan plan = stage.getPlanAlternatives().get(i);
            int s = scoreOf(plan, registry, ctx);
            if (s > winnerScore) {
                winner = plan;
                winnerScore = s;
            }
        }
        stage.setPlanAlternatives(List.of(winner));
    }

    private static int scoreOf(StagePlan plan, CapabilityRegistry registry, ShardPreferenceContext ctx) {
        AnalyticsSearchBackendPlugin backend = registry.getBackend(plan.backendId());
        BackendShardPreference pref = backend.getCapabilityProvider().shardPreference();
        if (pref == null) return 0;
        return pref.scoreFor(plan.resolvedFragment(), ctx).orElse(0);
    }
}
