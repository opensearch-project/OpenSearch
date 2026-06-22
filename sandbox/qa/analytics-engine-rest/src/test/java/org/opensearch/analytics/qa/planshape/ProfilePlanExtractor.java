/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa.planshape;

import org.opensearch.analytics.qa.planshape.ExpectedQueryPlan.PlanShapeLayer;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * Turns the {@code profile} block of a {@code /_plugins/_ppl?profile=true} response into the same
 * four {@link PlanShapeLayer} strings stored in a golden, so a captured plan can be diffed against
 * its {@link ExpectedQueryPlan}. The render + scrub rules here MUST match exactly how goldens were
 * authored.
 *
 * <ul>
 *   <li>{@link PlanShapeLayer#POST_CBO} = {@code profile.full_plan} lines joined by newline.
 *   <li>{@link PlanShapeLayer#FRAGMENT} = per stage, a header
 *       {@code [<execution_type> chosen_backend=<b> tree_shape=<t>]} then that stage's fragment lines.
 *   <li>{@link PlanShapeLayer#SHARD_PHYSICAL} = the SHARD_FRAGMENT stage's task physical plans,
 *       scrubbed and deduped (all shard tasks are identical after scrubbing). Empty if none
 *       (Lucene fast-path / empty shard).
 *   <li>{@link PlanShapeLayer#COORD_PHYSICAL} = the COORDINATOR_REDUCE stage's task physical plan,
 *       scrubbed. Empty if there is no coordinator stage (single-shard / non-splitting query).
 * </ul>
 */
public final class ProfilePlanExtractor {

    /** Single consistent token for every non-deterministic value we redact from a plan. */
    private static final String SCRUBBED = "<scrubbed>";

    /** Host/UUID/shard/generation-specific parquet path. */
    private static final Pattern FILE_GROUPS = Pattern.compile("file_groups=\\{[^}]*\\}");
    private static final String FILE_GROUPS_SCRUBBED = "file_groups={" + SCRUBBED + "}";

    // TopK SortExec's `, filter=[<expr> > N]` is a runtime value, not plan shape — its presence flips
    // with segment count, so we delete the whole clause (anchored on preserve_partitioning=[...] so
    // only SortExec is touched; DataSourceExec uses predicate=, not this).
    // TODO(plan-shape): confirm the exact mechanism. The "appears once the TopK heap fills to k"
    // explanation is from a read of DataFusion source, NOT verified end-to-end — it depends on WHEN
    // the Profile API snapshots the DF physical plan (pre-exec / mid-exec / post-exec). Trace the
    // QueryProfileBuilder -> Rust physical_plan capture path to nail this down. See core-axioms.
    private static final Pattern TOPK_DYNAMIC_FILTER =
        Pattern.compile("(preserve_partitioning=\\[[^\\]]*\\]), filter=\\[[^\\]]*\\]");
    private static final String TOPK_DYNAMIC_FILTER_KEPT = "$1";

    // input_partitions is deliberately NOT scrubbed: it equals the shard's segment count, which is a
    // controlled axis (captured per segment layout), so it's deterministic and worth asserting.

    /** The concrete index name is scrubbed too, so goldens are index-name agnostic. */
    private static final String INDEX_TOKEN = SCRUBBED;

    private static final String SHARD_FRAGMENT = "SHARD_FRAGMENT";
    private static final String COORDINATOR_REDUCE = "COORDINATOR_REDUCE";

    private final Map<PlanShapeLayer, Optional<String>> layers;

    private ProfilePlanExtractor(Map<PlanShapeLayer, Optional<String>> layers) {
        this.layers = layers;
    }

    /** The captured plan text for a layer, or empty if that layer was absent in the response. */
    public Optional<String> layer(PlanShapeLayer layer) {
        return layers.get(layer);
    }

    /**
     * Render the four layers from a profile response. {@code indexName} is the concrete index the
     * query ran against; every occurrence is scrubbed to {@link #INDEX_TOKEN} so a golden is
     * agnostic to which per-shard-count index was provisioned.
     */
    @SuppressWarnings("unchecked")
    public static ProfilePlanExtractor extractFrom(Map<String, Object> response, String indexName) {
        Map<String, Object> profile = (Map<String, Object>) response.get("profile");
        if (profile == null) {
            throw new AssertionError("response has no 'profile' block — request must set profile=true");
        }
        // The opensearch-sql plugin's QueryProfile {summary, phases, plan} embeds the analytics-engine
        // plan profile (full_plan + stages) under the `plan` field, so they live at profile.plan.
        Map<String, Object> planProfile = (Map<String, Object>) profile.get("plan");
        if (planProfile == null) {
            throw new AssertionError("profile has no 'plan' block; profile keys=" + profile.keySet());
        }
        List<Map<String, Object>> stages = (List<Map<String, Object>>) planProfile.get("stages");
        if (stages == null) {
            throw new AssertionError("profile.plan has no stages; plan keys=" + planProfile.keySet());
        }

        Map<PlanShapeLayer, Optional<String>> layers = new LinkedHashMap<>();
        layers.put(PlanShapeLayer.POST_CBO, Optional.of(joinLines((List<String>) planProfile.get("full_plan"))));
        layers.put(PlanShapeLayer.FRAGMENT, Optional.of(renderFragment(stages)));
        layers.put(PlanShapeLayer.SHARD_PHYSICAL, physicalPlanOf(stages, SHARD_FRAGMENT));
        layers.put(PlanShapeLayer.COORD_PHYSICAL, physicalPlanOf(stages, COORDINATOR_REDUCE));

        layers.replaceAll((layer, text) -> text.map(t -> t.replace(indexName, INDEX_TOKEN)));
        return new ProfilePlanExtractor(layers);
    }

    // TODO(plan-shape): FRAGMENT renders the PRE-FORK marked fragment. Once the profile exposes
    // post-fork `alternatives` (see QueryProfileBuilder TODO), assert on those instead.
    @SuppressWarnings("unchecked")
    private static String renderFragment(List<Map<String, Object>> stages) {
        StringBuilder rendered = new StringBuilder();
        for (Map<String, Object> stage : stages) {
            // tree_shape is the delegation shape (CONJUNCTIVE / INTERLEAVED_BOOLEAN_EXPRESSION /
            // ...) and MUST be asserted. "Absent" serializes inconsistently across builds (JSON
            // null vs the string "None"); canonicalize both to NONE so the value is always present
            // and node-agnostic.
            Object treeShape = stage.get("tree_shape");
            String treeShapeStr = (treeShape == null || "None".equals(treeShape)) ? "NONE" : treeShape.toString();
            rendered.append('[')
                .append(stage.get("execution_type"))
                .append(" chosen_backend=")
                .append(stage.get("chosen_backend"))
                .append(" tree_shape=")
                .append(treeShapeStr)
                .append(']')
                .append('\n');
            List<String> fragment = (List<String>) stage.get("fragment");
            if (fragment != null) {
                for (String line : fragment) {
                    rendered.append(line).append('\n');
                }
            }
        }
        return stripTrailingNewline(rendered.toString());
    }

    /**
     * The scrubbed, deduped physical plan for the stage of the given execution type, or empty if
     * the stage is absent or carries no task physical plan (Lucene / empty shard). All tasks of a
     * stage are expected to be identical after scrubbing; a divergence is a real finding and fails.
     */
    @SuppressWarnings("unchecked")
    private static Optional<String> physicalPlanOf(List<Map<String, Object>> stages, String executionType) {
        for (Map<String, Object> stage : stages) {
            if (!executionType.equals(stage.get("execution_type"))) {
                continue;
            }
            List<Map<String, Object>> tasks = (List<Map<String, Object>>) stage.get("tasks");
            if (tasks == null) {
                return Optional.empty();
            }
            TreeSet<String> distinctPlans = new TreeSet<>();
            for (Map<String, Object> task : tasks) {
                Object physicalPlan = task.get("physical_plan");
                if (physicalPlan instanceof String text && !text.isEmpty()) {
                    distinctPlans.add(scrub(text));
                }
            }
            if (distinctPlans.isEmpty()) {
                return Optional.empty();
            }
            if (distinctPlans.size() > 1) {
                throw new AssertionError(
                    "tasks of stage " + executionType + " produced differing physical plans after scrub:\n"
                        + String.join("\n--- vs ---\n", distinctPlans)
                );
            }
            return Optional.of(distinctPlans.first());
        }
        return Optional.empty();
    }

    private static String scrub(String physicalPlan) {
        String scrubbed = FILE_GROUPS.matcher(physicalPlan).replaceAll(FILE_GROUPS_SCRUBBED);
        scrubbed = TOPK_DYNAMIC_FILTER.matcher(scrubbed).replaceAll(TOPK_DYNAMIC_FILTER_KEPT);
        return scrubbed;
    }

    private static String joinLines(List<String> lines) {
        return lines == null ? "" : String.join("\n", lines);
    }

    private static String stripTrailingNewline(String s) {
        return s.endsWith("\n") ? s.substring(0, s.length() - 1) : s;
    }
}
