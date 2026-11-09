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
    private static final Pattern TOPK_DYNAMIC_FILTER =
        Pattern.compile("(preserve_partitioning=\\[[^\\]]*\\]), filter=\\[[^\\]]*\\]");
    private static final String TOPK_DYNAMIC_FILTER_KEPT = "$1";

    // input_partitions is deliberately NOT scrubbed: it equals the shard's segment count, which is a
    // controlled axis (captured per segment layout), so it's deterministic and worth asserting.

    // DataSourceExec DynamicFilter predicate: the TopK optimization pushes a boundary filter down into
    // DataSourceExec as `predicate=DynamicFilter [ <boundary expr> ]`. The boundary is a runtime value
    // that depends on data seen so far by the TopK heap — in multi-shard configs, each shard sees
    // different data and arrives at a different boundary, so both the DynamicFilter body AND its
    // derived pruning_predicate diverge across shards. We scrub:
    //   (a) Pure DynamicFilter predicate: when the entire predicate= is a DynamicFilter (no static
    //       WHERE clause), the pruning_predicate is entirely derived from it — scrub both.
    //   (b) Mixed predicate: when DynamicFilter is appended to static predicates (e.g.
    //       "predicate=X AND DynamicFilter [ empty ]"), only the DynamicFilter body is scrubbed;
    //       the static predicates and their pruning_predicate remain for assertion.
    private static final Pattern PURE_DYNAMIC_FILTER_PREDICATE = Pattern.compile(
        "predicate=DynamicFilter \\[[^\\]]*], pruning_predicate=[^,]*, ");
    private static final String PURE_DYNAMIC_FILTER_SCRUBBED =
        "predicate=DynamicFilter [ " + SCRUBBED + " ], pruning_predicate=" + SCRUBBED + ", ";

    private static final Pattern MIXED_DYNAMIC_FILTER_BODY = Pattern.compile(
        "DynamicFilter \\[[^\\]]*]");
    private static final String MIXED_DYNAMIC_FILTER_BODY_SCRUBBED =
        "DynamicFilter [ " + SCRUBBED + " ]";

    // Constant-folded aggregates: when an aggregate (e.g. min/max) can be resolved at plan-time from
    // parquet metadata, DataFusion folds the entire subtree to a ProjectionExec of literal constants
    // fed by PlaceholderRowExec. The literal values are data-dependent (each shard holds different
    // data), so they diverge across shards in multi-shard configs. We scrub bare numeric literals in
    // such projections. Safety: if the optimizer stops constant-folding (regression), PlaceholderRowExec
    // vanishes and the rule no longer fires — the structural change is still caught.
    private static final Pattern CONSTANT_FOLDED_PROJECTION = Pattern.compile(
        "(ProjectionExec: expr=\\[)([^\\]]+)(\\]\\s*\\n\\s*PlaceholderRowExec)"
    );
    /** Bare numeric literal as the expression side of "expr as alias" within a ProjectionExec. */
    private static final Pattern BARE_NUMERIC_LITERAL = Pattern.compile("(?<=^|, )-?\\d+(?= as )");

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
        // DynamicFilter scrub: pure first (scrubs both predicate + pruning_predicate), then mixed
        // (scrubs only the DynamicFilter body, leaving static predicates intact).
        scrubbed = PURE_DYNAMIC_FILTER_PREDICATE.matcher(scrubbed).replaceAll(PURE_DYNAMIC_FILTER_SCRUBBED);
        scrubbed = MIXED_DYNAMIC_FILTER_BODY.matcher(scrubbed).replaceAll(MIXED_DYNAMIC_FILTER_BODY_SCRUBBED);
        scrubbed = scrubConstantFoldedProjections(scrubbed);
        return scrubbed;
    }

    /**
     * Scrub data-dependent numeric literals in constant-folded projections (ProjectionExec above
     * PlaceholderRowExec). Replaces bare numeric values with {@code <const>} so per-shard variation
     * in folded aggregates (min/max/count on different data slices) doesn't break cross-shard dedup.
     */
    private static String scrubConstantFoldedProjections(String plan) {
        java.util.regex.Matcher m = CONSTANT_FOLDED_PROJECTION.matcher(plan);
        StringBuilder sb = new StringBuilder();
        while (m.find()) {
            String exprs = m.group(2);
            String scrubbedExprs = BARE_NUMERIC_LITERAL.matcher(exprs).replaceAll(SCRUBBED);
            m.appendReplacement(sb, java.util.regex.Matcher.quoteReplacement(m.group(1) + scrubbedExprs + m.group(3)));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    private static String joinLines(List<String> lines) {
        return lines == null ? "" : String.join("\n", lines);
    }

    private static String stripTrailingNewline(String s) {
        return s.endsWith("\n") ? s.substring(0, s.length() - 1) : s;
    }
}
