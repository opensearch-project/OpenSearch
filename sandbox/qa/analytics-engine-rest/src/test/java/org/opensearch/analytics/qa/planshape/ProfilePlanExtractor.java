/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa.planshape;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

    // FIXME [RemoveBeforeMerge]: verbose diagnostics while bringing up the harness.
    private static final Logger LOGGER = LogManager.getLogger(ProfilePlanExtractor.class);

    /** Single consistent token for every non-deterministic value we redact from a plan. */
    private static final String SCRUBBED = "<scrubbed>";

    /** Host/UUID/shard/generation-specific parquet path. */
    private static final Pattern FILE_GROUPS = Pattern.compile("file_groups=\\{[^}]*\\}");
    private static final String FILE_GROUPS_SCRUBBED = "file_groups={" + SCRUBBED + "}";

    // DataFusion's TopK dynamic-filter pushdown appends a `, filter=[<expr> > N]` clause to a
    // SortExec TopK line. It is a RUNTIME watermark, not plan structure: the SortExec shares a
    // mutable DynamicFilterPhysicalExpr Arc with the running TopK operator, and the rendered value
    // is whatever threshold the heap reached at display time — it only appears once a partition's
    // heap fills to k=fetch rows. Its very PRESENCE varies with partition/segment count (1 segment
    // -> heap fills -> shown; 2 segments -> each partial heap stays under k -> absent). That makes
    // it a non-plan-shape, capture-time-dependent token, so strip the whole clause (delete, don't
    // tokenize: the nondeterminism is presence vs absence, which a placeholder wouldn't unify).
    // Anchored on the preceding preserve_partitioning=[...] so only the SortExec dynamic filter is
    // removed (DataSourceExec uses predicate=/required_guarantees, never this `, filter=[...]`).
    private static final Pattern TOPK_DYNAMIC_FILTER =
        Pattern.compile("(preserve_partitioning=\\[[^\\]]*\\]), filter=\\[[^\\]]*\\]");
    private static final String TOPK_DYNAMIC_FILTER_KEPT = "$1";

    // NOTE: input_partitions is NOT scrubbed. It reflects the shard's parquet segment count, which
    // is now a CONTROLLED axis — every combo is captured under both a single-segment and a
    // multi-segment index variant, so input_partitions is deterministic per variant and meaningful
    // to assert. Guiding principle: control the input (pin segment topology), don't erase the
    // output. The only scrub left is the file_groups absolute path (genuinely host/UUID/gen
    // specific, not plan shape, not controllable).

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
    public static ProfilePlanExtractor from(Map<String, Object> response, String indexName) {
        // FIXME [RemoveBeforeMerge]: verbose bring-up diagnostics.
        LOGGER.info("PLAN-SHAPE DIAG: extracting for index={}, response top-keys={}", indexName, response.keySet());
        Map<String, Object> profile = (Map<String, Object>) response.get("profile");
        if (profile == null) {
            LOGGER.error("PLAN-SHAPE DIAG: NO profile block. full response={}", response);
            throw new AssertionError("response has no 'profile' block — request must set profile=true");
        }
        // full_plan + stages live under profile.plan in the nested profile shape
        // ({ summary, plan: {...}, phases }); some builds emit them flat directly on profile.
        // Accept either: use profile.plan when present, else profile itself.
        Map<String, Object> plan = profile.containsKey("plan")
            ? (Map<String, Object>) profile.get("plan")
            : profile;
        if (plan == null || plan.get("stages") == null) {
            LOGGER.error("PLAN-SHAPE DIAG: no stages found. profile keys={} response={}", profile.keySet(), response);
            throw new AssertionError("profile has no stages; profile keys=" + profile.keySet());
        }
        LOGGER.info("PLAN-SHAPE DIAG: profile keys={}, plan keys={}, full_plan lines={}",
            profile.keySet(), plan.keySet(), plan.get("full_plan") == null ? "null" : ((List<?>) plan.get("full_plan")).size());

        List<Map<String, Object>> stages = (List<Map<String, Object>>) plan.get("stages");
        if (stages == null) {
            LOGGER.error("PLAN-SHAPE DIAG: profile.stages is NULL. profile keys={} full response={}", profile.keySet(), response);
            throw new AssertionError("profile.stages is null; profile keys=" + profile.keySet() + " response=" + response);
        }
        LOGGER.info("PLAN-SHAPE DIAG: {} stage(s): {}", stages.size(),
            stages.stream().map(s -> s.get("execution_type") + "(backend=" + s.get("chosen_backend")
                + ",tasks=" + (s.get("tasks") == null ? "null" : ((List<?>) s.get("tasks")).size()) + ")").toList());

        Map<PlanShapeLayer, Optional<String>> layers = new LinkedHashMap<>();
        layers.put(PlanShapeLayer.POST_CBO, Optional.of(joinLines((List<String>) plan.get("full_plan"))));
        layers.put(PlanShapeLayer.FRAGMENT, Optional.of(renderFragment(stages)));
        layers.put(PlanShapeLayer.SHARD_PHYSICAL, physicalOf(stages, SHARD_FRAGMENT));
        layers.put(PlanShapeLayer.COORD_PHYSICAL, physicalOf(stages, COORDINATOR_REDUCE));

        layers.replaceAll((layer, text) -> text.map(t -> t.replace(indexName, INDEX_TOKEN)));
        layers.forEach((layer, text) -> LOGGER.info("PLAN-SHAPE DIAG: layer {} -> {}",
            layer, text.isEmpty() ? "ABSENT" : (text.get().split("\n", 2)[0] + " ...(" + text.get().length() + " chars)")));
        return new ProfilePlanExtractor(layers);
    }

    // TODO(plan-shape): FRAGMENT renders the PRE-FORK marked fragment. Once the profile exposes
    // post-fork `alternatives` (see QueryProfileBuilder TODO), assert on those instead.
    @SuppressWarnings("unchecked")
    private static String renderFragment(List<Map<String, Object>> stages) {
        StringBuilder sb = new StringBuilder();
        for (Map<String, Object> stage : stages) {
            // tree_shape is the delegation shape (CONJUNCTIVE / INTERLEAVED_BOOLEAN_EXPRESSION /
            // ...) and MUST be asserted. "Absent" serializes inconsistently across builds (JSON
            // null vs the string "None"); canonicalize both to NONE so the value is always present
            // and node-agnostic.
            Object treeShape = stage.get("tree_shape");
            String treeShapeStr = (treeShape == null || "None".equals(treeShape)) ? "NONE" : treeShape.toString();
            sb.append('[')
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
                    sb.append(line).append('\n');
                }
            }
        }
        return stripTrailingNewline(sb.toString());
    }

    /**
     * The scrubbed, deduped physical plan for the stage of the given execution type, or empty if
     * the stage is absent or carries no task physical plan (Lucene / empty shard). All tasks of a
     * stage are expected to be identical after scrubbing; a divergence is a real finding and fails.
     */
    @SuppressWarnings("unchecked")
    private static Optional<String> physicalOf(List<Map<String, Object>> stages, String executionType) {
        for (Map<String, Object> stage : stages) {
            if (!executionType.equals(stage.get("execution_type"))) {
                continue;
            }
            List<Map<String, Object>> tasks = (List<Map<String, Object>>) stage.get("tasks");
            if (tasks == null) {
                LOGGER.info("PLAN-SHAPE DIAG: {} has no tasks -> physical ABSENT", executionType);
                return Optional.empty();
            }
            TreeSet<String> distinct = new TreeSet<>();
            int withPlan = 0;
            for (Map<String, Object> task : tasks) {
                Object plan = task.get("physical_plan");
                if (plan instanceof String s && !s.isEmpty()) {
                    withPlan++;
                    distinct.add(scrub(s));
                }
            }
            LOGGER.info("PLAN-SHAPE DIAG: {} -> {} task(s), {} with physical_plan, {} distinct after scrub",
                executionType, tasks.size(), withPlan, distinct.size());
            if (distinct.isEmpty()) {
                return Optional.empty();
            }
            if (distinct.size() > 1) {
                // FIXME [RemoveBeforeMerge]: pinpoint which line(s) differ between the two variants.
                String[] a = distinct.first().split("\n", -1);
                String[] b = distinct.last().split("\n", -1);
                for (int i = 0; i < Math.max(a.length, b.length); i++) {
                    String la = i < a.length ? a[i] : "<none>";
                    String lb = i < b.length ? b[i] : "<none>";
                    if (!la.equals(lb)) {
                        LOGGER.error("PLAN-SHAPE DIAG: {} divergence at line {}:\n  A: {}\n  B: {}", executionType, i, la, lb);
                    }
                }
                throw new AssertionError(
                    "tasks of stage " + executionType + " produced differing physical plans after scrub:\n"
                        + String.join("\n--- vs ---\n", distinct)
                );
            }
            return Optional.of(distinct.first());
        }
        LOGGER.info("PLAN-SHAPE DIAG: no {} stage present -> physical ABSENT", executionType);
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
