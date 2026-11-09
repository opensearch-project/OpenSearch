/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.RelNodeUtils.IndexRemapShuttle;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchLateMaterialization;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchSort;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * QTF (Query-Then-Fetch / late materialization) post-CBO rewrite. Two phases:
 *
 * <ol>
 *   <li><b>Detect</b> ({@link #detect}) — read-only walk that runs the allow-list checks,
 *       computes the {@link Detection} bundle ({@code aboveAnchorPhysicalFields},
 *       {@code belowAnchorPhysicalFields}), and applies the skip predicate. Returns
 *       {@code null} when QTF doesn't apply.</li>
 *   <li><b>Rewrite</b> ({@link #applyRewrite}) — pure transform consuming a {@link Detection}.
 *       Builds the narrowed Scan, walks the below chain, declares {@code ___ugsi} on the
 *       ExchangeReducer, swaps the wrapper in for the anchor, and remaps RexNodes in the
 *       above chain.</li>
 * </ol>
 *
 * <h2>Skip predicate</h2>
 * Fire QTF iff {@code aboveAnchorPhysicalFields - belowAnchorPhysicalFields} is non-empty.
 * If every above-anchor reference is already in the reduce-set, the non-QTF path reads
 * the same physical fields and skips the fetch round-trip — strictly cheaper.
 *
 * <h2>Allow-lists</h2>
 * Above-anchor: {@link OpenSearchProject} (no {@code RexOver}), {@link OpenSearchFilter},
 * {@link OpenSearchSort}. {@link OpenSearchAggregate} is rejected for now — group / agg-call
 * remapping under a moving wrapper schema is a follow-up.
 * <p>
 * Below-anchor intermediate ops: {@link OpenSearchFilter}, {@link OpenSearchExchangeReducer},
 * passthrough {@link OpenSearchProject}.
 *
 * <h2>Plan shape after rewrite</h2>
 * <pre>
 *   AboveOps (RexInputRefs remapped)
 *   └── OpenSearchLateMaterialization        (output rowType = aboveAnchorPhysicalFields)
 *        └── OpenSearchSort                  (anchor; collation remapped to narrowed-Scan space)
 *             └── ...
 *                  └── OpenSearchExchangeReducer  (rowType: [reduce-set, ___row_id, ___ugsi])
 *                       └── ...
 *                            └── OpenSearchTableScan   (narrowed: [reduce-set, ___row_id])
 * </pre>
 *
 * @opensearch.internal
 */
public final class OpenSearchLateMaterializationRewriter {

    private static final Logger LOGGER = LogManager.getLogger(OpenSearchLateMaterializationRewriter.class);

    // TODO : Add support for Aggregate, Joins and Union here.
    private static final Set<Class<? extends RelNode>> ABOVE_ALLOWED = Set.of(
        OpenSearchFilter.class,
        OpenSearchProject.class,
        OpenSearchSort.class
    );

    private static final Set<Class<? extends RelNode>> BELOW_INTERMEDIATE_ALLOWED = Set.of(
        OpenSearchFilter.class,
        OpenSearchExchangeReducer.class,
        OpenSearchProject.class
    );

    // TODO : [Human Generated] Don't Delete until fixed.
    // TODO [Design] : We need to create Rewriting for Distributed Query Execution as a separate PlannerPhase.
    // TODO : One categorization that applies is Correctness v/s Performance. So, a RewritePhase -> LateMatRewriter.
    // TODO : Late Materialization is a rewrite done for performance.
    // TODO : Rewrite for TopK Approximation is done for Correctness as a response to an User ExecutionHint in request.

    private OpenSearchLateMaterializationRewriter() {}

    /** Returns the rewritten root iff QTF matched and fired; {@link Optional#empty()} otherwise. */
    public static Optional<RelNode> rewrite(RelNode root) {
        Detection detection = detect(root);
        if (detection == null) return Optional.empty();
        LOGGER.debug(
            "[QTF] fired: aboveAnchorPhysicalFields={}, belowAnchorPhysicalFields={}",
            detection.aboveAnchorPhysicalFields(),
            detection.belowAnchorPhysicalFields()
        );
        return Optional.of(applyRewrite(root, detection));
    }

    // ── Phase 1 — Detect ───────────────────────────────────────────────

    /**
     * Walks the plan, validates allow-lists, computes the data structures rewrite needs,
     * and applies the skip predicate. {@code null} return means "decline QTF."
     */
    private static Detection detect(RelNode root) {
        AnchorContext anchorCtx = findAnchor(root);
        if (anchorCtx == null) return null;
        if (!isAboveAllowed(anchorCtx.aboveAnchorOperators)) {
            LOGGER.debug("[QTF] above-anchor allow-list rejected; skipping rewrite");
            return null;
        }

        BelowChain belowChain = analyzeBelow(anchorCtx.anchor);
        if (belowChain == null) {
            LOGGER.debug("[QTF] below-anchor allow-list rejected; skipping rewrite");
            return null;
        }

        // Single-shard plans don't trigger late materialization.
        if (!belowChain.hasExchangeReducer()) {
            LOGGER.debug("[QTF] single-shard plan (no ExchangeReducer below anchor); skipping rewrite");
            return null;
        }

        Set<String> belowAnchorPhysicalFields = computeBelowAnchorPhysicalFields(anchorCtx.anchor, belowChain);
        LinkedHashSet<String> aboveAnchorPhysicalFields = computeAboveAnchorPhysicalFields(
            anchorCtx.aboveAnchorOperators,
            anchorCtx.anchor
        );

        // Skip predicate: aboveAnchorPhysicalFields - belowAnchorPhysicalFields must be non-empty.
        boolean hasFetchOnly = aboveAnchorPhysicalFields.stream().anyMatch(name -> !belowAnchorPhysicalFields.contains(name));
        if (!hasFetchOnly) {
            LOGGER.debug("[QTF] aboveAnchorPhysicalFields ⊆ belowAnchorPhysicalFields; QTF would not save any I/O — skipping");
            return null;
        }

        return new Detection(
            anchorCtx.anchor,
            anchorCtx.aboveAnchorOperators,
            belowChain,
            belowAnchorPhysicalFields,
            aboveAnchorPhysicalFields
        );
    }

    /**
     * Walks {@code root} downward along the single-input spine, picking the deepest
     * {@link OpenSearchSort} with non-empty collation and non-null fetch as the anchor.
     * Two-Sort shapes ({@code Sort(fetch) ← Sort(collation+fetch) ← ...}) put the lower
     * Sort as anchor and the upper Sort in the above chain (allowed there).
     *
     * <p><b>Scope: single-input plans only.</b> The walk terminates at any operator whose
     * {@code getInputs().size() != 1} — multi-input ops (Join, Union, Intersect, Minus) and
     * leaves (TableScan, Values). QTF across a Join or Union is not handled here and will
     * need dedicated rewriters with multi-input semantics (per-branch detection, merged
     * rebuild, and richer {@code ___row_id}/{@code ___ugsi} encoding to disambiguate which
     * branch a survivor row came from). Tracked in {@link OpenSearchLateMaterialization}'s
     * class-level TODO.
     */
    private static AnchorContext findAnchor(RelNode root) {
        List<RelNode> chainTopDown = new ArrayList<>();
        int deepestAnchorDepth = -1;
        OpenSearchSort deepestAnchor = null;

        RelNode cur = RelNodeUtils.unwrapHep(root);
        while (cur.getInputs().size() == 1) {
            if (cur instanceof OpenSearchSort sort && !sort.getCollation().getFieldCollations().isEmpty() && sort.fetch != null) {
                deepestAnchor = sort;
                deepestAnchorDepth = chainTopDown.size();
            }
            chainTopDown.add(cur);
            cur = RelNodeUtils.unwrapHep(cur.getInput(0));
        }
        if (deepestAnchor == null) return null;
        return new AnchorContext(deepestAnchor, chainTopDown.subList(0, deepestAnchorDepth));
    }

    private static boolean isAboveAllowed(List<RelNode> chain) {
        for (RelNode n : chain) {
            if (!ABOVE_ALLOWED.contains(n.getClass())) return false;
            if (n instanceof OpenSearchProject project && project.containsOver()) return false;
        }
        return true;
    }

    /**
     * Below-chain analysis. Walks {@code anchor.input} down to the Scan. Returns null
     * when an op falls outside the below-allow-list, when a below-Project is non-passthrough,
     * or when there's no Scan at the bottom.
     */
    private static BelowChain analyzeBelow(OpenSearchSort anchor) {
        List<RelNode> chain = new ArrayList<>();
        int[] belowProjOutToScan = null;
        OpenSearchTableScan scan = null;
        boolean hasExchangeReducer = false;
        RelNode n = RelNodeUtils.unwrapHep(anchor.getInput());
        while (n != null) {
            if (n instanceof OpenSearchTableScan s) {
                scan = s;
                break;
            }
            if (!BELOW_INTERMEDIATE_ALLOWED.contains(n.getClass())) return null;
            if (n instanceof OpenSearchExchangeReducer) hasExchangeReducer = true;
            if (n instanceof OpenSearchProject p) {
                if (belowProjOutToScan != null) {
                    LOGGER.debug("[QTF] multiple Projects below anchor — skipping");
                    return null;
                }
                // A RexOver (window function) below the anchor cannot be relocated above the
                // wrapper: window semantics need the full pre-Limit input, but above the wrapper
                // only the K survivors remain. Decline (matches the above-anchor RexOver rule).
                if (p.containsOver()) {
                    LOGGER.debug("[QTF] window function in below-Project — skipping (cannot relocate above wrapper)");
                    return null;
                }
                // Per-slot scan mapping: passthrough refs resolve to a scan column index;
                // expression slots map to -1 (display-only, reproduced above the wrapper during
                // rewrite). A -1 at a slot the anchor's collation references is a DERIVED SORT
                // KEY — still declines (sort can't order on a value computed from columns that
                // QTF would defer; sorting on the raw column ≠ sorting on the expression).
                int[] outToScan = belowProjectSlotMap(p);
                if (collationReferencesDerivedSlot(anchor, outToScan)) {
                    LOGGER.debug("[QTF] anchor sorts on a derived below-Project column — skipping (derived sort key)");
                    return null;
                }
                belowProjOutToScan = outToScan;
            }
            chain.add(n);
            if (n.getInputs().isEmpty()) return null;
            n = RelNodeUtils.unwrapHep(n.getInput(0));
        }
        if (scan == null) return null;
        return new BelowChain(chain, belowProjOutToScan, scan, hasExchangeReducer);
    }

    /**
     * Per-output-slot scan-column index for a below-Project: {@code out[i]} = scan column index
     * when slot {@code i} is a passthrough {@link RexInputRef}, else {@code -1} for an expression
     * slot (display-only; its physical deps are fetched and the expression is reproduced above
     * the wrapper during rewrite).
     */
    private static int[] belowProjectSlotMap(OpenSearchProject p) {
        int[] out = new int[p.getProjects().size()];
        for (int i = 0; i < p.getProjects().size(); i++) {
            out[i] = (p.getProjects().get(i) instanceof RexInputRef ref) ? ref.getIndex() : -1;
        }
        return out;
    }

    /** True iff any anchor collation index lands on an expression ({@code -1}) slot of the below-Project. */
    private static boolean collationReferencesDerivedSlot(OpenSearchSort anchor, int[] belowProjOutToScan) {
        for (RelFieldCollation fc : anchor.getCollation().getFieldCollations()) {
            if (belowProjOutToScan[fc.getFieldIndex()] < 0) return true;
        }
        return false;
    }

    /**
     * {@code BelowAnchorPhysicalFields} = anchor sort cols ∪ below-filter cols, expressed
     * as physical (Scan-level) field names. The narrowed Scan's rowType is built from this
     * set in scan-original order.
     */
    private static Set<String> computeBelowAnchorPhysicalFields(OpenSearchSort anchor, BelowChain belowChain) {
        Set<String> fields = new HashSet<>();
        List<RelDataTypeField> scanFields = belowChain.scan.getRowType().getFieldList();
        // Anchor sort cols (anchor space → scan space → physical name)
        for (RelFieldCollation fc : anchor.getCollation().getFieldCollations()) {
            int scanIdx = (belowChain.belowProjOutToScan == null) ? fc.getFieldIndex() : belowChain.belowProjOutToScan[fc.getFieldIndex()];
            fields.add(scanFields.get(scanIdx).getName());
        }
        // Below-filter cols (already in scan space)
        for (RelNode op : belowChain.chain) {
            if (op instanceof OpenSearchFilter f) {
                for (int refIdx : RelNodeUtils.collectInputRefs(f.getCondition())) {
                    fields.add(scanFields.get(refIdx).getName());
                }
            }
        }
        return fields;
    }

    /**
     * {@code AboveAnchorPhysicalFields} = the union of physical-field deps across the
     * topmost-above-anchor operator's output FSIs, in first-appearance order. Order is
     * the wrapper-output and fetch-RPC order.
     *
     * <p>When {@code aboveAnchorOperators} is empty the anchor itself is the topmost op —
     * its FSIs walk up from the below-chain via inheritance.
     */
    private static LinkedHashSet<String> computeAboveAnchorPhysicalFields(List<RelNode> aboveAnchorOperators, OpenSearchSort anchor) {
        OpenSearchRelNode topmost = aboveAnchorOperators.isEmpty() ? anchor : (OpenSearchRelNode) aboveAnchorOperators.getFirst();
        LinkedHashSet<String> fields = new LinkedHashSet<>();
        for (FieldStorageInfo fsi : topmost.getOutputFieldStorage()) {
            if (!fsi.isDerived()) {
                fields.add(fsi.getFieldName());
            } else {
                fields.addAll(fsi.getDependsOnPhysicalCols());
            }
        }
        return fields;
    }

    // ── Phase 2 — Rewrite ──────────────────────────────────────────────

    /**
     * Pure transform. Builds the narrowed Scan, walks the below chain (declaring
     * {@code ___ugsi} on the ER), rebuilds the anchor's collation, instantiates the wrapper,
     * and remaps RexNodes in the above chain.
     */
    private static RelNode applyRewrite(RelNode root, Detection detection) {
        OpenSearchTableScan origScan = detection.belowChain.scan;

        // 2a. Narrowed Scan: reduce-set in scan-original order + ___row_id.
        NarrowedScan narrowed = buildNarrowedScan(origScan, detection.belowAnchorPhysicalFields);

        // 2b. Walk below chain bottom-up; rebuild operators with narrowed input. ER appends ___ugsi.
        BelowRebuild belowRebuild = rebuildBelowChain(detection.belowChain, narrowed.newScan, narrowed.scanIdxRemap);

        // 2c. Anchor Sort: collation remapped from anchor space to narrowed-Scan space.
        OpenSearchSort newAnchor = rebuildAnchor(detection.anchor, belowRebuild.rebuilt, belowRebuild.anchorSlotRemap);

        // 2d. Wrapper. Output rowType = aboveAnchorPhysicalFields in iteration order.
        OpenSearchLateMaterialization wrapper = buildWrapper(newAnchor, detection.aboveAnchorPhysicalFields, origScan, detection.anchor);

        // 2f. SELECT projection above the wrapper. Without SORT_PROJECT_TRANSPOSE the SELECT-list
        // Project sits BELOW the anchor; its display columns (URL, Title, …) are deferred to the
        // fetch phase, so the projection — expressions and aliases included — is reproduced ABOVE
        // the wrapper with RexInputRefs rebased from scan space to wrapper-output space. Passthrough
        // slots of sort-only columns (not fetched, absent from the wrapper output) are dropped; the
        // above-chain never references them. When there's no below-Project, the wrapper stands alone.
        RelNode aboveWrapper = buildProjectAboveWrapper(wrapper, detection);

        // 2e. Above chain: every op's RexInputRefs remapped by column name from its origChild's
        // rowType to its newChild's rowType. Pass-through ops (Filter, Sort) leak the narrowed
        // rowType upward, so a single immediate-parent remap is insufficient — every above op
        // needs the same treatment, recursively.
        RelNode rewritten = rebuildAboveChain(RelNodeUtils.unwrapHep(root), detection.anchor, aboveWrapper);

        // 2g. Collapse adjacent Projects at the top. The reproduced below-Project (2f) is a bridge
        // exposing the anchor's output schema so the above-chain can graft on by name; when the
        // immediate above-op is itself a Project (the SELECT prune), the two stack. Compose them
        // into one — substituting the bridge's expressions into the outer Project's refs — so the
        // SELECT prune drops to a single Project and no bridge-only column (e.g. a refetched sort
        // key the outer doesn't select) leaks through an intermediate node.
        return mergeAdjacentTopProjects(rewritten);
    }

    /** While the top node is a Project over a Project, compose the two into one. */
    private static RelNode mergeAdjacentTopProjects(RelNode top) {
        while (top instanceof OpenSearchProject outer && RelNodeUtils.unwrapHep(outer.getInput()) instanceof OpenSearchProject inner) {
            List<RexNode> innerExprs = inner.getProjects();
            RexShuttle substitute = new RexShuttle() {
                @Override
                public RexNode visitInputRef(RexInputRef ref) {
                    return innerExprs.get(ref.getIndex());
                }
            };
            List<RexNode> composed = new ArrayList<>(outer.getProjects().size());
            for (RexNode expr : outer.getProjects()) {
                composed.add(expr.accept(substitute));
            }
            top = new OpenSearchProject(
                outer.getCluster(),
                outer.getTraitSet(),
                inner.getInput(),
                composed,
                outer.getRowType(),
                outer.getViableBackends()
            );
        }
        return top;
    }

    /**
     * Reproduces the below-anchor SELECT Project ABOVE the wrapper. Each below-Project output slot
     * becomes one output here, with the SAME name (preserving aliases) and its RexNode rebased from
     * scan-column space to wrapper-output (physical-field) space:
     * <ul>
     *   <li>passthrough ref to a fetched column → ref to that column's wrapper-output index;</li>
     *   <li>passthrough ref to a sort-only column (not fetched, absent from the wrapper) → dropped;</li>
     *   <li>expression → same RexCall with operands rebased (all its physical deps are fetched, so
     *       every operand resolves into the wrapper output).</li>
     * </ul>
     * Returns the bare {@code wrapper} when there is no below-Project to reproduce.
     */
    private static RelNode buildProjectAboveWrapper(OpenSearchLateMaterialization wrapper, Detection detection) {
        OpenSearchProject innerProject = null;
        for (RelNode op : detection.belowChain.chain) {
            if (op instanceof OpenSearchProject p) {
                innerProject = p;
                break;
            }
        }
        if (innerProject == null) return wrapper;

        RelDataTypeFactory typeFactory = wrapper.getCluster().getTypeFactory();
        List<RelDataTypeField> scanFields = detection.belowChain.scan.getRowType().getFieldList();
        List<RelDataTypeField> wrapperFields = wrapper.getRowType().getFieldList();

        // scan-column index → wrapper-output index, by physical name; -1 when that column was not fetched.
        Map<String, Integer> wrapperIdxByName = new HashMap<>(wrapperFields.size());
        for (int i = 0; i < wrapperFields.size(); i++) {
            wrapperIdxByName.put(wrapperFields.get(i).getName(), i);
        }
        int[] scanToWrapper = new int[scanFields.size()];
        for (int k = 0; k < scanFields.size(); k++) {
            Integer w = wrapperIdxByName.get(scanFields.get(k).getName());
            scanToWrapper[k] = (w == null) ? -1 : w;
        }
        IndexRemapShuttle toWrapper = new IndexRemapShuttle(scanToWrapper, wrapper.getRowType());

        List<RexNode> projects = new ArrayList<>();
        List<String> names = new ArrayList<>();
        List<RelDataTypeField> innerFields = innerProject.getRowType().getFieldList();
        for (int slot = 0; slot < innerProject.getProjects().size(); slot++) {
            RexNode expr = innerProject.getProjects().get(slot);
            RexNode rebased;
            if (expr instanceof RexInputRef ref) {
                int w = scanToWrapper[ref.getIndex()];
                if (w < 0) continue; // sort-only passthrough column — not fetched, drop
                rebased = new RexInputRef(w, wrapperFields.get(w).getType());
            } else {
                rebased = expr.accept(toWrapper); // expression: all physical deps are fetched
            }
            projects.add(rebased);
            names.add(innerFields.get(slot).getName());
        }

        RelDataTypeFactory.Builder rowType = typeFactory.builder();
        for (int i = 0; i < projects.size(); i++) {
            rowType.add(names.get(i), projects.get(i).getType());
        }
        return new OpenSearchProject(
            wrapper.getCluster(),
            wrapper.getTraitSet(),
            wrapper,
            projects,
            rowType.build(),
            innerProject.getViableBackends()
        );
    }

    // ── 2a. Narrowed Scan ─────────────────────────────────────────────

    private static NarrowedScan buildNarrowedScan(OpenSearchTableScan origScan, Set<String> belowAnchorPhysicalFields) {
        RelDataTypeFactory typeFactory = origScan.getCluster().getTypeFactory();
        List<RelDataTypeField> origFields = origScan.getRowType().getFieldList();
        List<FieldStorageInfo> origStorage = origScan.getOutputFieldStorage();

        RelDataTypeFactory.Builder rowTypeBuilder = typeFactory.builder();
        List<FieldStorageInfo> newStorage = new ArrayList<>(belowAnchorPhysicalFields.size() + 1);
        int[] scanIdxRemap = new int[origFields.size()];
        Arrays.fill(scanIdxRemap, -1);

        int nextNewIdx = 0;
        for (int origIdx = 0; origIdx < origFields.size(); origIdx++) {
            RelDataTypeField field = origFields.get(origIdx);
            if (belowAnchorPhysicalFields.contains(field.getName())) {
                scanIdxRemap[origIdx] = nextNewIdx++;
                rowTypeBuilder.add(field.getName(), field.getType());
                newStorage.add(origStorage.get(origIdx));
            }
        }
        rowTypeBuilder.add(OpenSearchLateMaterialization.ROW_ID_FIELD, typeFactory.createSqlType(SqlTypeName.BIGINT));
        newStorage.add(FieldStorageInfo.derivedColumn(OpenSearchLateMaterialization.ROW_ID_FIELD, SqlTypeName.BIGINT));

        OpenSearchTableScan newScan = new OpenSearchTableScan(
            origScan.getCluster(),
            origScan.getTraitSet(),
            origScan.getTable(),
            origScan.getViableBackends(),
            newStorage,
            rowTypeBuilder.build()
        );
        return new NarrowedScan(newScan, scanIdxRemap);
    }

    // ── 2b. Below chain rebuild ───────────────────────────────────────

    private static BelowRebuild rebuildBelowChain(BelowChain belowChain, RelNode newScan, int[] scanIdxRemap) {
        RelDataTypeFactory typeFactory = newScan.getCluster().getTypeFactory();
        RelNode rebuilt = newScan;
        int[] anchorSlotRemap = scanIdxRemap;

        for (int i = belowChain.chain.size() - 1; i >= 0; i--) {
            RelNode orig = belowChain.chain.get(i);
            switch (orig) {
                case OpenSearchFilter f -> {
                    RexNode remapped = f.getCondition().accept(new IndexRemapShuttle(scanIdxRemap, rebuilt.getRowType()));
                    rebuilt = new OpenSearchFilter(f.getCluster(), f.getTraitSet(), rebuilt, remapped, f.getViableBackends());
                }
                case OpenSearchProject p -> {
                    BelowProjectRebuild rebuild = rebuildBelowProject(p, rebuilt, scanIdxRemap, typeFactory);
                    rebuilt = rebuild.rebuilt;
                    anchorSlotRemap = rebuild.outputRemap;
                }
                case OpenSearchExchangeReducer er -> {
                    // Invariant 4: declare ___ugsi on the ER's output rowType (materialized at runtime
                    // by OrdinalAppendingSink before DataFusion's reduce sees the batch). Supply the
                    // matching FieldStorageInfo so the ER's rowType and FSI stay aligned 1:1 — ___ugsi
                    // is a derived (coord-side) column with no physical storage.
                    RelDataType erRowType = RelNodeUtils.appendField(
                        typeFactory,
                        rebuilt.getRowType(),
                        OpenSearchLateMaterialization.UGSI_FIELD,
                        typeFactory.createSqlType(SqlTypeName.INTEGER)
                    );
                    List<FieldStorageInfo> erStorage = new ArrayList<>(((OpenSearchRelNode) rebuilt).getOutputFieldStorage());
                    erStorage.add(FieldStorageInfo.derivedColumn(OpenSearchLateMaterialization.UGSI_FIELD, SqlTypeName.INTEGER));
                    rebuilt = new OpenSearchExchangeReducer(
                        er.getCluster(),
                        er.getTraitSet(),
                        rebuilt,
                        er.getViableBackends(),
                        er.getExchangeInfo(),
                        erRowType,
                        erStorage
                    );
                }
                default -> throw new IllegalStateException("Unexpected below-anchor operator: " + orig.getClass().getSimpleName());
            }
        }
        return new BelowRebuild(rebuilt, anchorSlotRemap);
    }

    private static BelowProjectRebuild rebuildBelowProject(
        OpenSearchProject p,
        RelNode newChild,
        int[] scanIdxRemap,
        RelDataTypeFactory typeFactory
    ) {
        // Below-Project output→scan map: passthrough slots give a scan index; expression slots
        // give -1 (display-only; reproduced above the wrapper, never rebuilt below). Both kinds
        // are dropped from the rebuilt below-Project — only sort/filter passthrough cols that
        // survive into the narrowed Scan are kept.
        int[] origOutToScan = belowProjectSlotMap(p);

        List<RexNode> newProjects = new ArrayList<>();
        List<String> newNames = new ArrayList<>();
        int[] outputRemap = new int[origOutToScan.length];
        Arrays.fill(outputRemap, -1);
        for (int origOut = 0; origOut < origOutToScan.length; origOut++) {
            if (origOutToScan[origOut] < 0) continue; // expression slot — pulled up above the wrapper
            int newScanIdx = scanIdxRemap[origOutToScan[origOut]];
            if (newScanIdx < 0) continue; // source dropped (now fetched, not in narrowed Scan)
            RelDataTypeField field = newChild.getRowType().getFieldList().get(newScanIdx);
            outputRemap[origOut] = newProjects.size();
            newProjects.add(new RexInputRef(newScanIdx, field.getType()));
            newNames.add(p.getRowType().getFieldList().get(origOut).getName());
        }
        // Pass through every trailing helper present in the child, by NAME, in the shared layout
        // order. When this below-Project sits above the ExchangeReducer the child already carries
        // ___ugsi (appended by the ER), so a positional "last column" lookup would grab ___ugsi
        // instead of ___row_id and drop the helper that the LM-stage drain / Stitcher read by name.
        List<RelDataTypeField> childFields = newChild.getRowType().getFieldList();
        for (String helper : OpenSearchLateMaterialization.TRAILING_HELPERS_IN_ORDER) {
            for (int i = 0; i < childFields.size(); i++) {
                if (helper.equals(childFields.get(i).getName())) {
                    newProjects.add(new RexInputRef(i, childFields.get(i).getType()));
                    newNames.add(helper);
                    break;
                }
            }
        }

        RelDataTypeFactory.Builder pb = typeFactory.builder();
        for (int j = 0; j < newProjects.size(); j++) {
            pb.add(newNames.get(j), newProjects.get(j).getType());
        }
        OpenSearchProject rebuilt = new OpenSearchProject(
            p.getCluster(),
            p.getTraitSet(),
            newChild,
            newProjects,
            pb.build(),
            p.getViableBackends()
        );
        return new BelowProjectRebuild(rebuilt, outputRemap);
    }

    // ── 2c. Anchor rebuild ────────────────────────────────────────────

    private static OpenSearchSort rebuildAnchor(OpenSearchSort anchor, RelNode newInput, int[] anchorSlotRemap) {
        List<RelFieldCollation> newCollations = new ArrayList<>(anchor.getCollation().getFieldCollations().size());
        for (RelFieldCollation fc : anchor.getCollation().getFieldCollations()) {
            int newIdx = anchorSlotRemap[fc.getFieldIndex()];
            if (newIdx < 0) {
                throw new IllegalStateException(
                    "Sort collation references slot " + fc.getFieldIndex() + " whose physical field was dropped from the narrowed Scan"
                );
            }
            newCollations.add(fc.withFieldIndex(newIdx));
        }
        return new OpenSearchSort(
            anchor.getCluster(),
            anchor.getTraitSet(),
            newInput,
            RelCollations.of(newCollations),
            anchor.offset,
            anchor.fetch,
            anchor.getViableBackends()
        );
    }

    // ── 2d. Wrapper ───────────────────────────────────────────────────

    /**
     * Builds the {@link OpenSearchLateMaterialization} wrapper. Output rowType has one
     * field per element of {@code aboveAnchorPhysicalFields} (in iteration order), each
     * named with the physical field name and typed from the original Scan.
     */
    private static OpenSearchLateMaterialization buildWrapper(
        OpenSearchSort newAnchor,
        LinkedHashSet<String> aboveAnchorPhysicalFields,
        OpenSearchTableScan origScan,
        OpenSearchSort origAnchor
    ) {
        Map<String, Integer> origScanIdxByName = new HashMap<>();
        List<RelDataTypeField> origFields = origScan.getRowType().getFieldList();
        for (int i = 0; i < origFields.size(); i++) {
            origScanIdxByName.put(origFields.get(i).getName(), i);
        }
        List<FieldStorageInfo> origStorage = origScan.getOutputFieldStorage();

        List<RelDataTypeField> wrapperFields = new ArrayList<>(aboveAnchorPhysicalFields.size());
        List<FieldStorageInfo> wrapperStorage = new ArrayList<>(aboveAnchorPhysicalFields.size());
        for (String name : aboveAnchorPhysicalFields) {
            Integer scanIdx = origScanIdxByName.get(name);
            if (scanIdx == null) {
                throw new IllegalStateException(
                    "aboveAnchorPhysicalFields references [" + name + "] which is not present in the original Scan rowType"
                );
            }
            wrapperFields.add(origFields.get(scanIdx));
            wrapperStorage.add(origStorage.get(scanIdx));
        }

        return new OpenSearchLateMaterialization(
            newAnchor.getCluster(),
            newAnchor.getTraitSet(),
            newAnchor,
            wrapperFields,
            wrapperStorage,
            origAnchor.getViableBackends()
        );
    }

    // ── 2e. Above chain rebuild ───────────────────────────────────────

    /**
     * Walks down the above chain, swapping the anchor's slot with {@code wrapperOrProject}
     * (the wrapper, or the SELECT Project sitting above it), then on the way up rewrites every
     * op's RexInputRefs via a by-name remap from {@code origChild}'s rowType to {@code newChild}'s
     * rowType. Names are the stable identity that survives narrowing — they exist in both rowTypes
     * verbatim for kept columns, and resolve to -1 (rejected by {@link IndexRemapShuttle}) for
     * dropped ones.
     */
    private static RelNode rebuildAboveChain(RelNode current, OpenSearchSort origAnchor, RelNode wrapperOrProject) {
        if (current == origAnchor) return wrapperOrProject;
        if (current.getInputs().size() != 1) {
            throw new IllegalStateException("Multi-input parent in QTF chain: " + current.getClass().getSimpleName());
        }
        RelNode origChild = RelNodeUtils.unwrapHep(current.getInput(0));
        RelNode newChild = rebuildAboveChain(origChild, origAnchor, wrapperOrProject);

        int[] remap = buildByNameRemap(origChild.getRowType(), newChild.getRowType());
        IndexRemapShuttle shuttle = new IndexRemapShuttle(remap, newChild.getRowType());

        switch (current) {
            case OpenSearchProject project -> {
                List<RexNode> newExprs = new ArrayList<>(project.getProjects().size());
                for (RexNode expr : project.getProjects()) {
                    newExprs.add(expr.accept(shuttle));
                }
                return project.copy(project.getTraitSet(), newChild, newExprs, project.getRowType());
            }
            case OpenSearchFilter filter -> {
                return filter.copy(filter.getTraitSet(), newChild, filter.getCondition().accept(shuttle));
            }
            case OpenSearchSort sort -> {
                List<RelFieldCollation> remapped = new ArrayList<>(sort.getCollation().getFieldCollations().size());
                for (RelFieldCollation fc : sort.getCollation().getFieldCollations()) {
                    int newIdx = remap[fc.getFieldIndex()];
                    if (newIdx < 0) {
                        throw new IllegalStateException(
                            "Above-anchor Sort references column at slot " + fc.getFieldIndex() + " not present in narrowed rowType"
                        );
                    }
                    remapped.add(fc.withFieldIndex(newIdx));
                }
                return sort.copy(sort.getTraitSet(), newChild, RelCollations.of(remapped), sort.offset, sort.fetch);
            }
            default -> throw new IllegalStateException("Unexpected above-anchor operator: " + current.getClass().getSimpleName());
        }
    }

    /** Maps each index of {@code origType} to the index of the same-named field in {@code newType}, or -1 if dropped. */
    private static int[] buildByNameRemap(RelDataType origType, RelDataType newType) {
        Map<String, Integer> newIdxByName = new HashMap<>(newType.getFieldCount());
        List<RelDataTypeField> newFields = newType.getFieldList();
        for (int i = 0; i < newFields.size(); i++) {
            newIdxByName.put(newFields.get(i).getName(), i);
        }
        List<RelDataTypeField> origFields = origType.getFieldList();
        int[] remap = new int[origFields.size()];
        for (int i = 0; i < origFields.size(); i++) {
            Integer mapped = newIdxByName.get(origFields.get(i).getName());
            remap[i] = mapped == null ? -1 : mapped;
        }
        return remap;
    }

    // ── Records ────────────────────────────────────────────────────────

    /**
     * Output of phase 1. Contains everything phase 2 needs — phase 2 must not re-walk
     * the original plan to recover any of these fields.
     */
    private record Detection(OpenSearchSort anchor, List<RelNode> aboveAnchorOperators, BelowChain belowChain, Set<
        String> belowAnchorPhysicalFields, LinkedHashSet<String> aboveAnchorPhysicalFields) {
    }

    private record AnchorContext(OpenSearchSort anchor, List<RelNode> aboveAnchorOperators) {
    }

    private record BelowChain(List<RelNode> chain, int[] belowProjOutToScan, OpenSearchTableScan scan, boolean hasExchangeReducer) {
    }

    private record NarrowedScan(OpenSearchTableScan newScan, int[] scanIdxRemap) {
    }

    private record BelowRebuild(RelNode rebuilt, int[] anchorSlotRemap) {
    }

    private record BelowProjectRebuild(OpenSearchProject rebuilt, int[] outputRemap) {
    }
}
