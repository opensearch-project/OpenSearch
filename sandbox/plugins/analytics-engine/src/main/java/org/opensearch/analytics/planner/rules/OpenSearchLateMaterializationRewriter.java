/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
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
 *       {@code belowAnchorPhysicalFields}, {@code anchorSlotToPhysicalField}), and applies
 *       the skip predicate. Returns {@code null} when QTF doesn't apply.</li>
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
    // TODO : Rewrite for TopK Approximation is done for Correctness as a response to an User ExecutionHint in request.

    private OpenSearchLateMaterializationRewriter() {}

    /** Returns the rewritten root iff QTF matched and fired; {@link Optional#empty()} otherwise. */
    public static Optional<RelNode> rewrite(RelNode root) {
        Detection detection = detect(root);
        if (detection == null) return Optional.empty();
        // DEBUG (off by default in prod). Tests flip the logger via cluster-settings to assert
        // engagement; production stays log-quiet on the hot path.
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

        BelowChain belowChain = analyzeBelow(anchorCtx.anchor.getInput());
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

        List<String> anchorSlotToPhysicalField = buildAnchorSlotToPhysicalField(belowChain);

        return new Detection(
            anchorCtx.anchor,
            anchorCtx.aboveAnchorOperators,
            belowChain,
            belowAnchorPhysicalFields,
            aboveAnchorPhysicalFields,
            anchorSlotToPhysicalField
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
    private static BelowChain analyzeBelow(RelNode subtree) {
        List<RelNode> chain = new ArrayList<>();
        int[] belowProjOutToScan = null;
        OpenSearchTableScan scan = null;
        boolean hasExchangeReducer = false;
        RelNode n = RelNodeUtils.unwrapHep(subtree);
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
                int[] outToScan = passthroughMap(p);
                if (outToScan == null) {
                    // TODO: derived below-Project lost-opportunity case. Today's algorithm
                    // declines plans like:
                    // SELECT description FROM hits ORDER BY UPPER(URL) LIMIT 10
                    // → Project(description) ← Sort($1 ASC) ← Project(description, UPPER(URL)) ← Scan
                    // The derived col (UPPER(URL)) is consumed only by the anchor's collation; we
                    // *could* push the derived expression above the wrapper, narrow the Scan to
                    // {URL}, sort below, and fetch {description} for survivors. Skipping for now —
                    // adding requires threading the derived RexNode into Detection and emitting
                    // a synthesized above-Project during rewrite. Separate slice. The non-QTF
                    // path remains correct in the meantime.
                    LOGGER.debug("[QTF] expression below-Project — skipping (derived-pushup not yet implemented)");
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

    /** Returns output→scanIdx map iff every project is a {@link RexInputRef}; else null. */
    private static int[] passthroughMap(OpenSearchProject p) {
        int[] out = new int[p.getProjects().size()];
        for (int i = 0; i < p.getProjects().size(); i++) {
            if (!(p.getProjects().get(i) instanceof RexInputRef ref)) return null;
            out[i] = ref.getIndex();
        }
        return out;
    }

    /**
     * {@code BelowAnchorPhysicalFields} = anchor sort cols ∪ below-filter cols, expressed
     * as physical (Scan-level) field names. The narrowed Scan's rowType is built from this
     * set in scan-original order.
     */
    private static Set<String> computeBelowAnchorPhysicalFields(OpenSearchSort anchor, BelowChain belowChain) {
        Set<String> fields = new java.util.HashSet<>();
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
        OpenSearchRelNode topmost = aboveAnchorOperators.isEmpty()
            ? (OpenSearchRelNode) anchor
            : (OpenSearchRelNode) aboveAnchorOperators.get(0);
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

    /**
     * Builds {@code anchorSlotToPhysicalField}: for each slot in {@code anchor.rowType},
     * the physical field name it ultimately reads from. Used during rewrite for above-op
     * RexInputRef remapping (anchor.slot → physicalName → wrapperOut.idx).
     */
    private static List<String> buildAnchorSlotToPhysicalField(BelowChain belowChain) {
        // anchor.rowType inherits from belowChain.chain[0]'s rowType (which inherits ER → Filter → Scan).
        RelNode topBelowOp = belowChain.chain.isEmpty() ? belowChain.scan : belowChain.chain.get(0);
        int slotCount = topBelowOp.getRowType().getFieldCount();
        List<RelDataTypeField> scanFields = belowChain.scan.getRowType().getFieldList();
        List<String> out = new ArrayList<>(slotCount);
        for (int slot = 0; slot < slotCount; slot++) {
            int scanIdx = (belowChain.belowProjOutToScan == null) ? slot : belowChain.belowProjOutToScan[slot];
            out.add(scanFields.get(scanIdx).getName());
        }
        return out;
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

        // 2e. Above chain: RexInputRef remap via anchor.slot → physicalName → wrapperOut.idx.
        Map<String, Integer> physToWrapperIdx = new HashMap<>(detection.aboveAnchorPhysicalFields.size());
        int wrapperIdx = 0;
        for (String name : detection.aboveAnchorPhysicalFields) {
            physToWrapperIdx.put(name, wrapperIdx++);
        }
        int[] anchorToWrapperOut = new int[detection.anchorSlotToPhysicalField.size()];
        for (int slot = 0; slot < anchorToWrapperOut.length; slot++) {
            Integer mapped = physToWrapperIdx.get(detection.anchorSlotToPhysicalField.get(slot));
            // -1 when this anchor slot's physical field isn't in the wrapper output (e.g. sort-only or
            // helper). Above-ops shouldn't reference such slots — the above-walk only fed
            // physical names into aboveAnchorPhysicalFields by following actual references.
            anchorToWrapperOut[slot] = mapped == null ? -1 : mapped;
        }
        return rebuildAboveChain(RelNodeUtils.unwrapHep(root), detection.anchor, wrapper, anchorToWrapperOut);
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
                    // by OrdinalAppendingSink before DataFusion's reduce sees the batch).
                    RelDataType erRowType = RelNodeUtils.appendField(
                        typeFactory,
                        rebuilt.getRowType(),
                        OpenSearchLateMaterialization.UGSI_FIELD,
                        typeFactory.createSqlType(SqlTypeName.INTEGER)
                    );
                    rebuilt = new OpenSearchExchangeReducer(
                        er.getCluster(),
                        er.getTraitSet(),
                        rebuilt,
                        er.getViableBackends(),
                        er.getExchangeInfo(),
                        erRowType
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
        // Below-Project is passthrough; its output→scan map was captured during analyzeBelow,
        // but we recompute here from p's projects since each project is a RexInputRef.
        int[] origOutToScan = new int[p.getProjects().size()];
        for (int i = 0; i < p.getProjects().size(); i++) {
            origOutToScan[i] = ((RexInputRef) p.getProjects().get(i)).getIndex();
        }

        List<RexNode> newProjects = new ArrayList<>();
        List<String> newNames = new ArrayList<>();
        int[] outputRemap = new int[origOutToScan.length];
        Arrays.fill(outputRemap, -1);
        for (int origOut = 0; origOut < origOutToScan.length; origOut++) {
            int newScanIdx = scanIdxRemap[origOutToScan[origOut]];
            if (newScanIdx < 0) continue; // source dropped (now fetched, not in narrowed Scan)
            RelDataTypeField field = newChild.getRowType().getFieldList().get(newScanIdx);
            outputRemap[origOut] = newProjects.size();
            newProjects.add(new RexInputRef(newScanIdx, field.getType()));
            newNames.add(p.getRowType().getFieldList().get(origOut).getName());
        }
        // Pass through ___row_id (always last in narrowed Scan rowType).
        int rowIdIdx = newChild.getRowType().getFieldCount() - 1;
        RelDataTypeField rowIdField = newChild.getRowType().getFieldList().get(rowIdIdx);
        newProjects.add(new RexInputRef(rowIdIdx, rowIdField.getType()));
        newNames.add(OpenSearchLateMaterialization.ROW_ID_FIELD);

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
     * Walks down the above chain from {@code current}, swapping the anchor's slot with
     * {@code wrapper}. The immediate parent of the anchor gets its RexNodes remapped via
     * {@code anchorToWrapperOut}; ops above the immediate parent reference their child's
     * output rowType (which doesn't shift past the immediate-parent rebuild) and propagate
     * via {@code copy}.
     */
    private static RelNode rebuildAboveChain(
        RelNode current,
        OpenSearchSort origAnchor,
        OpenSearchLateMaterialization wrapper,
        int[] anchorToWrapperOut
    ) {
        if (current == origAnchor) return wrapper;
        if (current.getInputs().size() != 1) {
            throw new IllegalStateException("Multi-input parent in QTF chain: " + current.getClass().getSimpleName());
        }
        RelNode origChild = RelNodeUtils.unwrapHep(current.getInput(0));
        RelNode newChild = rebuildAboveChain(origChild, origAnchor, wrapper, anchorToWrapperOut);
        boolean immediate = origChild == origAnchor;

        switch (current) {
            case OpenSearchProject project -> {
                List<RexNode> newExprs = project.getProjects();
                if (immediate) {
                    IndexRemapShuttle shuttle = new IndexRemapShuttle(anchorToWrapperOut, newChild.getRowType());
                    newExprs = new ArrayList<>(project.getProjects().size());
                    for (RexNode expr : project.getProjects()) {
                        newExprs.add(expr.accept(shuttle));
                    }
                }
                return project.copy(project.getTraitSet(), newChild, newExprs, project.getRowType());
            }
            case OpenSearchFilter filter -> {
                RexNode cond = immediate
                    ? filter.getCondition().accept(new IndexRemapShuttle(anchorToWrapperOut, newChild.getRowType()))
                    : filter.getCondition();
                return filter.copy(filter.getTraitSet(), newChild, cond);
            }
            case OpenSearchSort sort -> {
                RelCollation coll = sort.getCollation();
                if (immediate) {
                    List<RelFieldCollation> remapped = new ArrayList<>(sort.getCollation().getFieldCollations().size());
                    for (RelFieldCollation fc : sort.getCollation().getFieldCollations()) {
                        int newIdx = anchorToWrapperOut[fc.getFieldIndex()];
                        if (newIdx < 0) {
                            throw new IllegalStateException(
                                "Above-anchor Sort references slot " + fc.getFieldIndex() + " not in wrapper output"
                            );
                        }
                        remapped.add(fc.withFieldIndex(newIdx));
                    }
                    coll = RelCollations.of(remapped);
                }
                return sort.copy(sort.getTraitSet(), newChild, coll, sort.offset, sort.fetch);
            }
            default -> throw new IllegalStateException("Unexpected above-anchor operator: " + current.getClass().getSimpleName());
        }
    }

    // ── Records ────────────────────────────────────────────────────────

    /**
     * Output of phase 1. Contains everything phase 2 needs — phase 2 must not re-walk
     * the original plan to recover any of these fields.
     */
    private record Detection(OpenSearchSort anchor, List<RelNode> aboveAnchorOperators, BelowChain belowChain, Set<
        String> belowAnchorPhysicalFields, LinkedHashSet<String> aboveAnchorPhysicalFields, List<String> anchorSlotToPhysicalField) {
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
