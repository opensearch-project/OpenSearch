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
import org.opensearch.analytics.planner.rel.OpenSearchSort;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * QTF (Query-Then-Fetch / late materialization) post-CBO rewrite. Single entry point
 * {@link #rewrite(RelNode)} — returns the rewritten root if the QTF pattern matches and
 * fires, the original root otherwise.
 *
 * <p>Detect:
 * <ul>
 *   <li>Anchor = lowest {@link OpenSearchSort} on the root-to-leaf chain with non-empty
 *       collation and non-null fetch. Pure-LIMIT (no collation) excluded.</li>
 *   <li>Below-anchor allow-list: Scan / Filter / ER / Project (passthrough only — every
 *       project must be a {@link RexInputRef}; expression projects bail with TODO).</li>
 *   <li>Above-anchor allow-list: Filter / Project (no RexOver) / Aggregate / Sort.</li>
 *   <li>Fetched-column set (= pure-project candidates − keepBelow) must be non-empty.</li>
 * </ul>
 *
 * <p>Modify (when detect passes):
 * <ul>
 *   <li><b>M1</b> — append {@code ___row_id} (BIGINT) to Scan rowType.</li>
 *   <li><b>M2</b> — drop fetched cols from Scan rowType.</li>
 *   <li><b>M3</b> — bottom-up rebuild of Filter / below-Project / ER / Sort. Index spaces
 *       are tracked separately for scan-space (Scan/Filter) and below-Project space
 *       (anything above a passthrough below-Project). ER additionally declares
 *       {@code ___ugsi}.</li>
 *   <li><b>M4</b> — wrap above the anchor Sort with {@link OpenSearchLateMaterialization}.</li>
 *   <li><b>M5</b> — remap parent-chain RexNodes from anchor.rowType indices to wrapper
 *       output indices.</li>
 * </ul>
 *
 * <p>UT cases to cover (T4 — {@code SqlPlannerTestFixture} driven):
 * <ul>
 *   <li>Simple QTF: {@code SELECT URL, EventDate FROM hits ORDER BY EventDate LIMIT 10}.</li>
 *   <li>QTF with WHERE: {@code SELECT URL, EventDate FROM hits WHERE CounterID=5 ORDER BY EventDate LIMIT 10}.</li>
 *   <li>QTF with sort col also projected: {@code SELECT EventDate, URL FROM hits ORDER BY EventDate LIMIT 10}
 *       — EventDate stays in keepBelow + surfaces via wrapper passthrough.</li>
 *   <li>Passthrough below-Project (aliasing): {@code SELECT URL AS u, EventDate AS d FROM hits ORDER BY d LIMIT 10}.</li>
 *   <li>QTF declined — pure-LIMIT: {@code SELECT URL FROM hits LIMIT 10}.</li>
 *   <li>QTF declined — empty fetched set: {@code SELECT EventDate FROM hits ORDER BY EventDate LIMIT 10}.</li>
 *   <li>QTF declined — Aggregate below Sort: {@code SELECT CounterID, COUNT(*) FROM hits GROUP BY CounterID ORDER BY CounterID LIMIT 10}.</li>
 *   <li>QTF declined — Window in outer Project: {@code SELECT URL, ROW_NUMBER() OVER () FROM hits ORDER BY EventDate LIMIT 10}.</li>
 *   <li>QTF declined — expression Project below the anchor: {@code SELECT URL FROM hits ORDER BY UPPER(URL) LIMIT 10}.
 *       TODO: handle in a follow-up.</li>
 * </ul>
 *
 * @opensearch.internal
 */
public final class OpenSearchLateMaterializationRewriter {

    private static final Logger LOGGER = LogManager.getLogger(OpenSearchLateMaterializationRewriter.class);

    private static final Set<Class<? extends RelNode>> ABOVE_ALLOWED = Set.of(
        OpenSearchFilter.class,
        OpenSearchProject.class,
        OpenSearchSort.class,
        OpenSearchAggregate.class
    );

    /** Below-anchor intermediate ops; below-Project is allowed only with passthrough projects (checked in {@link #analyzeBelow}). */
    private static final Set<Class<? extends RelNode>> BELOW_INTERMEDIATE_ALLOWED = Set.of(
        OpenSearchFilter.class,
        OpenSearchExchangeReducer.class,
        OpenSearchProject.class
    );

    private OpenSearchLateMaterializationRewriter() {}

    /** Returns the rewritten root iff QTF matched and fired; {@link Optional#empty()} otherwise. */
    public static Optional<RelNode> rewrite(RelNode root) {
        AnchorContext ctx = findAnchor(root);
        if (ctx == null) return Optional.empty();
        if (!isChainAllowed(ctx.parentChain)) {
            LOGGER.debug("[QTF] above-allow-list rejected; skipping rewrite");
            return Optional.empty();
        }
        OpenSearchSort anchor = ctx.anchor;
        BelowSubtree below = analyzeBelow(anchor.getInput());
        if (below == null) {
            LOGGER.debug("[QTF] below-allow-list rejected; skipping rewrite");
            return Optional.empty();
        }

        FetchPlan fetch = planFetch(anchor, ctx.parentChain, below);
        if (fetch.fetchedScanIndices.isEmpty()) {
            LOGGER.debug("[QTF] no pure-project columns to fetch; skipping rewrite");
            return Optional.empty();
        }

        SubtreeRebuild rebuild = rebuildBelowAnchor(anchor, below, fetch);
        OpenSearchSort newAnchor = rebuildAnchorSort(anchor, rebuild);
        OpenSearchLateMaterialization wrapper = new OpenSearchLateMaterialization(
            newAnchor.getCluster(),
            newAnchor.getTraitSet(),
            newAnchor,
            fetch.fetchFields,
            fetch.fetchStorage,
            anchor.getViableBackends()
        );
        return Optional.of(wrapWithLateMatRel(root, anchor, wrapper, fetch, below));
    }

    // ── Detect ─────────────────────────────────────────────────────────

    /**
     * Walks {@code root} downward to a leaf, then finds the lowest {@link OpenSearchSort}
     * on the chain with non-empty collation and non-null fetch. For two-Sort shapes
     * ({@code Sort(fetch) ← Sort(collation+fetch) ← ...}) the lower (inner) Sort becomes
     * the anchor; the upper Sort lands in the parent chain and is allowed there.
     */
    private static AnchorContext findAnchor(RelNode root) {
        List<RelNode> chainTopDown = new ArrayList<>();
        RelNode cur = RelNodeUtils.unwrapHep(root);
        while (cur != null && cur.getInputs().size() == 1) {
            chainTopDown.add(cur);
            cur = RelNodeUtils.unwrapHep(cur.getInput(0));
        }
        for (int i = chainTopDown.size() - 1; i >= 0; i--) {
            RelNode n = chainTopDown.get(i);
            if (n instanceof OpenSearchSort sort
                && !sort.getCollation().getFieldCollations().isEmpty()
                && sort.fetch != null) {
                return new AnchorContext(sort, chainTopDown.subList(0, i));
            }
        }
        return null;
    }

    /** Above-anchor allow-list check; rejects {@link OpenSearchProject} that carries {@code RexOver}. */
    private static boolean isChainAllowed(List<RelNode> chain) {
        for (RelNode n : chain) {
            if (!ABOVE_ALLOWED.contains(n.getClass())) return false;
            if (n instanceof OpenSearchProject project && project.containsOver()) return false;
        }
        return true;
    }

    /**
     * Analyzes the below-anchor subtree. Returns null if not allow-listed, or if the
     * below-Project carries a non-passthrough expression (deferred — TODO follow-up).
     */
    private static BelowSubtree analyzeBelow(RelNode subtree) {
        List<RelNode> chain = new ArrayList<>();
        int[] belowProjOutToScan = null;
        OpenSearchTableScan scan = null;
        RelNode n = RelNodeUtils.unwrapHep(subtree);
        while (n != null) {
            if (n instanceof OpenSearchTableScan s) {
                scan = s;
                break;
            }
            if (!BELOW_INTERMEDIATE_ALLOWED.contains(n.getClass())) return null;
            if (n instanceof OpenSearchProject p) {
                if (belowProjOutToScan != null) {
                    LOGGER.debug("[QTF] multiple Projects below anchor (rare ER-between-Projects shape) — skipping");
                    return null;
                }
                int[] outToScan = passthroughMap(p);
                if (outToScan == null) {
                    LOGGER.debug("[QTF] expression below-Project — skipping (TODO follow-up)");
                    return null;
                }
                belowProjOutToScan = outToScan;
            }
            chain.add(n);
            if (n.getInputs().isEmpty()) return null;
            n = RelNodeUtils.unwrapHep(n.getInput(0));
        }
        if (scan == null) return null;
        return new BelowSubtree(chain, belowProjOutToScan, scan);
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

    // ── Plan fetch ─────────────────────────────────────────────────────

    /**
     * Computes scan-space indices that stay below (sortCols ∪ filterCols) vs go through
     * the fetch leg (above-refs ∩ ¬keepBelow). Empty fetch set ⇒ caller skips QTF.
     */
    private static FetchPlan planFetch(OpenSearchSort anchor, List<RelNode> parentChain, BelowSubtree below) {
        Set<Integer> sortColsAnchor = new HashSet<>();
        for (RelFieldCollation fc : anchor.getCollation().getFieldCollations()) sortColsAnchor.add(fc.getFieldIndex());
        Set<Integer> sortColsScan = toScanSpace(sortColsAnchor, below.belowProjOutToScan);

        Set<Integer> filterColsScan = new HashSet<>();
        for (RelNode n : below.chain) {
            if (n instanceof OpenSearchFilter f) filterColsScan.addAll(RelNodeUtils.collectInputRefs(f.getCondition()));
        }

        Set<Integer> aboveRefsAnchor = collectAboveRefs(parentChain);
        Set<Integer> aboveRefsScan = toScanSpace(aboveRefsAnchor, below.belowProjOutToScan);

        Set<Integer> keepBelowScan = new HashSet<>(sortColsScan);
        keepBelowScan.addAll(filterColsScan);

        Set<Integer> fetchedScan = new HashSet<>();
        for (int idx : aboveRefsScan) {
            if (!keepBelowScan.contains(idx)) fetchedScan.add(idx);
        }

        OpenSearchTableScan scan = below.scan;
        List<RelDataTypeField> origFields = scan.getRowType().getFieldList();
        List<FieldStorageInfo> origStorage = scan.getOutputFieldStorage();

        List<Integer> fetchedOrdered = new ArrayList<>(fetchedScan);
        fetchedOrdered.sort(Integer::compareTo);
        List<RelDataTypeField> fetchFields = new ArrayList<>(fetchedOrdered.size());
        List<FieldStorageInfo> fetchStorage = new ArrayList<>(fetchedOrdered.size());
        for (int idx : fetchedOrdered) {
            fetchFields.add(origFields.get(idx));
            fetchStorage.add(origStorage.get(idx));
        }

        List<Integer> keepBelowOrdered = new ArrayList<>(keepBelowScan);
        keepBelowOrdered.sort(Integer::compareTo);
        return new FetchPlan(keepBelowOrdered, fetchedOrdered, fetchFields, fetchStorage);
    }

    /** Translates anchor-space indices to scan space via the below-Project map (identity when no below-Project). */
    private static Set<Integer> toScanSpace(Set<Integer> anchorIdxs, int[] belowProjOutToScan) {
        if (belowProjOutToScan == null) return anchorIdxs;
        Set<Integer> out = new HashSet<>(anchorIdxs.size());
        for (int idx : anchorIdxs) out.add(belowProjOutToScan[idx]);
        return out;
    }

    /**
     * Collects RexInputRef indices from the immediate parent's RexNodes (in anchor.rowType
     * space). Other parents reference each other's outputs, not anchor's directly — those
     * are handled by wrapWithLateMatRel during M5.
     */
    private static Set<Integer> collectAboveRefs(List<RelNode> parentChain) {
        Set<Integer> out = new HashSet<>();
        if (parentChain.isEmpty()) return out;
        RelNode immediate = parentChain.get(parentChain.size() - 1);
        if (immediate instanceof OpenSearchProject p) {
            for (RexNode expr : p.getProjects()) out.addAll(RelNodeUtils.collectInputRefs(expr));
        } else if (immediate instanceof OpenSearchFilter f) {
            out.addAll(RelNodeUtils.collectInputRefs(f.getCondition()));
        }
        return out;
    }

    // ── M1 + M2 + M3 (subtree rebuild) ─────────────────────────────────

    /**
     * Rebuilds the subtree below the anchor with narrowed Scan + remapped operators.
     * Returns the rebuilt subtree + {@code remapAnchor} (origAnchorIdx → rebuiltInput idx).
     */
    private static SubtreeRebuild rebuildBelowAnchor(OpenSearchSort anchor, BelowSubtree below, FetchPlan fetch) {
        OpenSearchTableScan origScan = below.scan;
        RelDataTypeFactory typeFactory = origScan.getCluster().getTypeFactory();
        List<RelDataTypeField> origScanFields = origScan.getRowType().getFieldList();

        // Narrowed Scan rowType + storage (M1+M2): keepBelow cols + ___row_id.
        RelDataTypeFactory.Builder scanBuilder = typeFactory.builder();
        List<FieldStorageInfo> origStorage = origScan.getOutputFieldStorage();
        List<FieldStorageInfo> narrowedStorage = new ArrayList<>(fetch.keepBelowScanIndices.size() + 1);
        for (int idx : fetch.keepBelowScanIndices) {
            scanBuilder.add(origScanFields.get(idx).getName(), origScanFields.get(idx).getType());
            narrowedStorage.add(origStorage.get(idx));
        }
        scanBuilder.add(OpenSearchLateMaterialization.ROW_ID_FIELD, typeFactory.createSqlType(SqlTypeName.BIGINT));
        narrowedStorage.add(FieldStorageInfo.derivedColumn(OpenSearchLateMaterialization.ROW_ID_FIELD, SqlTypeName.BIGINT));

        OpenSearchTableScan newScan = new OpenSearchTableScan(
            origScan.getCluster(),
            origScan.getTraitSet(),
            origScan.getTable(),
            origScan.getViableBackends(),
            narrowedStorage,
            scanBuilder.build()
        );

        int[] remapScan = new int[origScanFields.size()];
        Arrays.fill(remapScan, -1);
        for (int newIdx = 0; newIdx < fetch.keepBelowScanIndices.size(); newIdx++) {
            remapScan[fetch.keepBelowScanIndices.get(newIdx)] = newIdx;
        }

        RelNode rebuilt = newScan;
        int[] remapAnchor = remapScan;
        for (int i = below.chain.size() - 1; i >= 0; i--) {
            RelNode orig = below.chain.get(i);
            if (orig instanceof OpenSearchFilter f) {
                RexNode remapped = f.getCondition().accept(new IndexRemapShuttle(remapScan, rebuilt.getRowType()));
                rebuilt = new OpenSearchFilter(f.getCluster(), f.getTraitSet(), rebuilt, remapped, f.getViableBackends());
            } else if (orig instanceof OpenSearchProject p) {
                List<RexNode> newProjects = new ArrayList<>();
                List<String> newNames = new ArrayList<>();
                int[] outputRemap = new int[below.belowProjOutToScan.length];
                Arrays.fill(outputRemap, -1);
                for (int origOut = 0; origOut < below.belowProjOutToScan.length; origOut++) {
                    int newScanIdx = remapScan[below.belowProjOutToScan[origOut]];
                    if (newScanIdx < 0) continue; // source dropped (now fetched)
                    RelDataTypeField field = rebuilt.getRowType().getFieldList().get(newScanIdx);
                    outputRemap[origOut] = newProjects.size();
                    newProjects.add(new RexInputRef(newScanIdx, field.getType()));
                    newNames.add(p.getRowType().getFieldList().get(origOut).getName());
                }
                int rowIdIdx = rebuilt.getRowType().getFieldCount() - 1;
                RelDataTypeField rowIdField = rebuilt.getRowType().getFieldList().get(rowIdIdx);
                newProjects.add(new RexInputRef(rowIdIdx, rowIdField.getType()));
                newNames.add(OpenSearchLateMaterialization.ROW_ID_FIELD);
                RelDataTypeFactory.Builder pb = typeFactory.builder();
                for (int j = 0; j < newProjects.size(); j++) pb.add(newNames.get(j), newProjects.get(j).getType());
                rebuilt = new OpenSearchProject(p.getCluster(), p.getTraitSet(), rebuilt, newProjects, pb.build(), p.getViableBackends());
                remapAnchor = outputRemap;
            } else if (orig instanceof OpenSearchExchangeReducer er) {
                // UGSI on the wire is an INTEGER ordinal — coord maintains the ordinal →
                // (shardOrd, indexUUID, nodeId) mapping side-table for the Scatter-Gather stage.
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
            } else {
                throw new IllegalStateException("Unexpected below-anchor operator: " + orig.getClass().getSimpleName());
            }
        }
        return new SubtreeRebuild(rebuilt, remapAnchor);
    }

    private static OpenSearchSort rebuildAnchorSort(OpenSearchSort anchor, SubtreeRebuild rebuild) {
        List<RelFieldCollation> newCollations = new ArrayList<>(anchor.getCollation().getFieldCollations().size());
        for (RelFieldCollation fc : anchor.getCollation().getFieldCollations()) {
            int newIdx = rebuild.remapAnchor[fc.getFieldIndex()];
            if (newIdx < 0) {
                throw new IllegalStateException("Sort collation references dropped column at original anchor idx " + fc.getFieldIndex());
            }
            newCollations.add(fc.withFieldIndex(newIdx));
        }
        return new OpenSearchSort(
            anchor.getCluster(),
            anchor.getTraitSet(),
            rebuild.rebuiltSubtree,
            RelCollations.of(newCollations),
            anchor.offset,
            anchor.fetch,
            anchor.getViableBackends()
        );
    }

    /**
     * Rebuilds the parent chain bottom-up, swapping the anchor's slot for the wrapper.
     * Only the immediate parent's RexNodes reference anchor.rowType directly — those get
     * remapped via {@code anchorToWrapperOut}. Ops above the immediate parent reference
     * each other's outputs and propagate via {@code copy}.
     *
     * <p>Wrapper output layout: {@code [keepBelow cols (in original scan order), then
     * fetched cols]}. Helper cols ({@code ___row_id}, {@code ___ugsi}) are stripped.
     */
    private static RelNode wrapWithLateMatRel(
        RelNode root,
        OpenSearchSort origAnchor,
        OpenSearchLateMaterialization wrapper,
        FetchPlan fetch,
        BelowSubtree below
    ) {
        int origAnchorFieldCount = origAnchor.getRowType().getFieldCount();
        int[] anchorToWrapperOut = new int[origAnchorFieldCount];
        Arrays.fill(anchorToWrapperOut, -1);
        for (int aIdx = 0; aIdx < origAnchorFieldCount; aIdx++) {
            int scanIdx = below.belowProjOutToScan == null ? aIdx : below.belowProjOutToScan[aIdx];
            int kb = fetch.keepBelowScanIndices.indexOf(scanIdx);
            if (kb >= 0) {
                anchorToWrapperOut[aIdx] = kb;
                continue;
            }
            int fIdx = fetch.fetchedScanIndices.indexOf(scanIdx);
            if (fIdx >= 0) anchorToWrapperOut[aIdx] = fetch.keepBelowScanIndices.size() + fIdx;
            // else: anchor-only col (e.g. sort key with no above ref) — left -1.
        }
        return rebuildParents(RelNodeUtils.unwrapHep(root), origAnchor, wrapper, anchorToWrapperOut);
    }

    private static RelNode rebuildParents(RelNode current, OpenSearchSort origAnchor, OpenSearchLateMaterialization wrapper, int[] remap) {
        if (current == origAnchor) return wrapper;
        if (current.getInputs().size() != 1) {
            throw new IllegalStateException("Multi-input parent in QTF chain: " + current.getClass().getSimpleName());
        }
        RelNode origChild = RelNodeUtils.unwrapHep(current.getInput(0));
        RelNode newChild = rebuildParents(origChild, origAnchor, wrapper, remap);
        boolean immediate = origChild == origAnchor;

        if (current instanceof OpenSearchProject project) {
            List<RexNode> newExprs = project.getProjects();
            if (immediate) {
                IndexRemapShuttle shuttle = new IndexRemapShuttle(remap, newChild.getRowType());
                newExprs = new ArrayList<>(project.getProjects().size());
                for (RexNode expr : project.getProjects()) newExprs.add(expr.accept(shuttle));
            }
            return project.copy(project.getTraitSet(), newChild, newExprs, project.getRowType());
        }
        if (current instanceof OpenSearchFilter filter) {
            RexNode cond = immediate ? filter.getCondition().accept(new IndexRemapShuttle(remap, newChild.getRowType())) : filter.getCondition();
            return filter.copy(filter.getTraitSet(), newChild, cond);
        }
        if (current instanceof OpenSearchSort sort) {
            RelCollation coll = sort.getCollation();
            if (immediate) {
                List<RelFieldCollation> remapped = new ArrayList<>(sort.getCollation().getFieldCollations().size());
                for (RelFieldCollation fc : sort.getCollation().getFieldCollations()) {
                    int newIdx = remap[fc.getFieldIndex()];
                    if (newIdx < 0) {
                        throw new IllegalStateException("Above-anchor Sort references dropped col at idx " + fc.getFieldIndex());
                    }
                    remapped.add(fc.withFieldIndex(newIdx));
                }
                coll = RelCollations.of(remapped);
            }
            return sort.copy(sort.getTraitSet(), newChild, coll, sort.offset, sort.fetch);
        }
        if (current instanceof OpenSearchAggregate agg) {
            // Aggregate above-anchor — first cut: copy with new input. GroupSet/aggCalls remap is a TODO.
            return agg.copy(agg.getTraitSet(), List.of(newChild));
        }
        throw new IllegalStateException("Unexpected above-anchor operator: " + current.getClass().getSimpleName());
    }

    // ── Records ────────────────────────────────────────────────────────

    private record AnchorContext(OpenSearchSort anchor, List<RelNode> parentChain) {}

    private record BelowSubtree(List<RelNode> chain, int[] belowProjOutToScan, OpenSearchTableScan scan) {}

    private record SubtreeRebuild(RelNode rebuiltSubtree, int[] remapAnchor) {}

    private record FetchPlan(
        List<Integer> keepBelowScanIndices,
        List<Integer> fetchedScanIndices,
        List<RelDataTypeField> fetchFields,
        List<FieldStorageInfo> fetchStorage
    ) {}
}
