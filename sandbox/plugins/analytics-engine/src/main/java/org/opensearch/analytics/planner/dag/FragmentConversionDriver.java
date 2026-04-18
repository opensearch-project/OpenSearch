/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeWriter;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.FragmentConvertor;

import java.util.ArrayList;
import java.util.List;

/**
 * Drives fragment conversion for a {@link StagePlan}. Walks the resolved fragment
 * top-down, strips annotations via {@link OpenSearchRelNode#stripAnnotations},
 * and calls the appropriate {@link FragmentConvertor} method based on the
 * stage's leaf and top nodes.
 *
 * <p>No operator-specific logic — each node strips itself. The driver only
 * determines which convertor method to call (scan vs shuffle-read vs in-memory)
 * and whether to append a shuffle writer.
 *
 * <p>TODO: delegation-aware conversion — when annotations target a different
 * backend, extract expressions, ask delegate backend to convert, pass
 * (delegationId, delegateBytes) to primary backend's convertor.
 *
 * @opensearch.internal
 */
public class FragmentConversionDriver {

    private FragmentConversionDriver() {}

    /**
     * Converts all StagePlan fragments in the DAG to backend-specific bytes.
     * Walks stages bottom-up, converts each plan alternative using the leaf
     * backend's FragmentConvertor.
     *
     * <p>TODO: current approach uses a single backend's convertor per StagePlan
     * (the leaf operator's backend). This won't work for delegation where we need
     * to first convert delegated predicates/expressions via the accepting backend's
     * convertor, then pass those bytes to the primary backend. Next step: walk the
     * resolved fragment's annotations, identify delegated ones (annotation backend
     * != operator backend), convert them via the delegate's convertor, and pass
     * (annotationId, delegateBytes) to the primary backend's convertor.
     */
    public static void convertAll(QueryDAG dag, CapabilityRegistry registry) {
        convertStage(dag.rootStage(), registry);
    }

    private static void convertStage(Stage stage, CapabilityRegistry registry) {
        for (Stage child : stage.getChildStages()) {
            convertStage(child, registry);
        }
        // LOCAL pass-through stages (bare StageInputScan leaf with no compute above,
        // or no children) don't run a backend fragment — the walker short-circuits them.
        // Skipping conversion here matches that contract; otherwise convertByLeafType
        // would try to convert their StageInputScan leaf via convertShuffleReadFragment
        // (which is for shuffle-read sources, not gather sources).
        if (stage.getExecutionType() == StageExecutionType.LOCAL && isPassThrough(stage)) {
            return;
        }
        List<StagePlan> converted = new ArrayList<>();
        for (StagePlan plan : stage.getPlanAlternatives()) {
            AnalyticsSearchBackendPlugin backend = registry.getBackend(plan.backendId());
            FragmentConvertor convertor = backend.getFragmentConvertor();
            byte[] bytes;
            if (stage.getExecutionType() == StageExecutionType.LOCAL && hasComputeAboveStageInputScan(stage)) {
                List<String> childStageInputIds = stage.getChildStages()
                    .stream()
                    .map(child -> "__stage_" + child.getStageId() + "_input__")
                    .toList();
                bytes = convertLocalStage(plan.resolvedFragment(), convertor, childStageInputIds);
            } else {
                bytes = convert(plan.resolvedFragment(), convertor);
            }
            converted.add(plan.withConvertedBytes(bytes));
        }
        stage.setPlanAlternatives(converted);
    }

    /**
     * Strips annotations and converts a local stage fragment via the
     * backend's {@link FragmentConvertor#convertLocalStageFragment}.
     *
     * <p>Before calling the convertor, rewrites each {@link OpenSearchStageInputScan}
     * leaf to a plain {@link TableScan} whose table name is the corresponding
     * stage input ID (e.g., {@code __stage_0_input__}). This decouples the
     * backend convertor from analytics-engine's RelNode types.
     *
     * @param resolvedFragment   the resolved fragment from a StagePlan
     * @param convertor          the primary backend's fragment convertor
     * @param childStageInputIds stage input IDs for each child stage
     * @return backend-specific serialized plan bytes
     */
    public static byte[] convertLocalStage(RelNode resolvedFragment, FragmentConvertor convertor, List<String> childStageInputIds) {
        RelNode stripped = strip(resolvedFragment);
        RelNode rewritten = rewriteStageInputScans(stripped, childStageInputIds);
        return convertor.convertLocalStageFragment(rewritten, childStageInputIds);
    }

    /**
     * Strips annotations from the resolved fragment and converts it via the
     * backend's FragmentConvertor.
     *
     * @param resolvedFragment the resolved fragment from a StagePlan
     * @param convertor        the primary backend's fragment convertor
     * @return backend-specific serialized plan bytes
     */
    // TODO: push conversion dispatch into the nodes themselves (e.g., leaf node calls
    // the right convertor method, top node appends shuffle writer) to avoid instanceof
    // checks here. Similar pattern to getAnnotations()/copyResolved()/stripAnnotations().
    public static byte[] convert(RelNode resolvedFragment, FragmentConvertor convertor) {
        RelNode stripped = strip(resolvedFragment);

        String tableName = findTableName(resolvedFragment);

        // TODO: push shuffle writer detection into OpenSearchExchangeWriter via a
        // node-level method (e.g., wrapConversion(convertor, innerBytes)) instead
        // of instanceof check here.
        if (resolvedFragment instanceof OpenSearchExchangeWriter writer) {
            RelNode strippedBody = strip(writer.getInput());
            byte[] converted = convertByLeafType(strippedBody, tableName, resolvedFragment, convertor);
            return convertor.appendShuffleWriter(converted, writer.getKeys(), 0);
        }

        return convertByLeafType(stripped, tableName, resolvedFragment, convertor);
    }

    // TODO: push leaf-type dispatch into the leaf nodes themselves. Each leaf node
    // should know which FragmentConvertor method to call (e.g., OpenSearchTableScan
    // calls convertScanFragment, OpenSearchStageInputScan calls convertShuffleReadFragment
    // or convertStreamingFragment depending on parent exchange type).
    private static byte[] convertByLeafType(RelNode stripped, String tableName, RelNode original, FragmentConvertor convertor) {
        RelNode leaf = findLeaf(original);
        if (leaf instanceof OpenSearchTableScan) {
            return convertor.convertScanFragment(tableName, stripped);
        }
        if (leaf instanceof OpenSearchStageInputScan) {
            // StageInputScan under ExchangeReducer → streaming source
            // StageInputScan under ShuffleReader → shuffle source
            return convertor.convertShuffleReadFragment(tableName, stripped);
        }
        throw new IllegalStateException("Unknown leaf type: " + leaf.getClass().getSimpleName());
    }

    /** Recursively strips annotations bottom-up. */
    private static RelNode strip(RelNode node) {
        if (node instanceof OpenSearchStageInputScan) {
            return node;
        }

        List<RelNode> strippedChildren = new ArrayList<>();
        for (RelNode input : node.getInputs()) {
            strippedChildren.add(strip(input));
        }

        if (node instanceof OpenSearchRelNode openSearchNode) {
            return openSearchNode.stripAnnotations(strippedChildren);
        }
        return node;
    }

    private static String findTableName(RelNode node) {
        if (node instanceof OpenSearchTableScan scan) {
            return scan.getTable().getQualifiedName().getLast();
        }
        for (RelNode input : node.getInputs()) {
            String name = findTableName(input);
            if (name != null) {
                return name;
            }
        }
        return null;
    }

    private static RelNode findLeaf(RelNode node) {
        if (node.getInputs().isEmpty()) {
            return node;
        }
        return findLeaf(node.getInputs().getFirst());
    }

    /**
     * Returns {@code true} if the stage is a pure pass-through: its fragment
     * is a bare {@link OpenSearchStageInputScan} leaf with no parent operators,
     * or the stage has no children (degenerate LOCAL).
     */
    private static boolean isPassThrough(Stage stage) {
        if (stage.getChildStages().isEmpty()) {
            return true;
        }
        RelNode fragment = stage.getPlanAlternatives().isEmpty()
            ? stage.getFragment()
            : stage.getPlanAlternatives().getFirst().resolvedFragment();
        if (fragment == null) {
            return true;
        }
        return fragment instanceof OpenSearchStageInputScan;
    }

    /**
     * Returns {@code true} if the stage has compute operators above a
     * {@link OpenSearchStageInputScan} — i.e., it's a LOCAL stage with
     * actual work to do (not a pass-through).
     */
    private static boolean hasComputeAboveStageInputScan(Stage stage) {
        RelNode fragment = stage.getPlanAlternatives().isEmpty()
            ? stage.getFragment()
            : stage.getPlanAlternatives().getFirst().resolvedFragment();
        if (fragment == null) {
            return false;
        }
        return hasComputeAbove(fragment, false);
    }

    private static boolean hasComputeAbove(RelNode node, boolean seenCompute) {
        if (node instanceof OpenSearchStageInputScan) {
            return seenCompute;
        }
        boolean isCompute = (node instanceof org.apache.calcite.rel.core.Aggregate)
            || (node instanceof org.apache.calcite.rel.core.Filter)
            || (node instanceof org.apache.calcite.rel.core.Sort)
            || (node instanceof org.apache.calcite.rel.core.Project);
        boolean computeFlag = seenCompute || isCompute;
        for (RelNode input : node.getInputs()) {
            if (hasComputeAbove(input, computeFlag)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Replaces each {@link OpenSearchStageInputScan} in the tree with a plain
     * {@link StageInputTableScan} whose table name is the stage input ID.
     * This allows the backend's Substrait visitor to convert it to a
     * {@code NamedScan} with the correct table reference.
     */
    private static RelNode rewriteStageInputScans(RelNode node, List<String> childStageInputIds) {
        if (node instanceof OpenSearchStageInputScan scan) {
            String inputId = findInputIdForStage(scan.getChildStageId(), childStageInputIds);
            return new StageInputTableScan(scan.getCluster(), scan.getTraitSet(), inputId, scan.getRowType());
        }

        List<RelNode> newInputs = new ArrayList<>();
        boolean changed = false;
        for (RelNode input : node.getInputs()) {
            RelNode rewritten = rewriteStageInputScans(input, childStageInputIds);
            newInputs.add(rewritten);
            if (rewritten != input) {
                changed = true;
            }
        }
        if (changed) {
            return node.copy(node.getTraitSet(), newInputs);
        }
        return node;
    }

    private static String findInputIdForStage(int childStageId, List<String> childStageInputIds) {
        String expected = "__stage_" + childStageId + "_input__";
        for (String inputId : childStageInputIds) {
            if (inputId.equals(expected)) {
                return inputId;
            }
        }
        throw new IllegalStateException("No stage input ID mapping for child stage " + childStageId);
    }

    /**
     * A minimal {@link TableScan} that represents a stage input source.
     * Its {@link RelOptTable} returns the stage input ID as the qualified name,
     * which Substrait visitors convert to a {@code NamedScan}.
     */
    public static class StageInputTableScan extends TableScan {
        public StageInputTableScan(RelOptCluster cluster, RelTraitSet traitSet, String stageInputId, RelDataType rowType) {
            super(cluster, traitSet, List.of(), new StageInputRelOptTable(stageInputId, rowType));
        }
    }

    /**
     * Minimal {@link RelOptTable} implementation that provides a table name and row type
     * for Substrait conversion. Only {@code getQualifiedName()} and {@code getRowType()}
     * are used by the Substrait visitor.
     */
    static class StageInputRelOptTable implements RelOptTable {
        private final List<String> qualifiedName;
        private final RelDataType rowType;

        StageInputRelOptTable(String stageInputId, RelDataType rowType) {
            this.qualifiedName = List.of(stageInputId);
            this.rowType = rowType;
        }

        @Override
        public List<String> getQualifiedName() {
            return qualifiedName;
        }

        @Override
        public RelDataType getRowType() {
            return rowType;
        }

        @Override
        public double getRowCount() {
            return 100;
        }

        @Override
        public RelOptSchema getRelOptSchema() {
            return null;
        }

        @Override
        public RelNode toRel(ToRelContext context) {
            throw new UnsupportedOperationException("StageInputRelOptTable.toRel not supported");
        }

        @Override
        public List<org.apache.calcite.schema.ColumnStrategy> getColumnStrategies() {
            return List.of();
        }

        @Override
        public <C> C unwrap(Class<C> aClass) {
            return null;
        }

        @Override
        public boolean isKey(ImmutableBitSet columns) {
            return false;
        }

        @Override
        public List<ImmutableBitSet> getKeys() {
            return List.of();
        }

        @Override
        public List<RelReferentialConstraint> getReferentialConstraints() {
            return List.of();
        }

        @Override
        public List<RelCollation> getCollationList() {
            return List.of();
        }

        @Override
        public RelDistribution getDistribution() {
            return RelDistributions.ANY;
        }

        @Override
        @SuppressWarnings("rawtypes")
        public Expression getExpression(Class clazz) {
            return null;
        }

        @Override
        public RelOptTable extend(List<RelDataTypeField> extendedFields) {
            return this;
        }
    }
}
