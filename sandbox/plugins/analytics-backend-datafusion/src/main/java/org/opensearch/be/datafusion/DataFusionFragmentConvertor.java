/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.spi.FragmentConvertor;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.AggregateFunctions;
import io.substrait.isthmus.ImmutableFeatureBoard;
import io.substrait.isthmus.SubstraitRelVisitor;
import io.substrait.isthmus.TypeConverter;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.plan.ProtoPlanConverter;
import io.substrait.relation.Aggregate;
import io.substrait.relation.Filter;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.relation.RelCopyOnWriteVisitor;
import io.substrait.relation.Sort;
import io.substrait.util.EmptyVisitationContext;

/**
 * Converts Calcite RelNode fragments to Substrait protobuf bytes
 * for the DataFusion Rust runtime.
 *
 * <p>Dispatch summary:
 * <ul>
 *   <li>{@link #convertShardScanFragment(String, RelNode)} and
 *       {@link #convertFinalAggFragment(RelNode)} — full-fragment conversions via
 *       {@link #convertToSubstrait(RelNode)}.</li>
 *   <li>{@link #attachPartialAggOnTop(RelNode, byte[])} and
 *       {@link #attachFragmentOnTop(RelNode, byte[])} — convert the wrapping
 *       operator standalone, then rewire its input to the decoded inner plan's
 *       root via {@link #rewire(Plan, Rel)}.</li>
 * </ul>
 *
 * @opensearch.internal
 */
public class DataFusionFragmentConvertor implements FragmentConvertor {

    private static final Logger LOGGER = LogManager.getLogger(DataFusionFragmentConvertor.class);

    private final SimpleExtension.ExtensionCollection extensions;

    public DataFusionFragmentConvertor(SimpleExtension.ExtensionCollection extensions) {
        this.extensions = extensions;
    }

    @Override
    public byte[] convertShardScanFragment(String tableName, RelNode fragment) {
        LOGGER.debug("Converting shard scan fragment for table [{}]", tableName);
        return convertToSubstrait(fragment);
    }

    @Override
    public byte[] attachPartialAggOnTop(RelNode partialAggFragment, byte[] innerBytes) {
        LOGGER.debug("Attaching partial aggregate on top of {} inner bytes", innerBytes.length);
        Plan inner = decodePlan(innerBytes);
        Rel wrapper = convertStandalone(partialAggFragment);
        Plan rewired = rewire(inner, withAggregationPhase(wrapper, Expression.AggregationPhase.INITIAL_TO_INTERMEDIATE));
        return serializePlan(rewired);
    }

    @Override
    public byte[] convertFinalAggFragment(RelNode fragment) {
        LOGGER.info("Converting final-aggregate fragment, rowType={}", fragment.getRowType());
        RelNode rewritten = rewriteStageInputScans(fragment);
        byte[] bytes = convertToSubstrait(rewritten);
        // For decomposable functions whose partial state is already intermediate
        // (e.g. approx_count_distinct → HLL sketch), set INTERMEDIATE_TO_RESULT
        // on the corresponding measures so DataFusion merges sketches correctly.
        return withIntermediatePhaseForDecomposedMeasures(bytes);
    }

    @Override
    public byte[] attachFragmentOnTop(RelNode fragment, byte[] innerBytes) {
        LOGGER.info("Attaching [{}] on top, fragment rowType={}, innerBytes={}", fragment.getClass().getSimpleName(), fragment.getRowType(), innerBytes.length);
        Plan inner = decodePlan(innerBytes);
        // Replace the fragment's child with a dummy TableScan so the Substrait visitor
        // doesn't traverse into nodes it can't handle (e.g. StageInputScan from stripped
        // reduce-stage trees). The rewire step replaces this dummy with the actual inner plan.
        RelNode dummyChild = new StageInputTableScan(
            fragment.getCluster(), fragment.getTraitSet(),
            "dummy", fragment.getInputs().getFirst().getRowType()
        );
        RelNode withDummy = fragment.copy(fragment.getTraitSet(), List.of(dummyChild));
        Rel wrapper = convertStandalone(withDummy);
        return serializePlan(rewire(inner, wrapper));
    }

    // ── Core conversion helpers ─────────────────────────────────────────────────

    private byte[] convertToSubstrait(RelNode fragment) {
        // Rewrite AVG aggregate calls to use the isthmus's SubstraitAvgAggFunction
        // so the AggregateFunctionConverter can find the binding.
        fragment = rewriteAvgCalls(fragment);

        RelRoot root = RelRoot.of(fragment, SqlKind.SELECT);
        SubstraitRelVisitor visitor = createVisitor(fragment);
        Rel substraitRel = visitor.apply(root.rel);

        List<String> fieldNames = root.fields.stream().map(field -> field.getValue()).toList();

        Plan.Root substraitRoot = Plan.Root.builder().input(substraitRel).names(fieldNames).build();
        Plan plan = Plan.builder().addRoots(substraitRoot).build();

        plan = new TableNameModifier().modifyTableNames(plan);

        io.substrait.proto.Plan protoPlan = new PlanProtoConverter().toProto(plan);
        byte[] bytes = protoPlan.toByteArray();
        LOGGER.debug("Substrait plan: {} bytes", bytes.length);
        return bytes;
    }

    /**
     * Converts a single operator into a Substrait {@link Rel}. The operator may carry
     * children (e.g. the {@code attachPartialAggOnTop} caller passes a
     * {@code LogicalAggregate} whose input is the already-stripped inner tree); we
     * deliberately discard those children by taking only the outermost rel of the
     * conversion and rewiring its input during {@link #rewire(Plan, Rel)}.
     */
    private Rel convertStandalone(RelNode operator) {
        operator = rewriteAvgCalls(operator);
        SubstraitRelVisitor visitor = createVisitor(operator);
        return visitor.apply(operator);
    }

    /**
     * Rewires the Substrait {@code wrapper} rel to sit above the root relation of
     * {@code inner}. Returns a new {@link Plan} whose single root is
     * {@code wrapper(inner.root)}. Supports the known single-input wrappers emitted
     * by our four SPI methods ({@link Aggregate}, {@link Sort}, {@link Filter},
     * {@link Project}).
     */
    static Plan rewire(Plan inner, Rel wrapper) {
        if (inner.getRoots().isEmpty()) {
            throw new IllegalArgumentException("Inner Substrait plan has no root relation to rewire under wrapper");
        }
        Plan.Root innerRoot = inner.getRoots().get(0);
        Rel innerRel = innerRoot.getInput();
        Rel rewired = replaceInput(wrapper, innerRel);
        // Use the wrapper's output field names when the wrapper changes the schema
        // (e.g. Aggregate produces different fields than its input). Fall back to
        // inner names for schema-preserving wrappers (Sort, Filter).
        List<String> names = deriveNames(rewired, innerRoot.getNames());
        return Plan.builder().addRoots(Plan.Root.builder().input(rewired).names(names).build()).build();
    }

    /**
     * Derives output field names for the rewired plan. For Aggregates, the output
     * schema differs from the input — use the measure names. For other wrappers
     * (Sort, Filter, Project), the inner names are preserved.
     */
    private static List<String> deriveNames(Rel rel, List<String> innerNames) {
        if (rel instanceof Aggregate agg) {
            List<String> names = new ArrayList<>();
            for (io.substrait.expression.Expression expr : agg.getGroupings().stream()
                .flatMap(g -> g.getExpressions().stream()).toList()) {
                names.add("group_" + names.size());
            }
            for (Aggregate.Measure m : agg.getMeasures()) {
                names.add("agg_" + names.size());
            }
            return names;
        }
        if (rel instanceof Project proj) {
            List<String> names = new ArrayList<>();
            for (int i = 0; i < proj.getExpressions().size(); i++) {
                names.add("proj_" + i);
            }
            return names;
        }
        return innerNames;
    }

    private static Rel replaceInput(Rel wrapper, Rel newInput) {
        if (wrapper instanceof Aggregate agg) {
            return Aggregate.builder().from(agg).input(newInput).build();
        }
        if (wrapper instanceof Sort sort) {
            return Sort.builder().from(sort).input(newInput).build();
        }
        if (wrapper instanceof Filter filter) {
            return Filter.builder().from(filter).input(newInput).build();
        }
        if (wrapper instanceof Project project) {
            return Project.builder().from(project).input(newInput).build();
        }
        throw new UnsupportedOperationException(
            "Cannot attach-on-top a Substrait Rel of type " + wrapper.getClass().getSimpleName() + " — no single-input rewire defined"
        );
    }

    /**
     * Overrides the {@link Expression.AggregationPhase} on every {@link Aggregate.Measure}
     * inside an {@link Aggregate} wrapper. No-op for non-aggregate wrappers.
     *
     * <p>Isthmus hardcodes {@code INITIAL_TO_RESULT} on every aggregate-function
     * invocation. For the partial-agg-attach-on-shard path we want
     * {@code INITIAL_TO_INTERMEDIATE}; the final-agg path stays at
     * {@code INITIAL_TO_RESULT} (isthmus's default) which the DataFusion
     * substrait deserialiser treats as the single-stage/final form.
     */
    private static Rel withAggregationPhase(Rel rel, Expression.AggregationPhase phase) {
        if (!(rel instanceof Aggregate agg)) {
            return rel;
        }
        List<Aggregate.Measure> newMeasures = new ArrayList<>(agg.getMeasures().size());
        for (Aggregate.Measure m : agg.getMeasures()) {
            AggregateFunctionInvocation fn = m.getFunction();
            AggregateFunctionInvocation rephased = AggregateFunctionInvocation.builder().from(fn).aggregationPhase(phase).build();
            newMeasures.add(Aggregate.Measure.builder().from(m).function(rephased).build());
        }
        return Aggregate.builder().from(agg).measures(newMeasures).build();
    }

    /**
     * Rewrites every {@link OpenSearchStageInputScan} in the RelNode tree to a plain
     * Calcite {@link TableScan} whose qualified name matches what
     * {@link DatafusionReduceSink#INPUT_ID} registers on the native session.
     * The isthmus visitor then emits a {@link NamedScan} with that name.
     *
     * <p>Single-input simplification: every {@link OpenSearchStageInputScan} maps to
     * the same {@code "input-0"} table id. Multi-input / join shapes will require
     * carrying per-input ids through {@link io.substrait.relation.NamedScan} once
     * implemented.
     */
    private static RelNode rewriteStageInputScans(RelNode node) {
        if (node instanceof OpenSearchStageInputScan scan) {
            return new StageInputTableScan(scan.getCluster(), scan.getTraitSet(), DatafusionReduceSink.INPUT_ID, scan.getRowType());
        }
        List<RelNode> newInputs = new ArrayList<>(node.getInputs().size());
        boolean changed = false;
        for (RelNode input : node.getInputs()) {
            RelNode rewritten = rewriteStageInputScans(input);
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

    // ── Visitor wiring ──────────────────────────────────────────────────────────

    /**
     * Rewrites any {@link Aggregate} in the Substrait plan whose measures use
     * functions that require {@code INTERMEDIATE_TO_RESULT} phase (e.g.
     * {@code approx_count_distinct} when the input is already HLL sketch bytes).
     */
    private byte[] withIntermediatePhaseForDecomposedMeasures(byte[] bytes) {
        Plan plan = decodePlan(bytes);
        Plan rewritten = new IntermediatePhaseRewriter().rewrite(plan);
        return serializePlan(rewritten);
    }

    private static class IntermediatePhaseRewriter extends RelCopyOnWriteVisitor<RuntimeException> {
        Plan rewrite(Plan plan) {
            List<Plan.Root> roots = new ArrayList<>();
            boolean changed = false;
            for (Plan.Root root : plan.getRoots()) {
                Optional<Rel> rel = root.getInput().accept(this, null);
                if (rel.isPresent()) {
                    roots.add(Plan.Root.builder().from(root).input(rel.get()).build());
                    changed = true;
                } else {
                    roots.add(root);
                }
            }
            return changed ? Plan.builder().from(plan).roots(roots).build() : plan;
        }

        @Override
        public Optional<Rel> visit(Aggregate agg, EmptyVisitationContext ctx) {
            // First recurse into children
            Optional<Rel> childResult = super.visit(agg, ctx);
            Aggregate base = (Aggregate) childResult.orElse(agg);

            boolean changed = false;
            List<Aggregate.Measure> newMeasures = new ArrayList<>(base.getMeasures().size());
            for (Aggregate.Measure m : base.getMeasures()) {
                String funcName = m.getFunction().declaration().name();
                if ("approx_count_distinct".equals(funcName)
                    && m.getFunction().aggregationPhase() == Expression.AggregationPhase.INITIAL_TO_RESULT) {
                    AggregateFunctionInvocation fn = AggregateFunctionInvocation.builder()
                        .from(m.getFunction())
                        .aggregationPhase(Expression.AggregationPhase.INTERMEDIATE_TO_RESULT)
                        .build();
                    newMeasures.add(Aggregate.Measure.builder().from(m).function(fn).build());
                    changed = true;
                } else {
                    newMeasures.add(m);
                }
            }
            return changed ? Optional.of(Aggregate.builder().from(base).measures(newMeasures).build()) : childResult;
        }
    }

    /**
     * Rewrites any {@link org.apache.calcite.rel.core.Aggregate} in the tree whose
     * {@link AggregateCall}s use Calcite's standard {@code SqlStdOperatorTable.AVG}
     * to use the isthmus's {@link AggregateFunctions#AVG} instead. The isthmus
     * {@link AggregateFunctionConverter} only recognizes its own AVG variant.
     */
    private static RelNode rewriteAvgCalls(RelNode node) {
        if (node instanceof org.apache.calcite.rel.core.Aggregate agg) {
            boolean changed = false;
            List<AggregateCall> newCalls = new ArrayList<>(agg.getAggCallList().size());
            for (AggregateCall call : agg.getAggCallList()) {
                if (call.getAggregation().getKind() == SqlKind.AVG
                    && call.getAggregation() != AggregateFunctions.AVG) {
                    newCalls.add(
                        AggregateCall.create(
                            AggregateFunctions.AVG,
                            call.isDistinct(),
                            call.isApproximate(),
                            call.ignoreNulls(),
                            call.rexList,
                            call.getArgList(),
                            call.filterArg,
                            call.distinctKeys,
                            call.collation,
                            call.type,
                            call.name
                        )
                    );
                    changed = true;
                } else {
                    newCalls.add(call);
                }
            }
            if (changed) {
                node = agg.copy(agg.getTraitSet(), rewriteAvgCalls(agg.getInput()), agg.getGroupSet(), agg.getGroupSets(), newCalls);
            }
        }
        // Recurse into children
        List<RelNode> newInputs = new ArrayList<>(node.getInputs().size());
        boolean childChanged = false;
        for (RelNode input : node.getInputs()) {
            RelNode rewritten = rewriteAvgCalls(input);
            newInputs.add(rewritten);
            if (rewritten != input) childChanged = true;
        }
        if (childChanged) {
            node = node.copy(node.getTraitSet(), newInputs);
        }
        return node;
    }

    private SubstraitRelVisitor createVisitor(RelNode relNode) {
        RelDataTypeFactory typeFactory = relNode.getCluster().getTypeFactory();
        TypeConverter typeConverter = TypeConverter.DEFAULT;

        AggregateFunctionConverter aggConverter = new AggregateFunctionConverter(extensions.aggregateFunctions(), typeFactory);
        ScalarFunctionConverter scalarConverter = new ScalarFunctionConverter(
            extensions.scalarFunctions(),
            List.of(),
            typeFactory,
            typeConverter
        );
        WindowFunctionConverter windowConverter = new WindowFunctionConverter(extensions.windowFunctions(), typeFactory);

        return new SubstraitRelVisitor(
            typeFactory,
            scalarConverter,
            aggConverter,
            windowConverter,
            typeConverter,
            ImmutableFeatureBoard.builder().build()
        );
    }

    // ── Plan serde helpers ──────────────────────────────────────────────────────

    /** Decodes serialized Substrait bytes into a model-level {@link Plan}. */
    private Plan decodePlan(byte[] bytes) {
        try {
            io.substrait.proto.Plan proto = io.substrait.proto.Plan.parseFrom(bytes);
            return new ProtoPlanConverter(extensions).from(proto);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Failed to decode Substrait plan bytes", e);
        }
    }

    /** Serializes a model-level {@link Plan} to proto bytes. */
    private static byte[] serializePlan(Plan plan) {
        return new PlanProtoConverter().toProto(plan).toByteArray();
    }

    // ── NamedScan prefix stripper ───────────────────────────────────────────────

    /**
     * Strips Calcite catalog prefixes (e.g. "opensearch") from {@link NamedScan} table
     * names — DataFusion expects the bare index / stage-input id.
     */
    private static class TableNameModifier {
        Plan modifyTableNames(Plan plan) {
            TableNameVisitor visitor = new TableNameVisitor();
            List<Plan.Root> modifiedRoots = new ArrayList<>();
            for (Plan.Root root : plan.getRoots()) {
                Optional<Rel> modifiedRel = root.getInput().accept(visitor, null);
                if (modifiedRel.isPresent()) {
                    modifiedRoots.add(Plan.Root.builder().from(root).input(modifiedRel.get()).build());
                } else {
                    modifiedRoots.add(root);
                }
            }
            return Plan.builder().from(plan).roots(modifiedRoots).build();
        }

        private static class TableNameVisitor extends RelCopyOnWriteVisitor<RuntimeException> {
            @Override
            public Optional<Rel> visit(NamedScan namedScan, EmptyVisitationContext context) {
                List<String> names = namedScan.getNames();
                if (names.size() > 1) {
                    return Optional.of(NamedScan.builder().from(namedScan).names(List.of(names.get(names.size() - 1))).build());
                }
                return super.visit(namedScan, context);
            }
        }
    }

    // ── Calcite TableScan wrappers for OpenSearchStageInputScan rewrite ─────────

    /**
     * Minimal {@link TableScan} representing a stage-input source. The backing
     * {@link StageInputRelOptTable} reports the stage-input id as its single qualified
     * name; isthmus converts this to a {@link NamedScan} with that one-element name.
     */
    static final class StageInputTableScan extends TableScan {
        StageInputTableScan(RelOptCluster cluster, RelTraitSet traitSet, String stageInputId, RelDataType rowType) {
            super(cluster, traitSet, List.of(), new StageInputRelOptTable(stageInputId, rowType));
        }
    }

    /**
     * Minimal {@link RelOptTable} implementation — only {@code getQualifiedName()} and
     * {@code getRowType()} are consulted by the isthmus visitor.
     */
    static final class StageInputRelOptTable implements RelOptTable {
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
        public List<ColumnStrategy> getColumnStrategies() {
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
        public org.apache.calcite.linq4j.tree.Expression getExpression(Class clazz) {
            return null;
        }

        @Override
        public RelOptTable extend(List<RelDataTypeField> extendedFields) {
            return this;
        }
    }
}
