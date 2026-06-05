/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.AnnotatedPredicate;
import org.opensearch.analytics.planner.rel.AnnotationResolver;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchLateMaterialization;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.planner.rel.OperatorAnnotation;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.DelegatedExpression;
import org.opensearch.analytics.spi.DelegatedPredicateSerializer;
import org.opensearch.analytics.spi.DelegationPossibleFunction;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.FilterTreeShape;
import org.opensearch.analytics.spi.FragmentConvertor;
import org.opensearch.analytics.spi.FragmentInstructionHandlerFactory;
import org.opensearch.analytics.spi.InstructionNode;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.analytics.spi.WireFormat;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

/**
 * Drives fragment conversion for all {@link StagePlan} alternatives in a {@link QueryDAG}.
 * Strips annotations from each resolved fragment and dispatches to the backend's
 * {@link FragmentConvertor} using composable calls — backends never traverse the plan.
 *
 * <p>Dispatch logic for PR2 (pure shard-scan path):
 * <ul>
 *   <li>Leaf = {@link OpenSearchTableScan}, top = {@link OpenSearchAggregate}(PARTIAL):
 *       {@code convertFragment} on everything below partial agg,
 *       then {@code attachPartialAggOnTop}</li>
 *   <li>Leaf = {@link OpenSearchTableScan}, top = anything else:
 *       {@code convertFragment} on the full fragment</li>
 *   <li>Leaf = {@link OpenSearchStageInputScan} (reduce stage):
 *       {@code convertFragment} on the final agg (ExchangeReducer stripped),
 *       then {@code attachFragmentOnTop} for any operators above it</li>
 * </ul>
 *
 * <p>TODO: as shuffle joins/aggregates and delegation are added, introduce a
 * {@code FragmentConversionStrategy} abstraction so each shape encapsulates its own
 * dispatch logic rather than growing this class with more {@code instanceof} checks.
 *
 * @opensearch.internal
 */
public class FragmentConversionDriver {

    private static final Logger LOGGER = LogManager.getLogger(FragmentConversionDriver.class);

    private FragmentConversionDriver() {}

    /**
     * Converts all {@link StagePlan} alternatives in the DAG, populating
     * {@link StagePlan#convertedBytes()} on each plan.
     *
     * @param fuseDualViable when {@code true}, performance-delegated leaves fuse with
     *     correctness-delegated siblings even under OR/NOT. Sourced from the cluster setting
     *     {@code analytics.delegation.fuse_dual_viable}.
     */
    public static void convertAll(QueryDAG dag, CapabilityRegistry registry, boolean fuseDualViable) {
        convertStage(dag.rootStage(), registry, fuseDualViable);
        // Root stage executes locally at coordinator — store factory for instruction dispatch.
        Stage root = dag.rootStage();
        if (root.getExchangeSinkProvider() != null && !root.getPlanAlternatives().isEmpty()) {
            AnalyticsSearchBackendPlugin backend = registry.getBackend(root.getPlanAlternatives().getFirst().backendId());
            root.setInstructionHandlerFactory(backend.getInstructionHandlerFactory());
        }
    }

    private static void convertStage(Stage stage, CapabilityRegistry registry, boolean fuseDualViable) {
        for (Stage child : stage.getChildStages()) {
            convertStage(child, registry, fuseDualViable);
        }
        // After children are converted, surface any decorator-induced schema delta as
        // postDecorationSchemaBytes on the child plans. The reduce sink consults this when
        // registering the partition so the catalog binding matches what the decorator delivers.
        populatePostDecorationSchemas(stage, registry);
        // LM stage runs Java-only scatter/gather/stitch — no Substrait compute. Emit a
        // stub Read carrying the wrapper's output schema so Stage 3's parent reduce sink
        // can derive the partition schema via the standard producerPlanBytes path.
        if (stage.getExecutionType() == StageExecutionType.LATE_MATERIALIZATION) {
            convertLateMaterializationStage(stage, registry);
            return;
        }
        List<StagePlan> converted = new ArrayList<>(stage.getPlanAlternatives().size());
        for (StagePlan plan : stage.getPlanAlternatives()) {
            AnalyticsSearchBackendPlugin backend = registry.getBackend(plan.backendId());
            FragmentConvertor convertor = backend.getFragmentConvertor();

            // Derive filter tree shape BEFORE stripping (annotations must be intact). Mirrors
            // fuseDualViable so the deriver's classification matches the post-combiner tree
            // the data node actually sees.
            OpenSearchFilter filter = RelNodeUtils.findNode(plan.resolvedFragment(), OpenSearchFilter.class);
            FilterTreeShape treeShape = filter != null
                ? FilterTreeShapeDeriver.derive(filter, plan.backendId(), fuseDualViable)
                : FilterTreeShape.NO_DELEGATION;

            IntraOperatorDelegationBytes delegationBytes = new IntraOperatorDelegationBytes(registry, fuseDualViable);
            byte[] bytes = convert(plan.resolvedFragment(), convertor, delegationBytes);

            // Assemble instruction list
            List<DelegatedExpression> delegated = delegationBytes.getResult();
            List<InstructionNode> instructions = assembleInstructions(backend, plan, treeShape, delegationBytes);

            converted.add(plan.withConvertedBytes(bytes, delegated).withInstructions(instructions));
            LOGGER.debug(
                "Stage [{}] converted: treeShape={}, delegatedExpressions={}{}",
                plan.backendId(),
                treeShape,
                delegated.size(),
                delegated.isEmpty() ? "" : " [ids=" + delegated.stream().map(d -> String.valueOf(d.getAnnotationId())).toList() + "]"
            );
        }
        stage.setPlanAlternatives(converted);
        // Store factory on coordinator-reduce stages (local execution, no serialization needed).
        // Shard stages get the factory from the local backend plugin at the data node.
        if (stage.getExchangeSinkProvider() != null && !converted.isEmpty()) {
            AnalyticsSearchBackendPlugin backend = registry.getBackend(converted.getFirst().backendId());
            stage.setInstructionHandlerFactory(backend.getInstructionHandlerFactory());
        }
    }

    /**
     * Emits a schema-only Read stub onto each child plan's {@code postDecorationSchemaBytes}
     * whenever the parent reduce sink can't safely decode the child's {@code convertedBytes} as
     * the wire format it expects. Two distinct triggers, both resolved by the CHILD's convertor
     * (the producer describes its own output schema):
     *
     * <ul>
     *   <li><b>Schema decoration</b> — a partition decorator (e.g. {@code OrdinalAppendingSink})
     *       widens the child's produced rowType before it reaches the partition boundary. The
     *       parent declares the wider {@code expected} rowType on its
     *       {@code OpenSearchStageInputScan} placeholder; the reducer needs that, not the
     *       producer's narrower natural schema.</li>
     *   <li><b>Opaque-wire-format producer</b> — the child plan's backend (Lucene today) emits
     *       a custom wire format from {@code convertFragment} the reducer can't decode generically.
     *       The child's {@code convertSchemaOnlyRead} returns a self-describing schema stub so the
     *       reducer can register the partition. Without this, the reducer runs decode over the
     *       opaque bytes and fails.</li>
     * </ul>
     *
     * <p>Producer wire-format is asked of the child's convertor via
     * {@link FragmentConvertor#wireFormat()}. The schema-only Read uses the {@code expected}
     * rowType (the post-decoration schema crossing the partition boundary).
     *
     * <p>TODO: Uses {@link RelNodeUtils#findNode} which only walks the first-input chain. Fine for
     * QTF today (linear fragments). When QTF extends to Joins/Unions, multi-input fragments will
     * have multiple {@code StageInputScan} leaves and this needs a multi-leaf walker.
     */
    private static void populatePostDecorationSchemas(Stage stage, CapabilityRegistry registry) {
        for (Stage child : stage.getChildStages()) {
            OpenSearchStageInputScan inputScan = RelNodeUtils.findNode(stage.getFragment(), OpenSearchStageInputScan.class);
            if (inputScan == null || inputScan.getChildStageId() != child.getStageId()) continue;
            RelDataType produced = child.getFragment().getRowType();
            RelDataType expected = inputScan.getRowType();
            boolean schemaMismatch = produced.getFieldCount() != expected.getFieldCount() || produced.equals(expected) == false;

            List<StagePlan> updated = new ArrayList<>(child.getPlanAlternatives().size());
            boolean changed = false;
            for (StagePlan plan : child.getPlanAlternatives()) {
                FragmentConvertor convertor = registry.getBackend(plan.backendId()).getFragmentConvertor();
                boolean selfDescribing = convertor.wireFormat() == WireFormat.SELF_DESCRIBING;
                // Stub needed when (a) the schema decorator widened the partition rowType, or
                // (b) the producer's wire format is opaque — the reducer can't decode its
                // convertedBytes for partition-schema derivation.
                if (schemaMismatch == false && selfDescribing) {
                    updated.add(plan);
                    continue;
                }
                byte[] postDecorationBytes = convertor.convertSchemaOnlyRead(child.getStageId(), expected);
                updated.add(plan.withPostDecorationSchemaBytes(postDecorationBytes));
                changed = true;
            }
            if (changed) child.setPlanAlternatives(updated);
        }
    }

    /**
     * Stub Substrait for the LM stage: a {@code Read { named_table: "input-<lmStageId>";
     * base_schema: wrapperOutput }} the parent reduce sink can register against. The plan's
     * {@code resolvedFragment} IS the wrapper (DAGBuilder builds it that way), but we don't
     * convert it — the LM stage runs Java-only scatter/gather/stitch and emits no Substrait
     * compute. We only need the schema-bearing Read so Stage 3's reduce sink derives a
     * partition schema via the standard producerPlanBytes path.
     */
    private static void convertLateMaterializationStage(Stage stage, CapabilityRegistry registry) {
        List<StagePlan> converted = new ArrayList<>(stage.getPlanAlternatives().size());
        for (StagePlan plan : stage.getPlanAlternatives()) {
            OpenSearchLateMaterialization wrapper = (OpenSearchLateMaterialization) plan.resolvedFragment();
            FragmentConvertor convertor = registry.getBackend(plan.backendId()).getFragmentConvertor();
            byte[] bytes = convertor.convertSchemaOnlyRead(stage.getStageId(), wrapper.getRowType());
            converted.add(plan.withConvertedBytes(bytes, List.of()).withInstructions(List.of()));
        }
        stage.setPlanAlternatives(converted);
    }

    private static List<InstructionNode> assembleInstructions(
        AnalyticsSearchBackendPlugin backend,
        StagePlan plan,
        FilterTreeShape treeShape,
        IntraOperatorDelegationBytes delegationBytes
    ) {
        FragmentInstructionHandlerFactory factory = backend.getInstructionHandlerFactory();
        LinkedList<InstructionNode> instructions = new LinkedList<>();
        RelNode resolvedFragment = plan.resolvedFragment();
        RelNode leaf = findLeaf(resolvedFragment);

        if (leaf instanceof OpenSearchTableScan tableScan) {
            // QTF narrows the Scan to [belowAnchorPhysicalFields..., __row_id__]; signal that to the
            // backend so it picks the row-id-aware table provider regardless of delegation.
            boolean requestsRowIds = tableScan.getRowType().getFieldNames().contains(OpenSearchLateMaterialization.ROW_ID_FIELD);
            List<DelegatedExpression> delegated = delegationBytes.getResult();
            if (!delegated.isEmpty()) {
                factory.createShardScanWithDelegationNode(treeShape, delegated.size(), requestsRowIds).ifPresent(instructions::add);
            } else {
                factory.createShardScanNode(requestsRowIds).ifPresent(instructions::add);
            }
            // Engine-native-merge PARTIAL (Type.APPROXIMATE + intermediateField with reducer == self,
            // currently APPROX_COUNT_DISTINCT for HLL): activate prepare_partial_plan +
            // force_aggregate_mode(Partial) so the data node strips Mode::Final and ships sketch state
            // on the wire (vs. cardinality scalar). SUM / COUNT / AVG / MIN / MAX are state == value
            // so they take the default executeLocalPlan path — no instruction needed.
            if (resolvedFragment instanceof OpenSearchAggregate agg
                && agg.getMode() == AggregateMode.PARTIAL
                && hasEngineNativeMergeMeasure(agg)) {
                LOGGER.debug(
                    "Emitting SETUP_PARTIAL_AGGREGATE for engine-native-merge PARTIAL fragment:\n{}",
                    org.apache.calcite.plan.RelOptUtil.toString(resolvedFragment)
                );
                factory.createPartialAggregateNode().ifPresent(instructions::add);
            }
        } else if (leaf instanceof OpenSearchStageInputScan && containsEngineNativeFinalAggregate(resolvedFragment)) {
            // Coord-side reduce stage with an engine-native-merge FINAL aggregate. Activate
            // prepare_final_plan + force_aggregate_mode(Final) so AggregateExec(Mode::Final) calls
            // merge_batch on the gathered sketch state (vs. update_batch which would treat the
            // state column as raw values and produce wrong cardinalities).
            LOGGER.debug(
                "Emitting SETUP_FINAL_AGGREGATE for engine-native-merge FINAL fragment:\n{}",
                org.apache.calcite.plan.RelOptUtil.toString(resolvedFragment)
            );
            factory.createFinalAggregateNode().ifPresent(instructions::add);
        }
        return instructions;
    }

    /**
     * Returns {@code true} when {@code agg} has at least one aggregate call whose
     * {@link org.opensearch.analytics.spi.AggregateFunction#intermediateFields()} declares a
     * single field with {@code reducer == self} — the engine-native-merge classification
     * (currently APPROX_COUNT_DISTINCT for HLL sketch merge).
     */
    private static boolean hasEngineNativeMergeMeasure(OpenSearchAggregate agg) {
        for (org.apache.calcite.rel.core.AggregateCall call : agg.getAggCallList()) {
            org.opensearch.analytics.spi.AggregateFunction func;
            try {
                func = org.opensearch.analytics.spi.AggregateFunction.fromSqlAggFunction(call.getAggregation());
            } catch (IllegalStateException ignored) {
                continue;
            }
            List<org.opensearch.analytics.spi.AggregateFunction.IntermediateField> fields = func.intermediateFields();
            if (fields != null && fields.size() == 1 && fields.get(0).reducer() == func) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns {@code true} when {@code root}'s subtree contains an
     * {@link OpenSearchAggregate} in {@link AggregateMode#FINAL} with at least one
     * engine-native-merge measure. Walks past Project / Sort / Filter wrappers above FINAL.
     */
    private static boolean containsEngineNativeFinalAggregate(RelNode root) {
        if (root instanceof OpenSearchAggregate agg && agg.getMode() == AggregateMode.FINAL && hasEngineNativeMergeMeasure(agg)) {
            return true;
        }
        for (RelNode child : root.getInputs()) {
            if (containsEngineNativeFinalAggregate(child)) return true;
        }
        return false;
    }

    /**
     * Accumulates serialized delegated query bytes during fragment conversion.
     *
     * <p>The resolver performs a single bottom-up traversal of the filter condition tree,
     * classifying each node as delegated (targets a non-operator backend like Lucene) or
     * native (evaluated by the driving backend). Tree-walking and combining logic is
     * delegated to {@link DelegatedPredicateCombiner}.
     */
    static final class IntraOperatorDelegationBytes {
        private final CapabilityRegistry registry;
        private final boolean fuseDualViable;
        private List<DelegatedExpression> delegatedExpressions;

        IntraOperatorDelegationBytes(CapabilityRegistry registry) {
            this(registry, false);
        }

        IntraOperatorDelegationBytes(CapabilityRegistry registry, boolean fuseDualViable) {
            this.registry = registry;
            this.fuseDualViable = fuseDualViable;
        }

        /**
         * Creates an annotation resolver that does a single bottom-up traversal.
         * Maximal same-backend delegated subtrees are converted via the backend's
         * {@code DelegatedSubtreeConvertor} into one DelegatedExpression each.
         */
        Function<OperatorAnnotation, RexNode> resolverFor(OpenSearchRelNode operator, RexBuilder rexBuilder) {
            String operatorBackend = operator.getViableBackends().getFirst();
            List<FieldStorageInfo> fieldStorage = operator.getOutputFieldStorage();
            if (delegatedExpressions == null) delegatedExpressions = new ArrayList<>();
            DelegatedPredicateCombiner classifier = new DelegatedPredicateCombiner(
                operatorBackend,
                fieldStorage,
                registry,
                rexBuilder,
                delegatedExpressions,
                fuseDualViable
            );
            return new AnnotationResolver() {

                @Override
                public RexNode resolveTree(RexNode condition) {
                    DelegatedPredicateCombiner.Classified result = classifier.classify(condition, this::apply);
                    if (result instanceof DelegatedPredicateCombiner.Delegated d) {
                        return classifier.finalizeDelegated(d);
                    }
                    return ((DelegatedPredicateCombiner.Resolved) result).node();
                }

                @Override
                public RexNode apply(OperatorAnnotation annotation) {
                    String annotationBackend = annotation.getViableBackends().getFirst();
                    if (annotationBackend.equals(operatorBackend)) {
                        // Performance-delegation candidate: dual-viable predicate kept on the operator's backend,
                        // but a peer can be opportunistically consulted at runtime.
                        if (annotation instanceof AnnotatedPredicate ap && !ap.getPerformanceDelegationBackends().isEmpty()) {
                            String peerBackend = ap.getPerformanceDelegationBackends().getFirst();
                            RexNode original = ap.unwrap();
                            if (!(original instanceof RexCall originalCall)) {
                                throw new IllegalStateException("Performance-delegation candidate must wrap a RexCall: " + original);
                            }
                            ScalarFunction function = ScalarFunction.fromSqlOperatorWithFallback(originalCall.getOperator());
                            DelegatedPredicateSerializer serializer = registry.getBackend(peerBackend)
                                .delegatedPredicateSerializers()
                                .get(function);
                            if (serializer == null) {
                                LOGGER.debug(
                                    "Performance-delegation skipped: no serializer for [{}] on delegated backend [{}]; falling back to native on operator [{}]",
                                    function,
                                    peerBackend,
                                    operatorBackend
                                );
                                return annotation.unwrap();
                            }
                            byte[] serialized = serializer.serialize(originalCall, fieldStorage);
                            LOGGER.debug(
                                "Performance-delegated annotation [id={}]: {} kept on operator [{}], wrapped for peer [{}], serialized {} bytes",
                                ap.getAnnotationId(),
                                function,
                                operatorBackend,
                                peerBackend,
                                serialized.length
                            );
                            if (delegatedExpressions == null) {
                                delegatedExpressions = new ArrayList<>();
                            }
                            delegatedExpressions.add(new DelegatedExpression(ap.getAnnotationId(), peerBackend, serialized));
                            return DelegationPossibleFunction.makeCall(rexBuilder, originalCall, ap.getAnnotationId());
                        }
                        LOGGER.debug(
                            "Native annotation [id={}]: backend [{}] matches operator",
                            annotation.getAnnotationId(),
                            operatorBackend
                        );
                        return annotation.unwrap();
                    }
                    RexNode original = annotation.unwrap();
                    if (!(original instanceof RexCall originalCall) || !(originalCall.getOperator() instanceof SqlFunction sqlFunction)) {
                        throw new IllegalStateException("Delegated expression must be a SqlFunction call: " + original);
                    }
                    ScalarFunction function = ScalarFunction.fromSqlFunction(sqlFunction);
                    DelegatedPredicateSerializer serializer = registry.getBackend(annotationBackend)
                        .getCapabilityProvider()
                        .delegatedPredicateSerializers()
                        .get(function);
                    if (serializer == null) {
                        throw new IllegalStateException(
                            "No DelegatedPredicateSerializer for ["
                                + function
                                + "] on backend ["
                                + annotationBackend
                                + "]. CapabilityRegistry should have rejected this at startup."
                        );
                    }
                    byte[] serialized = serializer.serialize(originalCall, fieldStorage);
                    LOGGER.debug(
                        "Delegated annotation [id={}]: {} from operator [{}] to [{}], serialized {} bytes",
                        annotation.getAnnotationId(),
                        function,
                        operatorBackend,
                        annotationBackend,
                        serialized.length
                    );
                    if (delegatedExpressions == null) {
                        delegatedExpressions = new ArrayList<>();
                    }
                    delegatedExpressions.add(new DelegatedExpression(annotation.getAnnotationId(), annotationBackend, serialized));
                    return annotation.makePlaceholder(rexBuilder);
                }
            };
        }

        List<DelegatedExpression> getResult() {
            return delegatedExpressions != null ? delegatedExpressions : List.of();
        }
    }

    /**
     * Dispatches conversion based on the fragment's leaf and top node types.
     */
    static byte[] convert(RelNode resolvedFragment, FragmentConvertor convertor, IntraOperatorDelegationBytes delegationBytes) {
        RelNode leaf = findLeaf(resolvedFragment);

        if (leaf instanceof OpenSearchTableScan) {
            // Partial agg at top: convert everything below it, then attach partial agg on top.
            // strippedInputs passed to stripAnnotations for schema validity (LogicalAggregate needs its inputs).
            if (resolvedFragment instanceof OpenSearchAggregate agg && agg.getMode() == AggregateMode.PARTIAL) {
                List<RelNode> strippedInputs = agg.getInputs().stream().map(input -> strip(input, delegationBytes)).toList();
                byte[] innerBytes = convertor.convertFragment(strippedInputs.getFirst());
                Function<OperatorAnnotation, RexNode> resolver = delegationBytes.resolverFor(agg, agg.getCluster().getRexBuilder());
                RelNode strippedAgg = agg.stripAnnotations(strippedInputs, resolver);
                return convertor.attachPartialAggOnTop(strippedAgg, innerBytes);
            }

            RelNode stripped = strip(resolvedFragment, delegationBytes);
            return convertor.convertFragment(stripped);
        }

        if (leaf instanceof OpenSearchStageInputScan) {
            return convertReduceFragment(resolvedFragment, convertor, delegationBytes);
        }

        if (leaf instanceof org.opensearch.analytics.planner.rel.OpenSearchValues) {
            // Coord-only literal source — convert the whole fragment via the same isthmus
            // path as reduce fragments. isthmus emits ReadRel.VirtualTable for the Values
            // leaf; DataFusion executes it locally without any input partitions.
            RelNode stripped = strip(resolvedFragment, delegationBytes);
            return convertor.convertFragment(stripped);
        }

        throw new IllegalStateException(
            "Unknown leaf type [" + leaf.getClass().getSimpleName() + "]. " + "Add a FragmentConversionStrategy for this leaf type."
        );
    }

    /**
     * Reduce stage conversion: strips ExchangeReducer, converts the final agg fragment
     * (with StageInputScan as leaf for schema), then attaches any operators above it
     * (Sort, Project, etc.) via attachFragmentOnTop.
     *
     * <p>Single-input ancestors of a single gathered subtree (Sort/Project/Aggregate over
     * a partial agg) reach convertFragment as soon as we see a node whose inputs
     * are all ExchangeReducers, and attach via attachFragmentOnTop on the way back up.
     *
     * <p>Multi-input nodes (Join, Union, Intersect, Minus) are converted as a single
     * subtree via convertFragment: isthmus handles all of them natively, and
     * rewriting OpenSearchStageInputScan leaves to plain TableScans (inside the convertor)
     * lets the whole gathered subtree serialize in one pass. No post-conversion
     * substrait-level stitching is needed.
     */
    private static byte[] convertReduceFragment(RelNode node, FragmentConvertor convertor, IntraOperatorDelegationBytes delegationBytes) {
        return convertReduceNode(node, convertor, false, delegationBytes);
    }

    private static byte[] convertReduceNode(
        RelNode node,
        FragmentConvertor convertor,
        boolean finalAggConverted,
        IntraOperatorDelegationBytes delegationBytes
    ) {
        if (node instanceof OpenSearchExchangeReducer) {
            // Strip ExchangeReducer — StageInputScan below it is the schema source.
            return convertor.convertFragment(strip(node.getInputs().getFirst(), delegationBytes));
        }
        if (node instanceof OpenSearchRelNode openSearchNode) {
            List<RelNode> strippedInputs = node.getInputs().stream().map(input -> strip(input, delegationBytes)).toList();
            Function<OperatorAnnotation, RexNode> resolver = delegationBytes.resolverFor(openSearchNode, node.getCluster().getRexBuilder());
            RelNode strippedNode = openSearchNode.stripAnnotations(strippedInputs, resolver);

            if (!finalAggConverted) {
                // First OpenSearchRelNode whose ALL inputs are ExchangeReducers is treated as the
                // boundary between the coordinator-side fragment and the data-node child stages.
                // For single-input shapes (Sort/Project/Aggregate over a partial agg) this is the
                // final-aggregate operator; for multi-input shapes (Union) every branch is itself
                // an ER → StageInputScan, and the entire Union+ER subtree is converted as one
                // fragment so all branches end up in the same Substrait plan reading from their
                // respective input partitions.
                boolean allChildrenAreExchangeReducer = !node.getInputs().isEmpty()
                    && node.getInputs().stream().allMatch(input -> input instanceof OpenSearchExchangeReducer);
                if (allChildrenAreExchangeReducer && node.getInputs().size() == 1) {
                    List<RelNode> finalAggInputs = new ArrayList<>(node.getInputs().size());
                    for (RelNode input : node.getInputs()) {
                        // Skip the ER, keep StageInputScan below it as the leaf for schema inference.
                        finalAggInputs.add(strip(input.getInputs().getFirst(), delegationBytes));
                    }
                    RelNode finalAggFragment = openSearchNode.stripAnnotations(finalAggInputs, resolver);
                    return convertor.convertFragment(finalAggFragment);
                }

                // LM-fed reduce stage (post-LM Stage 3): the LM stage emits a stitched VSR straight
                // into this stage's input partition with no ExchangeReducer between, so the
                // StageInputScan leaf sits directly under this op. Convert the whole node as one
                // fragment; the convertor's rewriteStageInputScans turns the leaf into a NamedScan
                // so isthmus can serialize it.
                boolean allChildrenAreStageInputScan = !node.getInputs().isEmpty()
                    && node.getInputs().stream().allMatch(input -> input instanceof OpenSearchStageInputScan);
                if (allChildrenAreStageInputScan) {
                    return convertor.convertFragment(strip(node, delegationBytes));
                }
            }

            // Multi-input node (Join, Union, Intersect, Minus): isthmus handles all of them
            // natively. The whole subtree — multi-input node + its branches + ERs +
            // StageInputScans — serializes in one convertFragment pass. The convertor's
            // StageInputScan → plain TableScan rewrite makes the leaves isthmus-friendly without
            // any post-conversion substrait-level stitching.
            if (node.getInputs().size() >= 2) {
                return convertor.convertFragment(strip(node, delegationBytes));
            }

            // Single-input operator above the final-fragment boundary — convert child first, then attach.
            byte[] innerBytes = convertReduceNode(node.getInputs().getFirst(), convertor, false, delegationBytes);
            return convertor.attachFragmentOnTop(strippedNode, innerBytes);
        }
        throw new IllegalStateException("Unexpected reduce stage node: " + node.getClass().getSimpleName());
    }

    /** Recursively strips annotations bottom-up. Keeps OpenSearchStageInputScan as-is. */
    private static RelNode strip(RelNode node, IntraOperatorDelegationBytes delegationBytes) {
        if (node instanceof OpenSearchStageInputScan) {
            return node; // kept for schema inference at reduce stage
        }
        if (node instanceof OpenSearchExchangeReducer) {
            return strip(node.getInputs().getFirst(), delegationBytes);
        }
        List<RelNode> strippedChildren = new ArrayList<>(node.getInputs().size());
        for (RelNode input : node.getInputs()) {
            strippedChildren.add(strip(input, delegationBytes));
        }
        if (node instanceof OpenSearchRelNode openSearchNode) {
            Function<OperatorAnnotation, RexNode> resolver = delegationBytes.resolverFor(openSearchNode, node.getCluster().getRexBuilder());
            if (node instanceof OpenSearchFilter filter && resolver instanceof AnnotationResolver ar) {
                // Combine delegated predicates in a single pass, then strip with simple unwrapper
                RexNode resolved = ar.resolveTree(filter.getCondition());
                RexNode flattened = RexUtil.flatten(node.getCluster().getRexBuilder(), resolved);
                return LogicalFilter.create(strippedChildren.getFirst(), flattened);
            }
            return openSearchNode.stripAnnotations(strippedChildren, resolver);
        }
        return node;
    }

    private static RelNode findLeaf(RelNode node) {
        if (node.getInputs().isEmpty()) {
            return node;
        }
        return findLeaf(node.getInputs().getFirst());
    }
}
