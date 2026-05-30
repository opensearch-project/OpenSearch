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
     */
    public static void convertAll(QueryDAG dag, CapabilityRegistry registry) {
        convertStage(dag.rootStage(), registry);
        // Root stage executes locally at coordinator — store factory for instruction dispatch.
        Stage root = dag.rootStage();
        if (root.getExchangeSinkProvider() != null && !root.getPlanAlternatives().isEmpty()) {
            AnalyticsSearchBackendPlugin backend = registry.getBackend(root.getPlanAlternatives().getFirst().backendId());
            root.setInstructionHandlerFactory(backend.getInstructionHandlerFactory());
        }
    }

    private static void convertStage(Stage stage, CapabilityRegistry registry) {
        for (Stage child : stage.getChildStages()) {
            convertStage(child, registry);
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

            // Derive filter tree shape BEFORE stripping (annotations must be intact)
            OpenSearchFilter filter = RelNodeUtils.findNode(plan.resolvedFragment(), OpenSearchFilter.class);
            FilterTreeShape treeShape = filter != null
                ? FilterTreeShapeDeriver.derive(filter, plan.backendId())
                : FilterTreeShape.NO_DELEGATION;

            IntraOperatorDelegationBytes delegationBytes = new IntraOperatorDelegationBytes(registry);
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
     * Detect a decorator-induced schema delta between a child stage's produced rowType and
     * what the parent declares it expects, and emit a schema-only Read for partition registration.
     *
     * <p>The expected rowType lives on the parent's {@code OpenSearchStageInputScan(childStageId)}
     * placeholder, which {@code DAGBuilder.cutAtExchange} sets to the reducer's output rowType
     * (widened by the rewriter when a decorator like {@code OrdinalAppendingSink} runs). The
     * produced rowType is the child's fragment top. When the two differ, the producer's natural
     * schema undersells what arrives at the partition boundary post-decorator — the reduce sink
     * needs the wider one.
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
            // Cheap int compare first, then digest string compare via equals.
            if (produced.getFieldCount() == expected.getFieldCount() && produced.equals(expected)) continue;

            List<StagePlan> updated = new ArrayList<>(child.getPlanAlternatives().size());
            for (StagePlan plan : child.getPlanAlternatives()) {
                FragmentConvertor convertor = registry.getBackend(plan.backendId()).getFragmentConvertor();
                byte[] postDecorationBytes = convertor.convertSchemaOnlyRead(child.getStageId(), expected);
                updated.add(plan.withPostDecorationSchemaBytes(postDecorationBytes));
            }
            child.setPlanAlternatives(updated);
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
        RelNode leaf = findLeaf(plan.resolvedFragment());

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
        }
        return instructions;
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
        private List<DelegatedExpression> delegatedExpressions;

        IntraOperatorDelegationBytes(CapabilityRegistry registry) {
            this.registry = registry;
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
                delegatedExpressions
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
     * Reduce-stage conversion: serialise the entire coordinator-side subtree (everything from
     * the ExchangeReducer boundary up through the user-level operators) in one convertFragment
     * pass. {@code strip(...)} removes ExchangeReducers and annotations on the way down, leaving
     * {@code OpenSearchStageInputScan} as the leaf; the convertor's {@code rewriteStageInputScans}
     * then swaps that leaf for a plain TableScan. Isthmus serialises the whole subtree
     * end-to-end, deriving every fieldRef from the same row-type system.
     *
     * <p>Splitting reduce-stage chains into {@code convertFragment(child) + attachFragmentOnTop
     * (wrapper, ...)} used to break helper-chain shapes (e.g.
     * {@code Sort(Project(Project-with-window(ER(StageInputScan))))} that PPL emits for
     * {@code streamstats by}) — the wrapper conversion bound fieldRefs against the placeholder
     * schema while the inner plan's actual schema may have shifted (partial-agg lowering,
     * NULL-typed CASE branches, etc.), producing DataFusion runtime panics like "index out of
     * bounds: len=6 idx=14". A single conversion pass eliminates that whole class of bugs.
     */
    private static byte[] convertReduceFragment(RelNode node, FragmentConvertor convertor, IntraOperatorDelegationBytes delegationBytes) {
        return convertor.convertFragment(strip(node, delegationBytes));
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
