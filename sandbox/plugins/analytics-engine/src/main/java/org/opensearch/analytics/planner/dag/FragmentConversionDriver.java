/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.AnnotatedPredicate;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
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
            List<InstructionNode> instructions = assembleInstructions(backend, plan, treeShape, delegationBytes);

            converted.add(plan.withConvertedBytes(bytes, delegationBytes.getResult()).withInstructions(instructions));
        }
        stage.setPlanAlternatives(converted);
        // Store factory on coordinator-reduce stages (local execution, no serialization needed).
        // Shard stages get the factory from the local backend plugin at the data node.
        if (stage.getExchangeSinkProvider() != null && !converted.isEmpty()) {
            AnalyticsSearchBackendPlugin backend = registry.getBackend(converted.getFirst().backendId());
            stage.setInstructionHandlerFactory(backend.getInstructionHandlerFactory());
        }
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

        if (leaf instanceof OpenSearchTableScan) {
            List<DelegatedExpression> delegated = delegationBytes.getResult();
            if (!delegated.isEmpty()) {
                // Delegation exists — use ShardScanWithDelegationInstructionNode which carries
                // treeShape + count for the driving backend to configure its custom scan operator
                factory.createShardScanWithDelegationNode(treeShape, delegated.size()).ifPresent(instructions::add);
            } else {
                factory.createShardScanNode().ifPresent(instructions::add);
            }
        }
        return instructions;
    }

    /**
     * Lazily accumulates serialized delegated query bytes during fragment conversion.
     * Only allocates the map when the first delegated annotation is encountered.
     *
     * <p>TODO: combine same-backend AnnotatedPredicate siblings into one serialized
     * predicate per (operator, accepting backend) pair before {@link #resolverFor}
     * runs. Today every AnnotatedPredicate is serialized in isolation, so a query
     * like {@code match(message, 'a') AND match(message, 'b') AND match(message, 'c')}
     * produces three separate {@link DelegatedExpression}s, three Lucene Weights, and
     * three FFM collectDocs round-trips per RG. Lucene can intersect skip-lists
     * across terms natively if we hand it a BooleanQuery, so a pre-strip pass
     * should walk the AND/OR/NOT tree, group adjacent same-backend predicates that
     * share an accepting backend, and ask the accepting backend to <em>combine</em>
     * them into one serialized Lucene BooleanQuery (one DelegatedExpression, one
     * Weight, one collectDocs per RG). Same shape applies to performance-delegation
     * candidates grouped by their (operator, peer) backend pair. Needs a new
     * {@code combine(List<RexCall>) -> byte[]} method on DelegatedPredicateSerializer
     * (current contract serializes one expression at a time). DF-side same-backend
     * grouping already happens naturally in Substrait — only the peer-bound side
     * needs this work. See requirements.md:34-37 +
     * features/shard-cost-function/analysis/filter-delegation-deep-dive/09-revamp-notes.md.
     *
     * <p>An attempt at this as a post-marking HEP rule
     * ({@code CombineDelegatedPredicatesRule}, reverted) hit two design blockers:
     * (1) Substrait wire representation for a fused {@code original = AND(call1,
     * call2, ...)} leaf — the resolver below requires a {@code SqlFunction} operator,
     * but AND is a connective; (2) Receiving-backend (Lucene) needs a way to turn the
     * combined payload back into a single BooleanQuery / Weight without polluting
     * {@code ScalarFunction} with AND. Resolve those before retrying.
     *
     * <p>Note: combining also subsumes the "multi-leaf performance consultation"
     * follow-up — if N adjacent dual-viable leaves fuse into one
     * {@code delegation_possible(AND(...), id)} marker, the Rust SingleCollector
     * evaluator only needs to consult one peer per RG (the fused query) instead of
     * iterating multiple delegation leaves. The Rust-side
     * {@code performance_provider_locks} loop becomes trivially single-key. No
     * separate multi-leaf change required once combining lands.
     * Needs revisiting.
     */
    static final class IntraOperatorDelegationBytes {
        private final CapabilityRegistry registry;
        private List<DelegatedExpression> delegatedExpressions;

        IntraOperatorDelegationBytes(CapabilityRegistry registry) {
            this.registry = registry;
        }

        /**
         * Creates an annotation resolver scoped to a specific operator. Compares each
         * annotation's viable backend against the operator's backend: native annotations
         * are unwrapped, delegated ones are serialized and replaced with a placeholder.
         */
        Function<OperatorAnnotation, RexNode> resolverFor(OpenSearchRelNode operator, RexBuilder rexBuilder) {
            String operatorBackend = operator.getViableBackends().getFirst();
            List<FieldStorageInfo> fieldStorage = operator.getOutputFieldStorage();
            return annotation -> {
                String annotationBackend = annotation.getViableBackends().getFirst();
                if (annotationBackend.equals(operatorBackend)) {
                    // Performance-delegation candidate: dual-viable predicate kept on the operator's backend,
                    // but a peer can be opportunistically consulted at runtime. Wrap with delegation_possible
                    // so the original predicate is preserved AND the peer can be reached via annotationId.
                    if (annotation instanceof AnnotatedPredicate ap && !ap.getPerformanceDelegationBackends().isEmpty()) {
                        // TODO: pick the best peer instead of the first when more than two backends are viable.
                        String peerBackend = ap.getPerformanceDelegationBackends().getFirst();
                        RexNode original = ap.unwrap();
                        if (!(original instanceof RexCall originalCall)) {
                            throw new IllegalStateException("Performance-delegation candidate must wrap a RexCall: " + original);
                        }
                        // Performance-delegated predicates are typically SqlBinaryOperators (=, <, >, etc.),
                        // not SqlFunctions like MATCH_PHRASE. fromSqlOperatorWithFallback handles both.
                        ScalarFunction function = ScalarFunction.fromSqlOperatorWithFallback(originalCall.getOperator());
                        DelegatedPredicateSerializer serializer = registry.getBackend(peerBackend)
                            .getCapabilityProvider()
                            .delegatedPredicateSerializers()
                            .get(function);
                        if (serializer == null) {
                            // Delegated backend declared filter capability for this op but doesn't
                            // ship a serializer for it (e.g. Lucene declares LESS_THAN_OR_EQUAL but
                            // only EqualsSerializer is wired today). Without a serializer we can't
                            // emit a delegation_possible(...) marker, so fall back to native: just
                            // unwrap as a regular predicate evaluated by the operator's own backend.
                            // Same end result as a single-viable predicate — no perf delegation for
                            // this leaf, correctness preserved. CapabilityRegistry startup validation
                            // will eventually catch the capability/serializer mismatch at boot and reject
                            // the plugin instead of silently degrading at query time.
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
                    LOGGER.debug("Native annotation [id={}]: backend [{}] matches operator", annotation.getAnnotationId(), operatorBackend);
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
