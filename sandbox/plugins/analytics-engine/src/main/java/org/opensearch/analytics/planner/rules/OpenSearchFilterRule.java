/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.AnnotatedPredicate;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.spi.DelegationType;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.analytics.spi.ScalarFunction;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.calcite.sql.type.SqlTypeName.BOOLEAN;

/**
 * Converts {@link Filter} → {@link OpenSearchFilter}.
 *
 * <p>Annotates each leaf predicate with viable backends by checking field storage
 * in the child's {@link FieldStorageInfo} and operator support in backend capabilities.
 * Wraps each leaf in an {@link AnnotatedPredicate} RexNode. Computes
 * operator-level viable backends from per-predicate annotations and
 * delegation capabilities.
 *
 * <p>A separate pass after CBO reads these annotations to generate alternative
 * StagePlans with different delegation strategies.
 *
 * @opensearch.internal
 */
public class OpenSearchFilterRule extends RelOptRule {

    private static final Logger LOGGER = LogManager.getLogger(OpenSearchFilterRule.class);

    private static final String LUCENE_BACKEND_NAME = "lucene";
    private static final String LUCENE_DATA_FORMAT = "lucene";

    /**
     * SqlFunction whose getName() upper-cases to {@link ScalarFunction#MATCHALL}.
     * Constructed inline at the only call site that materializes a synthetic
     * match-all RexCall ({@link #injectLuceneLiveDocsClauseIfNeeded}). The Lucene-side
     * {@code MatchAllSerializer} produces a {@code MatchAllQueryBuilder} from
     * this RexCall; at execution time, that query's {@code Weight.scorer} iterator
     * filters out soft-deleted documents — guaranteeing live-docs filtering even
     * when no user predicate naturally targets Lucene.
     */
    private static final SqlFunction MATCH_ALL_OPERATOR = new SqlFunction(
        "MATCHALL",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.NILADIC,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    private final PlannerContext context;

    public OpenSearchFilterRule(PlannerContext context) {
        super(operand(Filter.class, operand(RelNode.class, any())), "OpenSearchFilterRule");
        this.context = context;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter filter = call.rel(0);
        RelNode child = call.rel(1);

        if (filter instanceof OpenSearchFilter) {
            return;
        }

        if (!(child instanceof OpenSearchRelNode openSearchInput)) {
            throw new IllegalStateException(
                "Filter rule encountered unmarked child ["
                    + child.getClass().getSimpleName()
                    + "]. Ensure all child operators are marked before filter."
            );
        }

        List<String> childViableBackends = openSearchInput.getViableBackends();
        List<FieldStorageInfo> childFieldStorage = openSearchInput.getOutputFieldStorage();

        // Annotate every leaf predicate with viable backends.
        RexNode annotatedUserCondition = annotateCondition(filter.getCondition(), childFieldStorage, childViableBackends);

        // Inject a synthetic MATCHALL() leaf for hybrid Lucene+Parquet shards that have
        // no naturally-Lucene predicate, so live-docs filtering still happens via
        // Lucene's Weight.scorer iterator. Prepended before annotateCondition so the
        // existing FULL_TEXT field-less path resolves it to viableBackends=["lucene"]
        // without code duplication.
        RexNode annotatedCondition = injectLuceneLiveDocsClauseIfNeeded(
            annotatedUserCondition,
            childViableBackends,
            childFieldStorage,
            filter.getCluster().getRexBuilder()
        );

        // Compute operator-level viable backends: must be viable for child AND handle predicates
        List<String> viableBackends = computeFilterViableBackends(annotatedCondition, childViableBackends);

        if (viableBackends.isEmpty()) {
            throw new IllegalStateException(
                "No backend can execute filter: no viable backend among "
                    + childViableBackends
                    + " can evaluate all predicates and no delegation path exists"
            );
        }

        LOGGER.debug("Filter viable backends: {} (child viable: {})", viableBackends, childViableBackends);

        call.transformTo(
            new OpenSearchFilter(
                filter.getCluster(),
                child.getTraitSet(),
                RelNodeUtils.unwrapHep(filter.getInput()),
                annotatedCondition,
                viableBackends
            )
        );
    }

    // ---- Predicate annotation ----
    private RexNode injectLuceneLiveDocsClauseIfNeeded(
        RexNode annotatedCondition,
        List<String> childViableBackends,
        List<FieldStorageInfo> childFieldStorage,
        org.apache.calcite.rex.RexBuilder rexBuilder
    ) {
        // Lucene Backend can be lucene or mock-lucene, so better get the name from registry for Match_ALL.
        List<String> luceneBackends = getLuceneBackendName();
        if (luceneBackends.isEmpty() == true) {
            return annotatedCondition;
        }

        assert luceneBackends.size() == 1;
        String luceneBackEndName = luceneBackends.get(0);

        // Guard 1: Lucene is one of the configured data formats on this shard
        // (i.e., at least one non-derived field carries Lucene as a storage format).
        boolean luceneConfigured = false;
        for (FieldStorageInfo info : childFieldStorage) {
            if (info.isDerived()) continue;
            if (info.getIndexFormats().contains(LUCENE_DATA_FORMAT) || info.getDocValueFormats().contains(LUCENE_DATA_FORMAT)) {
                luceneConfigured = true;
                break;
            }
        }
        if (luceneConfigured == false) {
            return annotatedCondition;
        }

        // Guard 2: every annotated leaf in the user's condition has viableBackends == [datafusion]
        // (exactly DataFusion, no other backend). If any leaf includes Lucene (or anything else),
        // skip — that leaf is already Lucene-eligible (or unrelated to the live-docs concern).
        List<AnnotatedPredicate> leaves = new ArrayList<>();
        collectAnnotatedPredicates(annotatedCondition, leaves);
        if (leaves.isEmpty()) {
            return annotatedCondition;
        }
        for (AnnotatedPredicate leaf : leaves) {
            List<String> viable = leaf.getViableBackends();
            if (viable.size() == 1 && luceneBackEndName.equals(viable.get(0))) {
                return annotatedCondition;
            }
        }

        // All guards passed — build the synthetic, pre-annotated MATCHALL leaf and AND-attach it.
        RexNode matchAllCall = rexBuilder.makeCall(rexBuilder.getTypeFactory().createSqlType(BOOLEAN), MATCH_ALL_OPERATOR, List.of());
        AnnotatedPredicate annotatedMatchAll = new AnnotatedPredicate(
            matchAllCall.getType(),
            matchAllCall,
            List.of(luceneBackEndName),
            context.nextAnnotationId()
        );
        return RexUtil.composeConjunction(rexBuilder, List.of(annotatedCondition, annotatedMatchAll));
    }

    private List<String> getLuceneBackendName() {
        return context.getCapabilityRegistry().filterBackends(ScalarFunction.MATCHALL, FieldType.TEXT, LUCENE_BACKEND_NAME);
    }

    // ---- Predicate annotation ----

    /**
     * Recursively walks the condition tree. Boolean connectives (AND, OR, NOT) are
     * preserved — we recurse into their children. Leaf predicates are wrapped in
     * {@link AnnotatedPredicate} with viable backends resolved from child's field storage.
     */
    private RexNode annotateCondition(RexNode condition, List<FieldStorageInfo> fieldStorageInfos, List<String> childViableBackends) {
        if (!(condition instanceof RexCall rexCall)) {
            return condition;
        }
        if (rexCall.getKind() == SqlKind.AND || rexCall.getKind() == SqlKind.OR || rexCall.getKind() == SqlKind.NOT) {
            List<RexNode> annotatedOperands = new ArrayList<>();
            for (RexNode operand : rexCall.getOperands()) {
                annotatedOperands.add(annotateCondition(operand, fieldStorageInfos, childViableBackends));
            }
            return rexCall.clone(rexCall.getType(), annotatedOperands);
        }
        List<String> viableBackends = resolveViableBackends(rexCall, fieldStorageInfos, childViableBackends);
        // TODO: viableBackends here is computed from each backend's declared FilterCapability
        // (see resolveViableBackends below). Today a backend can advertise a function as
        // filter-capable without actually shipping a DelegatedPredicateSerializer for it; the
        // mismatch only surfaces when FragmentConversion tries to delegate (correctness) or
        // wrap as performance-delegation. CapabilityRegistry should validate at startup that
        // every declared FilterCapability has a matching serializer registered, and reject
        // the plugin otherwise — fail-fast at boot rather than at first dual-viable query.
        // Needs revisiting.
        return new AnnotatedPredicate(rexCall.getType(), rexCall, viableBackends, context.nextAnnotationId());
    }

    /**
     * Determines which backends can evaluate this leaf predicate.
     * Extracts all field references, looks up their {@link FieldStorageInfo} from the child,
     * checks backend format support, operator capability, and operator+fieldType support.
     * Intersects across all referenced fields.
     */
    private List<String> resolveViableBackends(
        RexCall predicate,
        List<FieldStorageInfo> fieldStorageInfos,
        List<String> childViableBackends
    ) {
        Set<Integer> fieldIndices = new HashSet<>();
        collectFieldIndices(predicate, fieldIndices);

        CapabilityRegistry registry = context.getCapabilityRegistry();

        ScalarFunction function = ScalarFunction.fromSqlOperatorWithFallback(predicate.getOperator());
        if (function == null) {
            throw new IllegalStateException(
                "Unrecognized filter operator [" + predicate.getOperator().getName() + " / " + predicate.getKind() + "]"
            );
        }

        if (fieldIndices.isEmpty()) {
            // Multi-field full-text functions (multi_match, query_string, simple_query_string)
            // encode field names as string literals in nested MAPs rather than RexInputRef.
            // Resolve viability against any backend that supports the function on text fields.
            if (function.getCategory() == ScalarFunction.Category.FULL_TEXT) {
                return new ArrayList<>(registry.filterBackendsAnyFormat(function, FieldType.TEXT));
            }
            // Non-deterministic predicates (e.g. RAND() > 0) have no field references but
            // ReduceExpressionsRule deliberately leaves them alone — folding them would
            // change semantics. Any filter-capable backend that supports the operator can
            // evaluate them as-is.
            if (!RexUtil.isDeterministic(predicate)) {
                return new ArrayList<>(childViableBackends);
            }
            throw new UnsupportedOperationException(
                "Constant predicate with no field references reached the filter rule: ["
                    + predicate
                    + "]. ReduceExpressionsRule in PlannerImpl should have eliminated it."
            );
        }

        Set<String> viableSet = new HashSet<>(registry.filterCapableBackends());

        for (int fieldIndex : fieldIndices) {
            FieldStorageInfo storageInfo = FieldStorageInfo.resolve(fieldStorageInfos, fieldIndex);

            Set<String> fieldViable;
            if (storageInfo.isDerived()) {
                // Derived columns (post-Aggregate, post-Join, post-Union, post-Project) are
                // computed in memory by the producer. The filter can only run on a backend
                // the producer is also viable for (its child's viableBackends), and further
                // only on backends that support this function on the field's logical type —
                // delegation isn't applicable because there's no physical storage to delegate
                // a scan against. Surfaced by testHavingFilterAfterJoin_multiShard etc., where
                // a HAVING clause filters on a stats-derived column.
                fieldViable = new HashSet<>(childViableBackends);
                fieldViable.retainAll(registry.filterBackendsAnyFormat(function, storageInfo.getFieldType()));
            } else {
                // Format-aware: backends that can access this field's storage (doc values + index).
                // A backend is viable only if it has the field in its own storage formats — ensuring
                // delegation targets are also field-storage-aware (e.g. Lucene is viable for a keyword
                // field only when the field has indexFormats=[lucene] set in the mapping).
                // TODO: for FULL_TEXT operators, extract required params from RexCall
                fieldViable = new HashSet<>(registry.filterBackendsForField(function, storageInfo));
            }

            viableSet.retainAll(fieldViable);
        }

        if (viableSet.isEmpty()) {
            throw new IllegalStateException(
                "No backend can evaluate filter predicate ["
                    + predicate.getKind()
                    + "] on fields "
                    + fieldIndices.stream()
                        .filter(i -> i < fieldStorageInfos.size())
                        .map(i -> fieldStorageInfos.get(i).getFieldName() + ":" + fieldStorageInfos.get(i).getMappingType())
                        .toList()
            );
        }
        return new ArrayList<>(viableSet);
    }

    /** Extracts all field indices referenced by RexInputRef nodes in the expression. */
    private void collectFieldIndices(RexNode node, Set<Integer> result) {
        if (node instanceof RexInputRef inputRef) {
            result.add(inputRef.getIndex());
        } else if (node instanceof RexCall rexCall) {
            for (RexNode operand : rexCall.getOperands()) {
                collectFieldIndices(operand, result);
            }
        }
    }

    // ---- Operator-level viable backends ----

    /**
     * Computes which backends can be the primary executor of this filter.
     * A backend is viable if:
     * 1. It is the child backend (data flows from child), AND for every predicate
     *    it can evaluate natively or delegate to another backend.
     * 2. OR it is a different backend that the child can delegate the entire filter to
     *    (child supports FILTER delegation, this backend accepts it, and this backend
     *    can handle all predicates natively).
     */
    private List<String> computeFilterViableBackends(RexNode annotatedCondition, List<String> childViableBackends) {
        List<AnnotatedPredicate> predicates = new ArrayList<>();
        collectAnnotatedPredicates(annotatedCondition, predicates);

        if (predicates.isEmpty()) {
            return new ArrayList<>(childViableBackends);
        }

        List<String> viable = new ArrayList<>();
        CapabilityRegistry registry = context.getCapabilityRegistry();

        for (String candidateName : childViableBackends) {
            if (!registry.filterCapableBackends().contains(candidateName)) {
                continue;
            }

            boolean canHandleAll = true;
            for (AnnotatedPredicate predicate : predicates) {
                if (!registry.canHandle(candidateName, predicate.getViableBackends(), DelegationType.FILTER)) {
                    canHandleAll = false;
                    break;
                }
            }
            if (canHandleAll) {
                viable.add(candidateName);
            }
        }
        return viable;
    }

    // ---- Strategy determination ----

    /** Recursively collects all {@link AnnotatedPredicate} leaves from the condition tree. */
    private void collectAnnotatedPredicates(RexNode node, List<AnnotatedPredicate> result) {
        if (node instanceof AnnotatedPredicate annotatedPredicate) {
            result.add(annotatedPredicate);
        } else if (node instanceof RexCall rexCall) {
            for (RexNode operand : rexCall.getOperands()) {
                collectAnnotatedPredicates(operand, result);
            }
        }
    }
}
