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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.FieldStorageInfo;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.FieldStorageInfo;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.AnnotatedPredicate;
import org.opensearch.analytics.planner.rel.FullTextFunctions;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.spi.DelegationType;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.analytics.spi.FilterOperator;
import org.opensearch.analytics.spi.OperatorCapability;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
                "Filter rule encountered unmarked child [" + child.getClass().getSimpleName()
                    + "]. Ensure all child operators are marked before filter.");
        }

        List<String> childViableBackends = openSearchInput.getViableBackends();
        List<FieldStorageInfo> childFieldStorage = openSearchInput.getOutputFieldStorage();

        RelDataType inputRowType = child.getRowType();

        // Annotate every leaf predicate with viable backends
        RexNode annotatedCondition = annotateCondition(filter.getCondition(), inputRowType,
            childFieldStorage, childViableBackends);

        // Compute operator-level viable backends: must be viable for child AND handle predicates
        List<String> viableBackends = computeFilterViableBackends(annotatedCondition, childViableBackends);

        if (viableBackends.isEmpty()) {
            throw new IllegalStateException(
                "No backend can execute filter: no viable backend among " + childViableBackends
                    + " can evaluate all predicates and no delegation path exists");
        }

        LOGGER.debug("Filter viable backends: {} (child viable: {})", viableBackends, childViableBackends);

        call.transformTo(new OpenSearchFilter(
            filter.getCluster(),
            child.getTraitSet(),
            RelNodeUtils.unwrapHep(filter.getInput()),
            annotatedCondition,
            viableBackends
        ));
    }

    // ---- Predicate annotation ----

    /**
     * Recursively walks the condition tree. Boolean connectives (AND, OR, NOT) are
     * preserved — we recurse into their children. Leaf predicates are wrapped in
     * {@link AnnotatedPredicate} with viable backends resolved from child's field storage.
     */
    private RexNode annotateCondition(RexNode condition, RelDataType inputRowType,
                                      List<FieldStorageInfo> fieldStorage,
                                      List<String> childViableBackends) {
        if (!(condition instanceof RexCall rexCall)) {
            return condition;
        }
        if (rexCall.getKind() == SqlKind.AND || rexCall.getKind() == SqlKind.OR || rexCall.getKind() == SqlKind.NOT) {
            List<RexNode> annotatedOperands = new ArrayList<>();
            for (RexNode operand : rexCall.getOperands()) {
                annotatedOperands.add(annotateCondition(operand, inputRowType, fieldStorage, childViableBackends));
            }
            return rexCall.clone(rexCall.getType(), annotatedOperands);
        }
        List<String> viableBackends = resolveViableBackends(rexCall, inputRowType, fieldStorage, childViableBackends);
        return new AnnotatedPredicate(rexCall.getType(), rexCall, viableBackends, context.nextAnnotationId());
    }

    /**
     * Determines which backends can evaluate this leaf predicate.
     * Extracts all field references, looks up their {@link FieldStorageInfo} from the child,
     * checks backend format support, operator capability, and operator+fieldType support.
     * Intersects across all referenced fields.
     */
    private List<String> resolveViableBackends(RexCall predicate, RelDataType inputRowType,
                                               List<FieldStorageInfo> fieldStorage,
                                               List<String> childViableBackends) {
        Set<Integer> fieldIndices = new HashSet<>();
        collectFieldIndices(predicate, fieldIndices);

        CapabilityRegistry registry = context.getCapabilityRegistry();

        if (fieldIndices.isEmpty()) {
            return new ArrayList<>(registry.operatorBackends(OperatorCapability.FILTER));
        }

        FilterOperator operator = null;
        if (predicate.getOperator() instanceof SqlFunction sqlFunction) {
            operator = FullTextFunctions.toFilterOperator(sqlFunction);
        }
        if (operator == null) {
            operator = FilterOperator.fromSqlKind(predicate.getKind());
        }
        if (operator == null) {
            throw new IllegalStateException("Unrecognized filter operator [" + predicate.getKind() + "]");
        }

        Set<String> viableSet = new HashSet<>(registry.operatorBackends(OperatorCapability.FILTER));

        for (int fieldIndex : fieldIndices) {
            if (fieldIndex >= fieldStorage.size()) {
                continue;
            }
            FieldStorageInfo storageInfo = fieldStorage.get(fieldIndex);
            FieldType fieldType = storageInfo.getFieldType();
            if (fieldType == null) {
                throw new IllegalStateException("Unrecognized field type [" + storageInfo.getMappingType()
                    + "] for field [" + storageInfo.getFieldName() + "]");
            }

            // TODO: for FULL_TEXT operators, extract required params from RexCall
            // and use registry.fullTextFilterBackends() instead
            Set<String> fieldViable = new HashSet<>();
            if (storageInfo.isDerived()) {
                // Derived column — only child viable backends + their delegation targets
                List<String> anyFormat = registry.filterBackendsAnyFormat(operator, fieldType);
                List<String> delegationAcceptors = registry.delegationAcceptors(DelegationType.FILTER);
                for (String name : childViableBackends) {
                    if (anyFormat.contains(name)) {
                        fieldViable.add(name);
                    }
                }
                // Delegation targets reachable from child viable backends
                List<String> delegationSupporters = registry.delegationSupporters(DelegationType.FILTER);
                if (childViableBackends.stream().anyMatch(delegationSupporters::contains)) {
                    for (String name : anyFormat) {
                        if (!fieldViable.contains(name) && delegationAcceptors.contains(name)) {
                            fieldViable.add(name);
                        }
                    }
                }
            } else {
                // Format-aware: backends that can access the field's data
                for (String format : storageInfo.getDocValueFormats()) {
                    fieldViable.addAll(registry.filterBackends(operator, fieldType, format));
                }
                for (String format : storageInfo.getIndexFormats()) {
                    fieldViable.addAll(registry.filterBackends(operator, fieldType, format));
                }
                // Format-agnostic: delegation targets that can evaluate but don't need data access
                for (String name : registry.filterBackendsAnyFormat(operator, fieldType)) {
                    if (!fieldViable.contains(name)) {
                        fieldViable.add(name);
                    }
                }
            }

            viableSet.retainAll(fieldViable);
        }

        if (viableSet.isEmpty()) {
            throw new IllegalStateException("No backend can evaluate filter predicate ["
                + predicate.getKind() + "] on fields " + fieldIndices.stream()
                    .filter(i -> i < fieldStorage.size())
                    .map(i -> fieldStorage.get(i).getFieldName() + ":" + fieldStorage.get(i).getMappingType())
                    .toList());
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
    private List<String> computeFilterViableBackends(RexNode annotatedCondition,
                                                     List<String> childViableBackends) {
        List<AnnotatedPredicate> predicates = new ArrayList<>();
        collectAnnotatedPredicates(annotatedCondition, predicates);

        if (predicates.isEmpty()) {
            return new ArrayList<>(childViableBackends);
        }

        List<String> viable = new ArrayList<>();
        CapabilityRegistry registry = context.getCapabilityRegistry();
        List<String> delegationSupporters = registry.delegationSupporters(DelegationType.FILTER);
        List<String> delegationAcceptors = registry.delegationAcceptors(DelegationType.FILTER);

        for (String candidateName : childViableBackends) {
            if (!registry.operatorBackends(OperatorCapability.FILTER).contains(candidateName)) {
                continue;
            }

            boolean canHandleAll = true;
            for (AnnotatedPredicate predicate : predicates) {
                List<String> predViable = predicate.getViableBackends();
                if (predViable.contains(candidateName)) {
                    continue;
                }
                if (delegationSupporters.contains(candidateName)
                        && predViable.stream().anyMatch(delegationAcceptors::contains)) {
                    continue;
                }
                canHandleAll = false;
                break;
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
