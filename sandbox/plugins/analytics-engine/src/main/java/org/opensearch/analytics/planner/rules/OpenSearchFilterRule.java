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
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.AnnotatedPredicate;
import org.opensearch.analytics.planner.rel.FullTextFunctions;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.DelegationType;
import org.opensearch.analytics.spi.FieldTypeFamily;
import org.opensearch.analytics.spi.FilterCapability;
import org.opensearch.analytics.spi.FilterOperator;
import org.opensearch.analytics.spi.FullTextOperator;
import org.opensearch.analytics.spi.OperatorCapability;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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

        String childBackend = openSearchInput.getBackend();
        List<FieldStorageInfo> childFieldStorage = openSearchInput.getOutputFieldStorage();

        RelDataType inputRowType = child.getRowType();

        // Annotate every leaf predicate with viable backends
        RexNode annotatedCondition = annotateCondition(filter.getCondition(), inputRowType, childFieldStorage);

        // Compute operator-level viable backends from per-predicate annotations + delegation
        List<String> viableBackends = computeFilterViableBackends(annotatedCondition, childBackend);

        if (viableBackends.isEmpty()) {
            throw new IllegalStateException(
                "No backend can execute filter: child backend [" + childBackend
                    + "] cannot evaluate all predicates and no delegation path exists");
        }

        // Preferred backend: child if viable, otherwise first viable
        String filterBackend = viableBackends.contains(childBackend)
            ? childBackend
            : viableBackends.getFirst();

        call.transformTo(new OpenSearchFilter(
            filter.getCluster(),
            child.getTraitSet(),
            RelNodeUtils.unwrapHep(filter.getInput()),
            annotatedCondition,
            filterBackend,
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
                                      List<FieldStorageInfo> fieldStorage) {
        if (!(condition instanceof RexCall rexCall)) {
            return condition;
        }
        if (rexCall.getKind() == SqlKind.AND || rexCall.getKind() == SqlKind.OR || rexCall.getKind() == SqlKind.NOT) {
            List<RexNode> annotatedOperands = new ArrayList<>();
            for (RexNode operand : rexCall.getOperands()) {
                annotatedOperands.add(annotateCondition(operand, inputRowType, fieldStorage));
            }
            return rexCall.clone(rexCall.getType(), annotatedOperands);
        }
        List<String> viableBackends = resolveViableBackends(rexCall, inputRowType, fieldStorage);
        return new AnnotatedPredicate(rexCall.getType(), rexCall, viableBackends);
    }

    /**
     * Determines which backends can evaluate this leaf predicate.
     * Extracts all field references, looks up their {@link FieldStorageInfo} from the child,
     * checks backend format support, operator capability, and operator+fieldType support.
     * Intersects across all referenced fields.
     */
    private List<String> resolveViableBackends(RexCall predicate, RelDataType inputRowType,
                                               List<FieldStorageInfo> fieldStorage) {
        // TODO : Try collapsing in one pass
        Set<Integer> fieldIndices = new HashSet<>();
        collectFieldIndices(predicate, fieldIndices);
        if (fieldIndices.isEmpty()) {
            // No field refs (e.g. literal expression) — all backends with FILTER capability
            return context.getBackends().values().stream()
                .filter(backend -> backend.supportedOperators().contains(OperatorCapability.FILTER))
                .map(AnalyticsSearchBackendPlugin::name)
                .collect(Collectors.toList());
        }

        FilterOperator filterOp = FilterOperator.fromSqlKind(predicate.getKind());

        // Check if this is a full-text function (MATCH, MATCH_PHRASE, etc.)
        FullTextOperator fullTextOp = null;
        if (predicate.getOperator() instanceof SqlFunction sqlFunction) {
            fullTextOp = FullTextFunctions.toFullTextOperator(sqlFunction);
        }

        // Start with all backends that have FILTER capability, intersect per field
        Set<String> viableSet = new HashSet<>();
        for (AnalyticsSearchBackendPlugin backend : context.getBackends().values()) {
            if (backend.supportedOperators().contains(OperatorCapability.FILTER)) {
                viableSet.add(backend.name());
            }
        }

        for (int fieldIndex : fieldIndices) {
            if (fieldIndex >= fieldStorage.size()) {
                continue;
            }
            FieldStorageInfo storageInfo = fieldStorage.get(fieldIndex);

            // Derived/expression column — only backends that can filter on expressions
            if (storageInfo.isDerived()) {
                viableSet.retainAll(
                    context.getBackends().values().stream()
                        .filter(b -> b.supportedOperators().contains(OperatorCapability.FILTER_ON_EXPRESSIONS))
                        .map(AnalyticsSearchBackendPlugin::name)
                        .collect(Collectors.toSet())
                );
                continue;
            }
            FieldTypeFamily typeFamily = FieldTypeFamily.fromMappingType(storageInfo.getFieldType());
            final FullTextOperator finalFullTextOp = fullTextOp;

            Set<String> fieldViable = new HashSet<>();
            for (AnalyticsSearchBackendPlugin backend : context.getBackends().values()) {
                if (!viableSet.contains(backend.name())) {
                    continue;
                }

                boolean formatMatch = backend.getSupportedFormats().stream().anyMatch(format ->
                    storageInfo.getDocValueFormats().contains(format.name())
                        || storageInfo.getIndexFormats().contains(format.name())
                );

                // Full-text ops require the backend to support that specific operator
                // AND the field must have an index (full-text needs inverted index)
                boolean operatorMatch;
                if (finalFullTextOp != null) {
                    operatorMatch = backend.supportedFullTextOperators().contains(finalFullTextOp)
                        && storageInfo.hasIndex();
                } else if (filterOp != null && typeFamily != null) {
                    operatorMatch = backend.supportedFilterCapabilities()
                        .contains(FilterCapability.of(filterOp, typeFamily));
                } else {
                    // Unknown operator or unrecognized field type — accept if backend has FILTER
                    operatorMatch = true;
                }

                if (formatMatch && operatorMatch) {
                    fieldViable.add(backend.name());
                }
            }
            viableSet.retainAll(fieldViable);
        }

        if (viableSet.isEmpty()) {
            throw new IllegalStateException("No backend can evaluate filter predicate ["
                + predicate.getKind() + "] on fields " + fieldIndices.stream()
                    .filter(i -> i < fieldStorage.size())
                    .map(i -> fieldStorage.get(i).getFieldName() + ":" + fieldStorage.get(i).getFieldType())
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
    private List<String> computeFilterViableBackends(RexNode annotatedCondition, String childBackend) {
        List<AnnotatedPredicate> predicates = new ArrayList<>();
        collectAnnotatedPredicates(annotatedCondition, predicates);

        if (predicates.isEmpty()) {
            return new ArrayList<>(context.getBackends().keySet());
        }

        AnalyticsSearchBackendPlugin childPlugin = context.getBackends().get(childBackend);
        List<String> viable = new ArrayList<>();

        for (AnalyticsSearchBackendPlugin candidate : context.getBackends().values()) {
            if (!candidate.supportedOperators().contains(OperatorCapability.FILTER)) {
                continue;
            }

            boolean isChild = candidate.name().equals(childBackend);

            // Non-child backend: only viable if child can delegate entire filter to it
            // and it accepts delegation and can handle all predicates natively
            if (!isChild) {
                if (childPlugin == null
                    || !childPlugin.supportedDelegations().contains(DelegationType.FILTER)
                    || !candidate.acceptedDelegations().contains(DelegationType.FILTER)) {
                    continue;
                }
            }

            boolean canHandleAll = true;
            for (AnnotatedPredicate predicate : predicates) {
                List<String> predViable = predicate.getViableBackends();
                if (predViable.contains(candidate.name())) {
                    continue;
                }
                // Child backend can delegate individual predicates
                if (isChild && candidate.supportedDelegations().contains(DelegationType.FILTER)) {
                    boolean someoneAccepts = predViable.stream().anyMatch(backendName -> {
                        AnalyticsSearchBackendPlugin other = context.getBackends().get(backendName);
                        return other != null && other.acceptedDelegations().contains(DelegationType.FILTER);
                    });
                    if (someoneAccepts) {
                        continue;
                    }
                }
                canHandleAll = false;
                break;
            }
            if (canHandleAll) {
                viable.add(candidate.name());
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
