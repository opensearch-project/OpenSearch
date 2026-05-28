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
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.AnnotatedPredicate;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.settings.DelegationBlockList;
import org.opensearch.analytics.spi.DelegationType;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.analytics.spi.ScalarFunction;

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
                "Filter rule encountered unmarked child ["
                    + child.getClass().getSimpleName()
                    + "]. Ensure all child operators are marked before filter."
            );
        }

        List<String> childViableBackends = openSearchInput.getViableBackends();
        List<FieldStorageInfo> childFieldStorage = openSearchInput.getOutputFieldStorage();

        // Annotate every leaf predicate with viable backends.
        RexNode annotatedCondition = annotateCondition(filter.getCondition(), childFieldStorage, childViableBackends);

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
        PredicateContents contents = new PredicateContents(new HashSet<>(), new ArrayList<>());
        for (RexNode operand : predicate.getOperands()) {
            collect(operand, contents);
        }
        Set<Integer> fieldIndices = contents.fieldIndices();

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
            // Extract the literal field names and resolve viability per-field using the actual
            // FieldStorageInfo lookup — same code path as RexInputRef-based fields. This ensures
            // that, e.g., query_string(['severityNumber'], ...) on an INTEGER field doesn't get
            // routed to a backend that only declared (QUERY_STRING, TEXT) capability.
            if (function.getCategory() == ScalarFunction.Category.FULL_TEXT) {
                List<String> literalFieldNames = extractLiteralFieldNames(predicate);
                if (literalFieldNames.isEmpty()) {
                    // No field references at all — fall back to TEXT type assumption.
                    // This covers zero-field full-text functions like MATCHALL or QUERY (no-field variant).
                    return new ArrayList<>(registry.filterBackendsAnyFormat(function, FieldType.TEXT));
                }
                // Eagerly reject text-relevance functions invoked on non-text/non-keyword fields.
                // The downstream capability intersection would also exclude these, but a generic
                // "no backend can evaluate" error is unhelpful — surface a precise, actionable
                // message naming the offending field and its type so users know to use the
                // typed comparison operator (e.g. {@code field > 15}) instead.
                rejectNonTextFieldsForTextFunction(predicate.getOperator().getName(), literalFieldNames, fieldStorageInfos);
                Set<String> viableSet = new HashSet<>(registry.filterCapableBackends());
                for (String fieldName : literalFieldNames) {
                    FieldStorageInfo storageInfo = findStorageByFieldName(fieldStorageInfos, fieldName);
                    if (storageInfo == null) {
                        // Unknown field — fall back to TEXT type assumption for this field.
                        viableSet.retainAll(registry.filterBackendsAnyFormat(function, FieldType.TEXT));
                    } else {
                        viableSet.retainAll(registry.filterBackendsForField(function, storageInfo));
                    }
                }
                if (viableSet.isEmpty()) {
                    throw new IllegalStateException(
                        "No backend can evaluate filter predicate ["
                            + predicate.getKind()
                            + "] on literal-named fields "
                            + literalFieldNames
                    );
                }
                return new ArrayList<>(viableSet);
            }
            // No field reference (non-deterministic, or an unfoldable constant like
            // mktime('...') > N): let any child-viable backend evaluate it.
            return new ArrayList<>(childViableBackends);
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

        // Every nested scalar function in the predicate must also be evaluable by a candidate backend
        for (RexCall scalarFunctionCall : contents.scalarFunctionCalls()) {
            // Calcite-internal value constructors (named-parameter MAP/ARRAY/ROW used by full-text
            // operators like match() to pass `field`, `query`, etc.) aren't real scalar functions
            // they're parameter-passing scaffolding. Skip them
            SqlKind kind = scalarFunctionCall.getKind();
            if (kind == SqlKind.MAP_VALUE_CONSTRUCTOR || kind == SqlKind.ARRAY_VALUE_CONSTRUCTOR || kind == SqlKind.ROW) {
                continue;
            }
            ScalarFunction scalarFunc = ScalarFunction.fromSqlOperatorWithFallback(scalarFunctionCall.getOperator());
            if (scalarFunc == null) {
                throw new IllegalStateException(
                    "Unrecognized scalar function ["
                        + scalarFunctionCall.getOperator().getName()
                        + "] in call ["
                        + scalarFunctionCall
                        + "] within filter predicate ["
                        + predicate
                        + "]"
                );
            }
            FieldType returnType = FieldType.fromSqlTypeName(scalarFunctionCall.getType().getSqlTypeName());
            // Polymorphic UDF fallback (e.g. SCALAR_MAX/MIN return SqlTypeName.ANY): infer
            // FieldType from the first concrete operand. Backend capabilities for these UDFs
            // are declared over operand types, so this preserves correct dispatch — see
            // OpenSearchProjectRule.resolveScalarViableBackends for the parallel fallback.
            if (returnType == null) {
                for (RexNode operand : scalarFunctionCall.getOperands()) {
                    FieldType operandType = FieldType.fromSqlTypeName(operand.getType().getSqlTypeName());
                    if (operandType != null) {
                        returnType = operandType;
                        break;
                    }
                }
                if (returnType == null) {
                    throw new IllegalStateException(
                        "Unmapped return type ["
                            + scalarFunctionCall.getType().getSqlTypeName()
                            + "] for scalar function ["
                            + scalarFunc
                            + "] in call ["
                            + scalarFunctionCall
                            + "] within filter predicate ["
                            + predicate
                            + "]"
                    );
                }
            }
            viableSet.retainAll(registry.scalarBackendsAnyFormat(scalarFunc, returnType));
        }

        // Per-backend delegation block-list. Drop backends that block this predicate so it is never
        // marked viable for them — but only when a non-blocked backend survives: blocking is a
        // delegation knob and must never make a predicate unexecutable.
        DelegationBlockList blockList = context.getDelegationBlockList();
        if (!blockList.isEmpty()) {
            boolean someBackendSurvives = viableSet.stream().anyMatch(backend -> !blockList.isBlocked(backend, function));
            if (someBackendSurvives) {
                viableSet.removeIf(backend -> blockList.isBlocked(backend, function));
            }
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

    /**
     * Result of a single walk over a predicate's operand subtree.
     *
     * <p>{@code fieldIndices} — RexInputRef indices feeding the field-storage intersection.
     * <p>{@code scalarFunctionCalls} — nested RexCalls feeding the scalar-function capability intersection.
     *
     * <p>TODO: ensure that the code for tagging and checking the scalar function of a predicate
     * remains the same as the code for tagging and checking its nested inner expressions as much
     * as possible.
     */
    private record PredicateContents(Set<Integer> fieldIndices, List<RexCall> scalarFunctionCalls) {
    }

    /** Recurses the operand subtree, populating {@code contents} in-place. */
    private void collect(RexNode node, PredicateContents contents) {
        if (node instanceof RexInputRef inputRef) {
            contents.fieldIndices().add(inputRef.getIndex());
        } else if (node instanceof RexCall rexCall) {
            contents.scalarFunctionCalls().add(rexCall);
            for (RexNode operand : rexCall.getOperands()) {
                collect(operand, contents);
            }
        }
    }

    /**
     * Extracts field names from the literal MAP structure of multi-field full-text RexCalls.
     *
     * <p>Multi-field functions like {@code query_string(['severityNumber'], 'severityNumber:>15')}
     * encode field names as string literals in a nested MAP_VALUE_CONSTRUCTOR rather than as
     * {@link RexInputRef}. The structure is:
     * <pre>
     *   func(
     *     MAP('fields', MAP('field1':VARCHAR, boost1:DOUBLE, 'field2':VARCHAR, boost2:DOUBLE, ...)),
     *     MAP('query', '...':VARCHAR)
     *   )
     * </pre>
     * This method walks the call tree to find all string literals that represent field names —
     * specifically, RexLiteral children at even indices of a nested MAP whose outer key is "fields"
     * or "field". Returns an empty list if no such names are found (e.g. zero-field functions
     * like MATCHALL or QUERY without an explicit field list).
     */
    private List<String> extractLiteralFieldNames(RexCall predicate) {
        List<String> names = new ArrayList<>();
        for (RexNode operand : predicate.getOperands()) {
            if (operand instanceof RexCall outerMap && outerMap.getOperands().size() >= 2) {
                // Outer MAP: MAP('key', value). Check if the key is "fields" or "field".
                RexNode keyNode = outerMap.getOperands().get(0);
                if (!(keyNode instanceof RexLiteral keyLit)) continue;
                String key = keyLit.getValueAs(String.class);
                if (!"fields".equals(key) && !"field".equals(key)) continue;

                RexNode valueNode = outerMap.getOperands().get(1);
                if (valueNode instanceof RexCall nestedMap) {
                    // Nested MAP with field-name/boost pairs at even/odd indices.
                    List<RexNode> nestedOperands = nestedMap.getOperands();
                    for (int i = 0; i < nestedOperands.size(); i += 2) {
                        RexNode fieldNode = nestedOperands.get(i);
                        if (fieldNode instanceof RexLiteral fieldLit) {
                            String fieldName = fieldLit.getValueAs(String.class);
                            if (fieldName != null && !fieldName.isEmpty()) {
                                names.add(fieldName);
                            }
                        }
                    }
                } else if (valueNode instanceof RexLiteral valueLit) {
                    // Single-field shape: MAP('field', 'fieldName').
                    String fieldName = valueLit.getValueAs(String.class);
                    if (fieldName != null && !fieldName.isEmpty()) {
                        names.add(fieldName);
                    }
                }
            }
        }
        return names;
    }

    /**
     * Looks up a {@link FieldStorageInfo} by field name. Returns null if no match.
     */
    private FieldStorageInfo findStorageByFieldName(List<FieldStorageInfo> fieldStorageInfos, String fieldName) {
        for (FieldStorageInfo info : fieldStorageInfos) {
            if (fieldName.equals(info.getFieldName())) {
                return info;
            }
        }
        return null;
    }

    /**
     * Validates that all explicitly named fields used in a text-relevance function
     * (e.g. {@code query_string}, {@code simple_query_string}, {@code multi_match})
     * are mapped as {@code text} or {@code keyword}. Throws a descriptive
     * {@link IllegalArgumentException} naming the offending field, its mapping type,
     * and the function — surfaced at planning so users get a clear, actionable error
     * instead of a generic "no backend can evaluate" failure later in the pipeline.
     *
     * <p>Fields whose storage info cannot be resolved (unknown columns, dynamic
     * mappings) are skipped — they fall through to the existing capability-based
     * matching where they get the conservative TEXT-type assumption.
     */
    private void rejectNonTextFieldsForTextFunction(
        String functionName,
        List<String> fieldNames,
        List<FieldStorageInfo> fieldStorageInfos
    ) {
        Set<FieldType> allowed = new HashSet<>();
        allowed.addAll(FieldType.text());
        allowed.addAll(FieldType.keyword());
        for (String fieldName : fieldNames) {
            FieldStorageInfo storageInfo = findStorageByFieldName(fieldStorageInfos, fieldName);
            if (storageInfo == null) {
                continue;
            }
            FieldType type = storageInfo.getFieldType();
            if (type != null && allowed.contains(type) == false) {
                throw new IllegalArgumentException(
                    "Text-relevance function ["
                        + functionName
                        + "] cannot be applied to field ["
                        + fieldName
                        + "] of type ["
                        + storageInfo.getMappingType()
                        + "]. Only text and keyword fields are supported. "
                        + "Use a typed comparison (e.g. ["
                        + fieldName
                        + " > value]) for non-text fields."
                );
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
