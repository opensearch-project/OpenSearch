/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to this file be licensed under
 * the Apache-2.0 license or a compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.spi.ArrowBatchSourceFactory.ColumnKind;
import org.opensearch.analytics.spi.ArrowBatchSourceFactory.InputColumn;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.FieldType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

/** Classifies one Lucene fragment as the count path, Arrow source path, or unsupported. */
final class LuceneFragmentPlanner {

    private static final String SOURCE_INPUT_ID = "input-0";

    private LuceneFragmentPlanner() {}

    sealed interface Shape permits CountShape, ArrowSourceShape, UnsupportedShape {
        List<String> outputNames();

        Filter filter();
    }

    record CountShape(List<String> outputNames, Filter filter) implements Shape {
    }

    record UnsupportedShape(List<String> outputNames, Filter filter) implements Shape {
    }

    static Shape classify(RelNode fragment) {
        if (isCountFastPath(fragment)) {
            return new CountShape(aggregateOutputNames(fragment), findFilter(fragment));
        }
        ArrowSourceShape arrowSource = extractArrowSourceShape(fragment);
        if (arrowSource != null) {
            return arrowSource;
        }
        return new UnsupportedShape(aggregateOutputNames(fragment), findFilter(fragment));
    }

    private static List<String> aggregateOutputNames(RelNode root) {
        RelNode current = root;
        while (current != null) {
            if (current instanceof Aggregate aggregate) {
                List<String> names = new ArrayList<>(aggregate.getAggCallList().size());
                for (int i = 0; i < aggregate.getAggCallList().size(); i++) {
                    String name = aggregate.getAggCallList().get(i).getName();
                    names.add(name == null ? "EXPR$" + i : name);
                }
                return names;
            }
            current = current.getInputs().isEmpty() ? null : current.getInputs().getFirst();
        }
        return List.of();
    }

    private static Filter findFilter(RelNode node) {
        RelNode current = node;
        while (current != null) {
            if (current instanceof Filter filter) {
                return filter;
            }
            current = current.getInputs().isEmpty() ? null : current.getInputs().getFirst();
        }
        return null;
    }

    /**
     * True iff the top is an {@link Aggregate} with empty group-set whose every call is
     * {@link SqlKind#COUNT} — what {@code IndexSearcher.count} can answer from the term
     * dictionary. Read by {@link LuceneShardPreference} to score this fragment.
     *
     * <p>Defense-in-depth: PlanForker's chain-agreement filter already narrows aggregate
     * alternatives to declared capabilities (prod Lucene declares only COUNT), so this
     * guards against capability-declaration drift.
     */
    static boolean isCountFastPath(RelNode fragment) {
        if (fragment instanceof Aggregate == false) return false;
        Aggregate agg = (Aggregate) fragment;
        if (agg.getGroupSet().isEmpty() == false || agg.getAggCallList().isEmpty()) return false;
        for (AggregateCall call : agg.getAggCallList()) {
            if (call.getAggregation().getKind() != SqlKind.COUNT || call.getArgList().isEmpty() == false) return false;
        }
        return true;
    }

    private static InputColumn docValuesColumn(List<FieldStorageInfo> storage, int ordinal) {
        if (ordinal < 0 || ordinal >= storage.size()) {
            return null;
        }
        FieldStorageInfo info = storage.get(ordinal);
        if (info.isDerived()) {
            return null;
        }
        List<String> docValueFormats = info.getDocValueFormats();
        if (docValueFormats == null || docValueFormats.contains(LuceneDataFormat.LUCENE_FORMAT_NAME) == false) {
            return null;
        }
        if (info.getFieldType() == FieldType.DATE) {
            return new InputColumn(info.getFieldName(), ColumnKind.TIMESTAMP);
        }
        if (info.getFieldType() == FieldType.LONG) {
            return new InputColumn(info.getFieldName(), ColumnKind.LONG);
        }
        if (info.getFieldType() == FieldType.KEYWORD) {
            return new InputColumn(info.getFieldName(), ColumnKind.KEYWORD);
        }
        return null;
    }

    /** A supported fragment rebased onto the named Arrow source table. */
    record ArrowSourceShape(String inputId, RelNode rebasedFragment, List<InputColumn> inputColumns, Filter filter, List<
        String> outputNames) implements Shape {
    }

    /**
     * Extracts aggregate and row-returning unary fragments that can read all required input
     * values from single-valued Lucene doc values.
     */
    private static ArrowSourceShape extractArrowSourceShape(RelNode fragment) {
        RelNode originalFragment = fragment;
        List<RelNode> wrappers = new ArrayList<>();
        while (fragment instanceof Aggregate == false) {
            if (fragment.getInputs().size() != 1
                || (fragment instanceof Project == false
                    && fragment instanceof Filter == false
                    && fragment instanceof org.apache.calcite.rel.core.Sort == false)) {
                return extractRowArrowSourceShape(originalFragment);
            }
            wrappers.add(fragment);
            fragment = fragment.getInput(0);
        }

        Aggregate aggregate = (Aggregate) fragment;
        if (aggregate.getGroupSets() != null && aggregate.getGroupSets().size() > 1) {
            return null;
        }
        RelNode below = aggregate.getInput();
        Project project = null;
        if (below instanceof Project candidate) {
            project = candidate;
            below = candidate.getInput();
        }
        Filter filter = null;
        if (below instanceof Filter candidate) {
            filter = candidate;
            below = candidate.getInput();
        }
        if (below instanceof OpenSearchRelNode == false || below.getInputs().isEmpty() == false) {
            return null;
        }
        List<FieldStorageInfo> storage = ((OpenSearchRelNode) below).getOutputFieldStorage();
        if (storage == null) {
            return null;
        }

        TreeSet<Integer> referenced = new TreeSet<>();
        if (project != null) {
            RexShuttle collector = inputReferenceCollector(referenced);
            for (RexNode expression : project.getProjects()) {
                expression.accept(collector);
            }
        } else {
            for (int ordinal : aggregate.getGroupSet()) {
                referenced.add(ordinal);
            }
            for (AggregateCall call : aggregate.getAggCallList()) {
                referenced.addAll(call.getArgList());
                if (call.filterArg >= 0) {
                    return null;
                }
            }
        }
        // Preserve the metadata count fast path for COUNT(*).
        if (referenced.isEmpty()) {
            return null;
        }

        RebasedInput input = rebaseInput(aggregate, below, storage, referenced);
        if (input == null) {
            return null;
        }
        RexShuttle remap = inputRemapper(input.oldToNew());

        RelNode rebasedInput;
        Aggregate rebasedAggregate;
        if (project != null) {
            List<RexNode> remapped = new ArrayList<>(project.getProjects().size());
            for (RexNode expression : project.getProjects()) {
                remapped.add(expression.accept(remap));
            }
            rebasedInput = LogicalProject.create(input.scan(), project.getHints(), remapped, project.getRowType().getFieldNames());
            rebasedAggregate = aggregate.copy(
                aggregate.getTraitSet(),
                rebasedInput,
                aggregate.getGroupSet(),
                aggregate.getGroupSets(),
                aggregate.getAggCallList()
            );
        } else {
            ImmutableBitSet.Builder newGroupSet = ImmutableBitSet.builder();
            for (int ordinal : aggregate.getGroupSet()) {
                newGroupSet.set(input.oldToNew()[ordinal]);
            }
            List<AggregateCall> remappedCalls = new ArrayList<>(aggregate.getAggCallList().size());
            for (AggregateCall call : aggregate.getAggCallList()) {
                List<Integer> remappedArguments = new ArrayList<>(call.getArgList().size());
                for (int argument : call.getArgList()) {
                    remappedArguments.add(input.oldToNew()[argument]);
                }
                remappedCalls.add(call.copy(remappedArguments, -1, call.collation));
            }
            rebasedAggregate = aggregate.copy(aggregate.getTraitSet(), input.scan(), newGroupSet.build(), null, remappedCalls);
        }

        RelNode rebased = rebasedAggregate;
        for (int i = wrappers.size() - 1; i >= 0; i--) {
            RelNode wrapper = wrappers.get(i);
            rebased = wrapper.copy(wrapper.getTraitSet(), List.of(rebased));
        }
        return new ArrowSourceShape(SOURCE_INPUT_ID, rebased, input.columns(), filter, resultNames(originalFragment));
    }

    private static ArrowSourceShape extractRowArrowSourceShape(RelNode fragment) {
        Project topProject = null;
        RelNode node = fragment;
        if (node instanceof Project candidate) {
            topProject = candidate;
            node = candidate.getInput();
        }
        org.apache.calcite.rel.core.Sort sort = null;
        if (node instanceof org.apache.calcite.rel.core.Sort candidate) {
            if (candidate.offset != null) {
                return null;
            }
            sort = candidate;
            node = candidate.getInput();
        }
        Project middleProject = null;
        if (node instanceof Project candidate) {
            middleProject = candidate;
            node = candidate.getInput();
        }
        Filter filter = null;
        if (node instanceof Filter candidate) {
            filter = candidate;
            node = candidate.getInput();
        }
        if (node instanceof OpenSearchRelNode == false || node.getInputs().isEmpty() == false) {
            return null;
        }
        if (topProject == null && sort == null && middleProject == null) {
            return null;
        }
        List<FieldStorageInfo> storage = ((OpenSearchRelNode) node).getOutputFieldStorage();
        if (storage == null) {
            return null;
        }

        TreeSet<Integer> referenced = new TreeSet<>();
        RexShuttle collector = inputReferenceCollector(referenced);
        if (middleProject != null) {
            for (RexNode expression : middleProject.getProjects()) {
                expression.accept(collector);
            }
        } else if (topProject != null) {
            for (RexNode expression : topProject.getProjects()) {
                expression.accept(collector);
            }
            if (sort != null) {
                for (org.apache.calcite.rel.RelFieldCollation field : sort.getCollation().getFieldCollations()) {
                    referenced.add(field.getFieldIndex());
                }
            }
        } else {
            for (int i = 0; i < storage.size(); i++) {
                referenced.add(i);
            }
        }

        // A constant projection still needs one source column to preserve input row count.
        if (referenced.isEmpty()) {
            TreeSet<Integer> filterReferences = new TreeSet<>();
            if (filter != null) {
                filter.getCondition().accept(inputReferenceCollector(filterReferences));
            }
            for (int ordinal : filterReferences) {
                if (docValuesColumn(storage, ordinal) != null) {
                    referenced.add(ordinal);
                    break;
                }
            }
            if (referenced.isEmpty()) {
                for (int i = 0; i < storage.size(); i++) {
                    if (docValuesColumn(storage, i) != null) {
                        referenced.add(i);
                        break;
                    }
                }
            }
            if (referenced.isEmpty()) {
                return null;
            }
        }

        RebasedInput input = rebaseInput(fragment, node, storage, referenced);
        if (input == null) {
            return null;
        }
        RexShuttle remap = inputRemapper(input.oldToNew());
        RelNode rebased = input.scan();
        if (middleProject != null) {
            List<RexNode> expressions = new ArrayList<>(middleProject.getProjects().size());
            for (RexNode expression : middleProject.getProjects()) {
                expressions.add(expression.accept(remap));
            }
            rebased = LogicalProject.create(rebased, middleProject.getHints(), expressions, middleProject.getRowType().getFieldNames());
        }
        if (sort != null) {
            org.apache.calcite.rel.RelCollation collation = sort.getCollation();
            if (middleProject == null) {
                List<org.apache.calcite.rel.RelFieldCollation> fields = new ArrayList<>();
                for (org.apache.calcite.rel.RelFieldCollation field : collation.getFieldCollations()) {
                    fields.add(field.withFieldIndex(input.oldToNew()[field.getFieldIndex()]));
                }
                collation = org.apache.calcite.rel.RelCollations.of(fields);
            }
            rebased = org.apache.calcite.rel.logical.LogicalSort.create(rebased, collation, null, sort.fetch);
        }
        if (topProject != null) {
            List<RexNode> expressions = new ArrayList<>(topProject.getProjects().size());
            for (RexNode expression : topProject.getProjects()) {
                expressions.add(middleProject == null ? expression.accept(remap) : expression);
            }
            rebased = LogicalProject.create(rebased, topProject.getHints(), expressions, topProject.getRowType().getFieldNames());
        }
        return new ArrowSourceShape(SOURCE_INPUT_ID, rebased, input.columns(), filter, resultNames(fragment));
    }

    private static RebasedInput rebaseInput(
        RelNode owner,
        RelNode originalScan,
        List<FieldStorageInfo> storage,
        TreeSet<Integer> referenced
    ) {
        List<InputColumn> columns = new ArrayList<>(referenced.size());
        int[] oldToNew = new int[storage.size()];
        Arrays.fill(oldToNew, -1);
        RelDataTypeFactory.Builder rowBuilder = owner.getCluster().getTypeFactory().builder();
        List<FieldStorageInfo> rebasedStorage = new ArrayList<>(referenced.size());
        for (int ordinal : referenced) {
            InputColumn column = docValuesColumn(storage, ordinal);
            if (column == null || ordinal >= originalScan.getRowType().getFieldCount()) {
                return null;
            }
            oldToNew[ordinal] = columns.size();
            columns.add(column);
            RelDataTypeField field = originalScan.getRowType().getFieldList().get(ordinal);
            rowBuilder.add(column.name(), field.getType());
            rebasedStorage.add(storage.get(ordinal));
        }
        OpenSearchStageInputScan scan = new OpenSearchStageInputScan(
            owner.getCluster(),
            owner.getTraitSet(),
            0,
            rowBuilder.build(),
            List.of(),
            rebasedStorage
        );
        return new RebasedInput(scan, columns, oldToNew);
    }

    private static RexShuttle inputReferenceCollector(TreeSet<Integer> references) {
        return new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef ref) {
                references.add(ref.getIndex());
                return ref;
            }
        };
    }

    private static RexShuttle inputRemapper(int[] oldToNew) {
        return new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef ref) {
                int mapped = oldToNew[ref.getIndex()];
                if (mapped < 0) {
                    throw new IllegalStateException("unreferenced input ordinal [" + ref.getIndex() + "] survived source-plan rebasing");
                }
                return new RexInputRef(mapped, ref.getType());
            }
        };
    }

    static List<String> resultNames(RelNode fragment) {
        List<String> names = fragment.getRowType().getFieldNames();
        List<String> result = new ArrayList<>(names.size());
        for (int i = 0; i < names.size(); i++) {
            result.add(names.get(i) == null ? "EXPR$" + i : names.get(i));
        }
        return result;
    }

    private record RebasedInput(OpenSearchStageInputScan scan, List<InputColumn> columns, int[] oldToNew) {
    }
}
