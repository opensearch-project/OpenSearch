/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.rel.AnnotatedPredicate;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.spi.ArrowBatchSourceExecutorHolder;
import org.opensearch.analytics.spi.ArrowBatchSourceFactory.ColumnKind;
import org.opensearch.analytics.spi.ArrowBatchSourceFactory.InputColumn;
import org.opensearch.analytics.spi.DelegatedPredicateSerializer;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.analytics.spi.FragmentConvertor;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.analytics.spi.WireFormat;
import org.opensearch.be.lucene.serializers.AbstractQuerySerializer;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import io.substrait.proto.NamedStruct;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelRoot;
import io.substrait.proto.Type;

/**
 * Lucene-as-driver {@link FragmentConvertor}. Walks the resolved fragment, finds the
 * {@link OpenSearchFilter}, and serializes its condition as a {@link BoolQueryBuilder}'s
 * NamedWriteable bytes. Empty bytes when the fragment has no filter ({@code count(*)} over
 * MatchAllDocs at the data node).
 *
 * <p>Reuses the same leaf-serializer registry as {@link LuceneSubtreeConvertor} via
 * {@link QuerySerializerRegistry} — keyword equality, MATCH, MATCH_PHRASE, etc. all
 * round-trip through the same {@link DelegatedPredicateSerializer} → {@link QueryBuilder}
 * path. The data-node Lucene driver deserializes the bytes via NamedWriteable and runs
 * {@code IndexSearcher.count} on the resulting {@link QueryBuilder#toQuery(QueryShardContext)}.
 *
 * <p>Multi-stage / non-shard-scan fragments aren't supported: Lucene drives shard-local
 * count fragments only. Reduce or coordinator stages still run on DataFusion, so this
 * convertor is invoked only when the planner picked Lucene as the StagePlan's backend —
 * which happens exclusively for count-fast-path-eligible shards today.
 *
 * @opensearch.internal
 */
final class LuceneFragmentConvertor implements FragmentConvertor {

    private static final Logger LOGGER = LogManager.getLogger(LuceneFragmentConvertor.class);

    private final Map<ScalarFunction, DelegatedPredicateSerializer> leafSerializers;

    LuceneFragmentConvertor(Map<ScalarFunction, DelegatedPredicateSerializer> leafSerializers) {
        this.leafSerializers = leafSerializers;
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

    static final String ARROW_SOURCE_PLAN_MARKER = " arrow-source-plan ";
    private static final String SOURCE_INPUT_ID = "input-0";

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
    record ArrowSourceShape(RelNode rebasedFragment, List<InputColumn> inputColumns, Filter filter, List<String> outputNames) {
    }

    /**
     * Extracts aggregate and row-returning unary fragments that can read all required input
     * values from single-valued Lucene doc values.
     */
    static ArrowSourceShape extractArrowSourceShape(RelNode fragment) {
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
        return new ArrowSourceShape(rebased, input.columns(), filter, outputNames(originalFragment));
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
        return new ArrowSourceShape(rebased, input.columns(), filter, outputNames(fragment));
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

    private static List<String> outputNames(RelNode fragment) {
        List<String> names = fragment.getRowType().getFieldNames();
        List<String> result = new ArrayList<>(names.size());
        for (int i = 0; i < names.size(); i++) {
            result.add(names.get(i) == null ? "EXPR$" + i : names.get(i));
        }
        return result;
    }

    private record RebasedInput(OpenSearchStageInputScan scan, List<InputColumn> columns, int[] oldToNew) {
    }

    @Override
    public byte[] convertFragment(RelNode fragment) {
        ArrowSourceShape shape = extractArrowSourceShape(fragment);
        if (shape != null) {
            if (ArrowBatchSourceExecutorHolder.isAvailable() == false) {
                throw new IllegalStateException("Arrow batch source executor became unavailable during Lucene fragment conversion");
            }
            return convertArrowSourceShape(shape, false);
        }

        // Lucene-driver wire format: [columnNames StringCollection] [hasFilter boolean]
        // [QueryBuilder NamedWriteable]?. Both ends are controlled (this convertor on the
        // coordinator, LuceneScanInstructionHandler on the data node), so a tiny custom
        // format is fine — beats threading column names through the InstructionNode.
        // columnNames may be empty when the convertor runs against a non-count Lucene
        // alternative kept around for delegation (e.g. DF drives, Lucene is the peer); the
        // bytes are produced but the data node never invokes them — selector or runtime
        // alternative-selection drops this plan before dispatch.
        List<String> columnNames = extractAggCallNames(fragment);
        QueryBuilder filterQuery = null;
        Filter filter = findFilter(fragment);
        if (filter != null) {
            // strip() in FragmentConversionDriver replaces OpenSearchFilter with a plain
            // LogicalFilter, so the field-storage info lives on the OpenSearch ancestor
            // below (the TableScan). Walk down past LogicalFilter to find the nearest
            // OpenSearchRelNode and use its output field storage. The condition itself was
            // already resolved (annotation placeholders unwrapped) by the resolver in strip().
            List<FieldStorageInfo> fieldStorage = findFieldStorage(filter);
            filterQuery = toQueryBuilder(filter.getCondition(), fieldStorage);
        }
        byte[] bytes;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeStringCollection(columnNames);
            if (filterQuery == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeNamedWriteable(filterQuery);
            }
            bytes = BytesReference.toBytes(out.bytes());
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialize Lucene-driver fragment", e);
        }
        LOGGER.debug("[lucene-count] convertFragment columnNames={} filterQuery={} bytes={}", columnNames, filterQuery, bytes.length);
        return bytes;
    }

    private byte[] convertArrowSourceShape(ArrowSourceShape shape, boolean partialAggregate) {
        byte[] planBytes = ArrowBatchSourceExecutorHolder.get().compile(shape.rebasedFragment(), partialAggregate);
        List<String> encoded = new ArrayList<>();
        encoded.add(ARROW_SOURCE_PLAN_MARKER);
        encoded.add(java.util.Base64.getEncoder().encodeToString(planBytes));
        encoded.add(SOURCE_INPUT_ID);
        encoded.add(Integer.toString(shape.inputColumns().size()));
        for (InputColumn column : shape.inputColumns()) {
            encoded.add(column.name());
            encoded.add(column.kind().name());
        }
        encoded.add(Integer.toString(shape.outputNames().size()));
        encoded.addAll(shape.outputNames());

        QueryBuilder filterQuery = null;
        if (shape.filter() != null) {
            filterQuery = toQueryBuilder(shape.filter().getCondition(), findFieldStorage(shape.filter()));
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeStringCollection(encoded);
            out.writeBoolean(filterQuery != null);
            if (filterQuery != null) {
                out.writeNamedWriteable(filterQuery);
            }
            byte[] bytes = BytesReference.toBytes(out.bytes());
            LOGGER.debug(
                "[lucene-arrow-source] inputColumns={} outputNames={} plan={}B filter={} bytes={}",
                shape.inputColumns(),
                shape.outputNames(),
                planBytes.length,
                filterQuery,
                bytes.length
            );
            return bytes;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialize Lucene Arrow source fragment", e);
        }
    }

    /**
     * Walks down to find an Aggregate (Calcite {@link Aggregate} or {@code OpenSearchAggregate})
     * and extracts the user-facing call names. These become the Arrow output column names so
     * the coordinator's reduce sink sees the schema it expects.
     */
    private static List<String> extractAggCallNames(RelNode root) {
        RelNode current = root;
        while (current != null) {
            if (current instanceof Aggregate agg) {
                List<String> names = new ArrayList<>(agg.getAggCallList().size());
                for (int i = 0; i < agg.getAggCallList().size(); i++) {
                    names.add(outputName(agg.getAggCallList().get(i).getName(), i));
                }
                return names;
            }
            if (current.getInputs().isEmpty()) break;
            current = current.getInputs().getFirst();
        }
        return List.of();
    }

    @Override
    public byte[] attachPartialAggOnTop(RelNode partialAggFragment, byte[] innerBytes) {
        // Lucene-as-driver count fragments DO go through the partial-agg split — the driver's
        // FragmentConversionDriver invokes convertFragment on the input subtree (the
        // TableScan / Filter, no Aggregate above), then attachPartialAggOnTop on the
        // OpenSearchAggregate node. Without this rewrite, innerBytes carries an empty
        // columnNames list (extractAggCallNames found no Aggregate in the input) and the
        // data-node Lucene exec engine emits a 0-column Arrow batch — the coordinator
        // reduce sink then stalls waiting for the count column.
        //
        // Strategy: re-decode innerBytes' columnNames length-prefix (always present, possibly
        // empty), then preserve the remaining tail (hasFilter + optional QueryBuilder)
        // verbatim. Re-emit with the partialAggFragment's aggregate-call names as the new
        // columnNames. Avoids needing a NamedWriteableRegistry at coordinator-side conversion.
        if (!(partialAggFragment instanceof Aggregate agg)) {
            throw new IllegalStateException(
                "Lucene attachPartialAggOnTop expected an Aggregate fragment, got " + partialAggFragment.getClass().getSimpleName()
            );
        }
        ArrowSourceShape shape = extractArrowSourceShape(agg);
        if (shape != null) {
            if (ArrowBatchSourceExecutorHolder.isAvailable() == false) {
                throw new IllegalStateException("Arrow batch source executor became unavailable during partial aggregate conversion");
            }
            return convertArrowSourceShape(shape, true);
        }

        List<String> columnNames = new ArrayList<>(agg.getAggCallList().size());
        for (int i = 0; i < agg.getAggCallList().size(); i++) {
            columnNames.add(outputName(agg.getAggCallList().get(i).getName(), i));
        }

        // Read past the inner columnNames StringCollection to get the byte offset of the
        // hasFilter + optional QueryBuilder tail. We then copy the tail verbatim into the new
        // bytes prefixed by the aggregate's column names.
        int tailOffset;
        try (StreamInput in = StreamInput.wrap(innerBytes)) {
            in.readStringList(); // discard inner columnNames; we'll write the agg names instead
            tailOffset = innerBytes.length - in.available();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to decode Lucene innerBytes during partial-agg attach", e);
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeStringCollection(columnNames);
            out.writeBytes(innerBytes, tailOffset, innerBytes.length - tailOffset);
            byte[] bytes = BytesReference.toBytes(out.bytes());
            LOGGER.debug("[lucene-count] attachPartialAggOnTop columnNames={} bytes={}", columnNames, bytes.length);
            return bytes;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialize Lucene-driver partial-agg bytes", e);
        }
    }

    @Override
    public byte[] attachFragmentOnTop(RelNode fragment, byte[] innerBytes) {
        List<String> metadata;
        int tailOffset;
        try (StreamInput input = StreamInput.wrap(innerBytes)) {
            metadata = input.readStringList();
            tailOffset = innerBytes.length - input.available();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to decode Lucene Arrow source plan during fragment attachment", e);
        }
        if (metadata.isEmpty() || ARROW_SOURCE_PLAN_MARKER.equals(metadata.getFirst()) == false) {
            throw new UnsupportedOperationException("Cannot attach a fragment to the Lucene count wire format");
        }

        int position = 1;
        byte[] innerPlan = java.util.Base64.getDecoder().decode(metadata.get(position++));
        String inputId = metadata.get(position++);
        int inputCount = Integer.parseInt(metadata.get(position++));
        List<InputColumn> columns = new ArrayList<>(inputCount);
        for (int i = 0; i < inputCount; i++) {
            columns.add(new InputColumn(metadata.get(position++), ColumnKind.valueOf(metadata.get(position++))));
        }
        byte[] plan = ArrowBatchSourceExecutorHolder.get().attachFragment(fragment, innerPlan);
        List<String> encoded = new ArrayList<>();
        encoded.add(ARROW_SOURCE_PLAN_MARKER);
        encoded.add(java.util.Base64.getEncoder().encodeToString(plan));
        encoded.add(inputId);
        encoded.add(Integer.toString(columns.size()));
        for (InputColumn column : columns) {
            encoded.add(column.name());
            encoded.add(column.kind().name());
        }
        List<String> names = outputNames(fragment);
        encoded.add(Integer.toString(names.size()));
        encoded.addAll(names);

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.writeStringCollection(encoded);
            output.writeBytes(innerBytes, tailOffset, innerBytes.length - tailOffset);
            return BytesReference.toBytes(output.bytes());
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialize attached Lucene Arrow source fragment", e);
        }
    }

    private static String outputName(String name, int ordinal) {
        return name == null ? "EXPR$" + ordinal : name;
    }

    @Override
    public WireFormat wireFormat() {
        // convertFragment emits a custom NamedWriteable wire format ([columnNames][hasFilter]
        // [BoolQueryBuilder]?), not self-describing. The orchestrator queries this so it
        // knows to emit a separate schema-only stub via convertSchemaOnlyRead for the
        // coordinator's reduce-sink partition registration.
        return WireFormat.OPAQUE;
    }

    /**
     * Substrait stub describing the count fragment's output partition: one
     * {@code Plan{Read{named_table; base_schema}}} carrying the partition's named-table id
     * and column types. Mirrors {@code DataFusionFragmentConvertor.convertSchemaOnlyRead} —
     * same proto shape, decoded by the same Rust {@code derive_schema_from_partial_plan} on
     * the coordinator.
     *
     * <p>In production (selector with default {@code prefer_metadata_driver=true}) the only
     * Lucene plans reaching this method are the Aggregate-rooted count fast path, where the
     * stub describes a single {@code I64 NOT NULL} column per aggregate call. Tests that pin
     * {@code prefer=false} keep both alternatives — the Lucene plan there can be Filter-rooted
     * over the upstream scan rowType, which is why {@link #toSubstraitType} maps a few extra
     * primitives. Those bytes are never dispatched (the data node picks the peer alternative);
     * the mapping exists so the test path doesn't blow up at conversion.
     */
    @Override
    public byte[] convertSchemaOnlyRead(int childStageId, RelDataType rowType) {
        // Struct-level nullability stays REQUIRED (the row itself is always present); per-field
        // nullability is encoded inside each Type via toSubstraitType. Declared per-field
        // nullability MUST match what LuceneSearchExecEngine.buildSchema produces — Lucene's
        // count emission uses nullable Int64, so the stub's columns must say NULLABLE too. A
        // mismatch here used to silently hang at the partition stream (Rust registers a
        // NOT-NULL partition, runtime batches arrive nullable, drain stalls).
        Type.Struct.Builder structBuilder = Type.Struct.newBuilder().setNullability(Type.Nullability.NULLABILITY_REQUIRED);
        NamedStruct.Builder namedStructBuilder = NamedStruct.newBuilder();
        for (RelDataTypeField field : rowType.getFieldList()) {
            namedStructBuilder.addNames(field.getName());
            structBuilder.addTypes(toSubstraitType(field.getType()));
        }
        namedStructBuilder.setStruct(structBuilder.build());

        ReadRel readRel = ReadRel.newBuilder()
            .setNamedTable(ReadRel.NamedTable.newBuilder().addNames("input-" + childStageId).build())
            .setBaseSchema(namedStructBuilder.build())
            .build();
        Rel inputRel = Rel.newBuilder().setRead(readRel).build();
        PlanRel planRel = PlanRel.newBuilder()
            .setRoot(RelRoot.newBuilder().setInput(inputRel).addAllNames(rowType.getFieldNames()).build())
            .build();

        byte[] bytes = Plan.newBuilder().addRelations(planRel).build().toByteArray();
        LOGGER.debug(
            "[lucene-count] convertSchemaOnlyRead stage={} fields={} bytes={}",
            childStageId,
            rowType.getFieldNames(),
            bytes.length
        );
        return bytes;
    }

    /**
     * Minimal Calcite→Substrait type mapper for the schema-only Read. Covers the count
     * fast path (BIGINT) plus the few primitives a non-driver Lucene plan's row type can
     * carry (text/keyword → string, numerics, boolean). The result is only used for
     * coordinator-side partition registration; the bytes never round-trip back to a
     * Calcite type.
     *
     * <p><b>Nullability:</b> Calcite's COUNT aggregate types as BIGINT NOT NULL, but Lucene's
     * runtime emits a nullable Int64 column ({@code LuceneSearchExecEngine.buildSchema}
     * builds {@code FieldType(true, Int(64,true), null)} — the leading {@code true} is
     * nullable). The Substrait stub MUST reflect the producer's actual runtime schema, not
     * the Calcite logical type, otherwise the Rust-side partition stream registers as
     * NOT-NULL and silently stalls when nullable batches arrive. Force nullable for now;
     * when the driver supports more shapes, this will need a per-column source-of-truth.
     *
     * <p>TODO: when Lucene-driver shapes beyond COUNT land (group-by-count keys), wire in a
     * proper Calcite→Substrait converter so the stub describes real producer schemas.
     */
    private static Type toSubstraitType(RelDataType type) {
        // Always nullable to match LuceneSearchExecEngine.buildSchema's output. See class doc.
        Type.Nullability n = Type.Nullability.NULLABILITY_NULLABLE;
        return switch (type.getSqlTypeName()) {
            case BIGINT -> Type.newBuilder().setI64(Type.I64.newBuilder().setNullability(n)).build();
            case INTEGER -> Type.newBuilder().setI32(Type.I32.newBuilder().setNullability(n)).build();
            case SMALLINT -> Type.newBuilder().setI16(Type.I16.newBuilder().setNullability(n)).build();
            case TINYINT -> Type.newBuilder().setI8(Type.I8.newBuilder().setNullability(n)).build();
            case BOOLEAN -> Type.newBuilder().setBool(Type.Boolean.newBuilder().setNullability(n)).build();
            case DOUBLE -> Type.newBuilder().setFp64(Type.FP64.newBuilder().setNullability(n)).build();
            case FLOAT, REAL -> Type.newBuilder().setFp32(Type.FP32.newBuilder().setNullability(n)).build();
            case VARCHAR, CHAR -> Type.newBuilder().setString(Type.String.newBuilder().setNullability(n)).build();
            case DATE, TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE -> Type.newBuilder()
                .setPrecisionTimestamp(Type.PrecisionTimestamp.newBuilder().setPrecision(3).setNullability(n))
                .build();
            default -> throw new IllegalStateException(
                "Lucene convertSchemaOnlyRead: unmapped Calcite type " + type.getSqlTypeName() + " for field of type " + type
            );
        };
    }

    /**
     * Walks the linear input chain looking for any Calcite {@link Filter} (covers both
     * {@link OpenSearchFilter} and the plain {@code LogicalFilter} that
     * {@code FragmentConversionDriver.strip} produces once annotation resolution unwraps the
     * filter's condition into native predicate calls).
     */
    private static Filter findFilter(RelNode node) {
        RelNode current = node;
        while (current != null) {
            if (current instanceof Filter filter) return filter;
            if (current.getInputs().isEmpty()) return null;
            current = current.getInputs().getFirst();
        }
        return null;
    }

    /**
     * Returns the field-storage info for a filter's child operator. When the filter is a
     * native {@link OpenSearchFilter} this is just its own {@code getOutputFieldStorage()};
     * for a plain {@code LogicalFilter} produced by {@code strip()}, walk the input chain to
     * the nearest {@link OpenSearchRelNode} (the TableScan) and use its storage. Per-leaf
     * serializers consult this list to resolve column references back to their backing fields.
     */
    private static List<FieldStorageInfo> findFieldStorage(Filter filter) {
        if (filter instanceof OpenSearchFilter osf) {
            return osf.getOutputFieldStorage();
        }
        RelNode current = filter.getInput();
        while (current != null) {
            if (current instanceof OpenSearchRelNode osNode) {
                return osNode.getOutputFieldStorage();
            }
            if (current.getInputs().isEmpty()) break;
            current = current.getInputs().getFirst();
        }
        // Every Lucene-driver fragment has an OpenSearchTableScan ancestor by construction
        // (the table-scan rule wraps it before forking). If we got here, FragmentConversionDriver
        // produced an unexpected shape — fail loud so the planner bug is visible at conversion
        // time, not later when a serializer NPEs on missing field storage.
        throw new IllegalStateException("Lucene-driver filter has no OpenSearchRelNode ancestor: " + filter);
    }

    /**
     * Recursively converts a filter condition RexNode to a {@link QueryBuilder}. Mirrors
     * {@link LuceneSubtreeConvertor#toQueryBuilder} — same boolean structure handling
     * (AND→MUST, OR→SHOULD, NOT→MUST_NOT), same per-leaf serializer lookup. The duplication
     * is intentional: the delegation flow operates on a {@code DelegatedSubtreeConvertor}
     * SPI typed for serialized-bytes output, while the driver flow operates on
     * {@link FragmentConvertor} typed for whole-fragment serialization. Sharing the leaf
     * logic via a shared helper would be a follow-up cleanup.
     */
    private QueryBuilder toQueryBuilder(RexNode node, List<FieldStorageInfo> fieldStorage) {
        if (node instanceof AnnotatedPredicate ap) {
            node = ap.unwrap();
        }
        if (node instanceof RexCall call) {
            switch (call.getKind()) {
                case AND: {
                    BoolQueryBuilder b = new BoolQueryBuilder();
                    for (RexNode child : call.getOperands()) {
                        b.must(toQueryBuilder(child, fieldStorage));
                    }
                    return b;
                }
                case OR: {
                    BoolQueryBuilder b = new BoolQueryBuilder();
                    for (RexNode child : call.getOperands()) {
                        b.should(toQueryBuilder(child, fieldStorage));
                    }
                    return b;
                }
                case NOT: {
                    BoolQueryBuilder b = new BoolQueryBuilder();
                    b.mustNot(toQueryBuilder(call.getOperands().get(0), fieldStorage));
                    return b;
                }
                default:
                    return leafToQueryBuilder(call, fieldStorage);
            }
        }
        throw new IllegalStateException("Unexpected RexNode in Lucene-driver filter condition: " + node);
    }

    private QueryBuilder leafToQueryBuilder(RexCall call, List<FieldStorageInfo> fieldStorage) {
        ScalarFunction fn = ScalarFunction.fromSqlOperatorWithFallback(call.getOperator());
        if (fn == null) {
            throw new IllegalStateException("Unrecognized operator in Lucene-driver filter: " + call.getOperator());
        }
        DelegatedPredicateSerializer serializer = leafSerializers.get(fn);
        if (serializer == null) {
            throw new IllegalStateException("No Lucene serializer for [" + fn + "] in driver-mode filter");
        }
        return ((AbstractQuerySerializer) serializer).buildQueryBuilder(call, fieldStorage);
    }
}
