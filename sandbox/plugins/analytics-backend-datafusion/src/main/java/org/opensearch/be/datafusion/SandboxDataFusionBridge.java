/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.opensearch.analytics.backend.EngineBridge;
import org.opensearch.analytics.backend.EngineResultBatch;
import org.opensearch.analytics.backend.EngineResultBatchIterator;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.core.action.ActionListener;
import org.opensearch.be.datafusion.jni.NativeBridge;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.ImmutableFeatureBoard;
import io.substrait.isthmus.SubstraitRelVisitor;
import io.substrait.isthmus.TypeConverter;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.FunctionMappings;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.relation.Rel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Sandbox bridge: Calcite RelNode → Substrait bytes → native DataFusion execution.
 * Uses {@link DatafusionReader} for the reader and {@link NativeBridge} for JNI calls.
 * No dependency on engine-datafusion's search classes.
 */
public class SandboxDataFusionBridge implements EngineBridge<byte[], SandboxDataFusionBridge.ResultStream, RelNode>, Closeable {

    private static volatile SimpleExtension.ExtensionCollection EXTENSIONS;

    private final long runtimePtr;
    private final DatafusionReader reader;
    private final BufferAllocator allocator;

    public SandboxDataFusionBridge(long runtimePtr, DatafusionReader reader) {
        this.runtimePtr = runtimePtr;
        this.reader = reader;
        this.allocator = new RootAllocator(Long.MAX_VALUE);
    }

    static SimpleExtension.ExtensionCollection getExtensions() {
        if (EXTENSIONS == null) {
            synchronized (SandboxDataFusionBridge.class) {
                if (EXTENSIONS == null) {
                    Thread t = Thread.currentThread();
                    ClassLoader original = t.getContextClassLoader();
                    t.setContextClassLoader(SandboxDataFusionBridge.class.getClassLoader());
                    try {
                        EXTENSIONS = DefaultExtensionCatalog.DEFAULT_COLLECTION;
                    } finally {
                        t.setContextClassLoader(original);
                    }
                }
            }
        }
        return EXTENSIONS;
    }

    @Override
    public byte[] convertFragment(RelNode fragment) {
        RelRoot root = RelRoot.of(fragment, SqlKind.SELECT);
        SubstraitRelVisitor visitor = createVisitor(fragment);
        Rel substraitRel = visitor.apply(root.rel);

        List<String> fieldNames = root.fields.stream()
            .map(f -> f.getValue())
            .collect(Collectors.toList());

        Plan plan = Plan.builder()
            .addRoots(Plan.Root.builder().input(substraitRel).names(fieldNames).build())
            .build();

        io.substrait.proto.Plan protoPlan = new PlanProtoConverter().toProto(plan);
        // Strip schema prefix from NamedTable — native DataFusion only knows bare table names
        byte[] bytes = stripSchemaFromPlan(protoPlan);
        return bytes;
    }

    /**
     * Recursively strips schema prefixes from NamedTable references in the plan.
     * e.g. names: ["opensearch", "hits"] → names: ["hits"]
     */
    static String extractTableNameFromBytes(byte[] substraitBytes) {
        return extractTableName(substraitBytes);
    }

    /**
     * Static conversion: RelNode → Substrait bytes. No reader/bridge instance needed.
     */
    static byte[] convertRelNode(RelNode fragment) {
        RelRoot root = RelRoot.of(fragment, SqlKind.SELECT);
        SubstraitRelVisitor visitor = createVisitor(fragment);
        Rel substraitRel = visitor.apply(root.rel);

        List<String> fieldNames = root.fields.stream()
            .map(f -> f.getValue())
            .collect(Collectors.toList());

        Plan plan = Plan.builder()
            .addRoots(Plan.Root.builder().input(substraitRel).names(fieldNames).build())
            .build();

        io.substrait.proto.Plan protoPlan = new PlanProtoConverter().toProto(plan);
        return stripSchemaFromPlan(protoPlan);
    }

    private static String extractTableName(byte[] substraitBytes) {
        try {
            io.substrait.proto.Plan plan = io.substrait.proto.Plan.parseFrom(substraitBytes);
            for (io.substrait.proto.PlanRel rel : plan.getRelationsList()) {
                if (rel.hasRoot()) {
                    String name = findTableName(rel.getRoot().getInput());
                    if (name != null) return name;
                }
            }
        } catch (Exception e) {
            // fall through
        }
        return "hits"; // fallback
    }

    private static String findTableName(io.substrait.proto.Rel rel) {
        if (rel.hasRead() && rel.getRead().hasNamedTable()) {
            var names = rel.getRead().getNamedTable().getNamesList();
            return names.isEmpty() ? null : names.get(names.size() - 1);
        }
        if (rel.hasFilter()) return findTableName(rel.getFilter().getInput());
        if (rel.hasProject()) return findTableName(rel.getProject().getInput());
        if (rel.hasAggregate()) return findTableName(rel.getAggregate().getInput());
        if (rel.hasSort()) return findTableName(rel.getSort().getInput());
        if (rel.hasFetch()) return findTableName(rel.getFetch().getInput());
        return null;
    }

    private static byte[] stripSchemaFromPlan(io.substrait.proto.Plan plan) {
        io.substrait.proto.Plan.Builder builder = plan.toBuilder();
        for (int i = 0; i < builder.getRelationsCount(); i++) {
            io.substrait.proto.PlanRel rel = builder.getRelations(i);
            if (rel.hasRoot()) {
                io.substrait.proto.RelRoot root = rel.getRoot();
                io.substrait.proto.Rel fixed = stripSchemaFromRel(root.getInput());
                builder.setRelations(i, rel.toBuilder().setRoot(root.toBuilder().setInput(fixed)).build());
            }
        }
        return builder.build().toByteArray();
    }

    private static io.substrait.proto.Rel stripSchemaFromRel(io.substrait.proto.Rel rel) {
        io.substrait.proto.Rel.Builder b = rel.toBuilder();
        if (rel.hasRead() && rel.getRead().hasNamedTable()) {
            io.substrait.proto.ReadRel read = rel.getRead();
            io.substrait.proto.ReadRel.NamedTable table = read.getNamedTable();
            if (table.getNamesCount() > 1) {
                // Keep only the last name (bare table name)
                String bareName = table.getNames(table.getNamesCount() - 1);
                b.setRead(read.toBuilder().setNamedTable(table.toBuilder().clearNames().addNames(bareName)));
            }
        }
        // Recurse into common sub-relations
        if (rel.hasFilter()) b.setFilter(rel.getFilter().toBuilder().setInput(stripSchemaFromRel(rel.getFilter().getInput())));
        if (rel.hasProject()) b.setProject(rel.getProject().toBuilder().setInput(stripSchemaFromRel(rel.getProject().getInput())));
        if (rel.hasAggregate()) b.setAggregate(rel.getAggregate().toBuilder().setInput(stripSchemaFromRel(rel.getAggregate().getInput())));
        if (rel.hasSort()) b.setSort(rel.getSort().toBuilder().setInput(stripSchemaFromRel(rel.getSort().getInput())));
        if (rel.hasFetch()) b.setFetch(rel.getFetch().toBuilder().setInput(stripSchemaFromRel(rel.getFetch().getInput())));
        return b.build();
    }

    @Override
    public ResultStream execute(byte[] substraitBytes) {
        // Extract table name from the Substrait plan for native registration
        String tableName = extractTableName(substraitBytes);
        CompletableFuture<Long> future = new CompletableFuture<>();
        NativeBridge.executeQueryPhaseAsync(
            reader.getPtr(), tableName, substraitBytes, false, 1, runtimePtr,
            new ActionListener<Long>() {
                @Override
                public void onResponse(Long ptr) {
                    future.complete(ptr);
                }

                @Override
                public void onFailure(Exception e) {
                    future.completeExceptionally(e);
                }
            }
        );
        long streamPtr = future.join();
        return new ResultStream(streamPtr, runtimePtr, allocator);
    }

    /**
     * Wraps an externally-obtained stream pointer into a consumable ResultStream.
     */
    @Override
    public ResultStream consumeStream(long streamPtr) {
        return new ResultStream(streamPtr, runtimePtr, new org.apache.arrow.memory.RootAllocator(Long.MAX_VALUE));
    }

    @Override
    public void close() {
        reader.close();
        allocator.close();
    }

    private static SubstraitRelVisitor createVisitor(RelNode relNode) {
        RelDataTypeFactory typeFactory = relNode.getCluster().getTypeFactory();
        TypeConverter typeConverter = TypeConverter.DEFAULT;

        List<FunctionMappings.Sig> aggSigs = List.of(
            new FunctionMappings.Sig(SqlStdOperatorTable.COUNT, "count"),
            new FunctionMappings.Sig(SqlStdOperatorTable.SUM, "sum"),
            new FunctionMappings.Sig(SqlStdOperatorTable.SUM0, "sum0"),
            new FunctionMappings.Sig(SqlStdOperatorTable.MIN, "min"),
            new FunctionMappings.Sig(SqlStdOperatorTable.MAX, "max"),
            new FunctionMappings.Sig(SqlStdOperatorTable.AVG, "avg"),
            new FunctionMappings.Sig(SqlStdOperatorTable.STDDEV, "std_dev"),
            new FunctionMappings.Sig(SqlStdOperatorTable.STDDEV_POP, "std_dev"),
            new FunctionMappings.Sig(SqlStdOperatorTable.STDDEV_SAMP, "std_dev"),
            new FunctionMappings.Sig(SqlStdOperatorTable.VARIANCE, "variance"),
            new FunctionMappings.Sig(SqlStdOperatorTable.VAR_POP, "variance"),
            new FunctionMappings.Sig(SqlStdOperatorTable.VAR_SAMP, "variance")
        );

        return new SubstraitRelVisitor(
            typeFactory,
            new ScalarFunctionConverter(getExtensions().scalarFunctions(), Collections.emptyList(), typeFactory, typeConverter),
            new AggregateFunctionConverter(getExtensions().aggregateFunctions(), aggSigs, typeFactory, typeConverter),
            new WindowFunctionConverter(getExtensions().windowFunctions(), typeFactory),
            typeConverter,
            ImmutableFeatureBoard.builder().build()
        );
    }

    // ---- Result types using NativeBridge directly ----

    /** Stream of Arrow record batches from native execution. */
    public static class ResultStream implements EngineResultStream {
        private final RecordBatchStream stream;
        private BatchIterator iteratorInstance;

        ResultStream(long streamPtr, long runtimePtr, BufferAllocator allocator) {
            this.stream = new RecordBatchStream(streamPtr, runtimePtr, allocator);
        }

        @Override
        public BatchIterator iterator() {
            if (iteratorInstance == null) {
                iteratorInstance = new BatchIterator(new RecordBatchIterator(stream));
            }
            return iteratorInstance;
        }

        @Override
        public void close() {
            try {
                stream.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    /** Adapts Arrow VectorSchemaRoot batches to EngineResultBatch. */
    public static class BatchIterator implements EngineResultBatchIterator {
        private final RecordBatchIterator delegate;

        BatchIterator(RecordBatchIterator delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public EngineResultBatch next() {
            VectorSchemaRoot root = delegate.next();
            return new Batch(root);
        }
    }

    /** Single Arrow record batch as EngineResultBatch. */
    public static class Batch implements EngineResultBatch {
        private final VectorSchemaRoot root;
        private final List<String> fieldNames;

        Batch(VectorSchemaRoot root) {
            this.root = root;
            this.fieldNames = root.getSchema().getFields().stream()
                .map(Field::getName)
                .collect(Collectors.toUnmodifiableList());
        }

        @Override
        public List<String> getFieldNames() {
            return fieldNames;
        }

        @Override
        public int getRowCount() {
            return root.getRowCount();
        }

        @Override
        public Object getFieldValue(String fieldName, int rowIndex) {
            FieldVector vector = root.getVector(fieldName);
            if (vector == null) throw new IllegalArgumentException("Unknown field: " + fieldName);
            return vector.getObject(rowIndex);
        }
    }
}
