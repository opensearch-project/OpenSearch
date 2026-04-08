/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

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
import org.apache.arrow.memory.BufferAllocator;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.EngineBridge;
import org.opensearch.core.action.ActionListener;
import org.opensearch.datafusion.jni.NativeBridge;
import org.opensearch.datafusion.search.DatafusionContext;
import org.opensearch.datafusion.search.DatafusionReader;

import java.io.Closeable;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * DataFusion implementation of {@link EngineBridge} that converts Calcite
 * {@link RelNode} fragments to Substrait bytes and executes them via the
 * native DataFusion engine through JNI.
 *
 * <p>Each bridge instance is tied to a specific {@link DatafusionReader}
 * and runtime pointer. The bridge owns the reader lifecycle and closes it
 * when {@link #close()} is called.
 *
 * @opensearch.internal
 */

public class DataFusionBridge implements EngineBridge<byte[], DataFusionResultStream, RelNode>, Closeable {

    private static final Logger logger = LogManager.getLogger(DataFusionBridge.class);

    private static volatile SimpleExtension.ExtensionCollection EXTENSIONS;

    /** Must be called with the plugin classloader as thread context classloader. */
    static SimpleExtension.ExtensionCollection getExtensions() {
        if (EXTENSIONS == null) {
            synchronized (DataFusionBridge.class) {
                if (EXTENSIONS == null) {
                    Thread t = Thread.currentThread();
                    ClassLoader original = t.getContextClassLoader();
                    t.setContextClassLoader(DataFusionBridge.class.getClassLoader());
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

    private final long runtimePointer;
    private final DatafusionReader reader;
    private final BufferAllocator allocator;
    private long contextId;

    /**
     * Creates a new DataFusion bridge.
     *
     * @param runtimePointer the native DataFusion runtime pointer
     * @param reader         the native reader wrapping catalog snapshot files
     * @param allocator      the Arrow buffer allocator for result streams
     */
    public DataFusionBridge(long runtimePointer, DatafusionReader reader, BufferAllocator allocator) {
        this.runtimePointer = runtimePointer;
        this.reader = reader;
        this.allocator = allocator;
    }

    public void setContextId(long contextId) {
        this.contextId = contextId;
    }

    /**
     * Converts a Calcite {@link RelNode} fragment into Substrait bytes.
     *
     * <p>Uses the Calcite Isthmus library to visit the relational tree and
     * produce a Substrait {@link Plan}, then serialises it to protobuf bytes
     * that the native DataFusion engine can consume.
     *
     * @param fragment the logical plan subtree to serialise
     * @return Substrait protobuf bytes representing the plan
     */
    @Override
    public byte[] convertFragment(RelNode fragment) {
        logger.debug("Converting RelNode to Substrait: {}", fragment.getRelTypeName());

        RelRoot root = RelRoot.of(fragment, SqlKind.SELECT);
        SubstraitRelVisitor visitor = createVisitor(fragment);
        Rel substraitRel = visitor.apply(root.rel);

        // Build Plan.Root with field names from the RelRoot
        List<String> fieldNames = root.fields.stream()
            .map(field -> field.getValue())
            .collect(Collectors.toList());
        Plan.Root substraitRoot = Plan.Root.builder()
            .input(substraitRel)
            .names(fieldNames)
            .build();

        Plan plan = Plan.builder().addRoots(substraitRoot).build();

        // Serialize to protobuf bytes
        PlanProtoConverter planProtoConverter = new PlanProtoConverter();
        io.substrait.proto.Plan protoPlan = planProtoConverter.toProto(plan);

        byte[] bytes = protoPlan.toByteArray();
        logger.debug("Substrait plan serialized: {} bytes", bytes.length);
        return bytes;
    }

    /**
     * Creates a {@link SubstraitRelVisitor} configured with the default
     * Substrait extension catalog and the type factory from the given RelNode.
     */
    private static SubstraitRelVisitor createVisitor(RelNode relNode) {
        RelDataTypeFactory typeFactory = relNode.getCluster().getTypeFactory();
        TypeConverter typeConverter = TypeConverter.DEFAULT;

        // Map standard Calcite operators to Substrait function names.
        // The default FunctionMappings.*_SIGS map Isthmus's custom operators,
        // but PPL produces standard Calcite SqlStdOperatorTable operators.

        // Aggregate mappings
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
        AggregateFunctionConverter aggConverter = new AggregateFunctionConverter(
            getExtensions().aggregateFunctions(),
            aggSigs,
            typeFactory,
            typeConverter
        );

        ScalarFunctionConverter scalarConverter = new ScalarFunctionConverter(
            getExtensions().scalarFunctions(),
            Collections.emptyList(),
            typeFactory,
            typeConverter
        );
        WindowFunctionConverter windowConverter = new WindowFunctionConverter(
            getExtensions().windowFunctions(),
            typeFactory
        );

        return new SubstraitRelVisitor(
            typeFactory,
            scalarConverter,
            aggConverter,
            windowConverter,
            typeConverter,
            ImmutableFeatureBoard.builder().build()
        );
    }

    /**
     * Executes the Substrait plan bytes against the native DataFusion engine
     * and returns a {@link DataFusionResultStream} for consuming record batches.
     *
     * @param fragment the Substrait plan bytes produced by {@link #convertFragment}
     * @return a result stream wrapping the native record batch stream
     */
    @Override
    public DataFusionResultStream execute(byte[] fragment) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        NativeBridge.executeQueryPhaseAsync(
            reader.getReaderPtr(),
            "",       // tableName — resolved by the native engine from the plan
            fragment,
            false,    // isQueryPlanExplainEnabled
            0,        // partitionCount — use engine default
            runtimePointer,
            contextId,
            new ActionListener<Long>() {
                @Override
                public void onResponse(Long streamPointer) {
                    future.complete(streamPointer);
                }

                @Override
                public void onFailure(Exception e) {
                    future.completeExceptionally(e);
                }
            }
        );

        long streamPointer;
        try {
            streamPointer = future.join();
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute query phase", e);
        }
        return new DataFusionResultStream(streamPointer, runtimePointer, allocator);
    }

    @Override
    public void close() {
        reader.close();
    }
}
