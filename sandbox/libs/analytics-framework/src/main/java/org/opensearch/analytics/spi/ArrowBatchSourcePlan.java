/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to this file be licensed under
 * the Apache-2.0 license or a compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.analytics.spi.ArrowBatchSourceFactory.InputColumn;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Engine plan that consumes one named {@link ArrowBatchSourceFactory} input.
 *
 * @opensearch.internal
 */
public record ArrowBatchSourcePlan(String inputId, byte[] planBytes, List<InputColumn> inputColumns) implements Writeable {

    public ArrowBatchSourcePlan {
        inputId = Objects.requireNonNull(inputId, "inputId");
        if (inputId.isBlank()) {
            throw new IllegalArgumentException("inputId must not be blank");
        }
        planBytes = Objects.requireNonNull(planBytes, "planBytes").clone();
        if (planBytes.length == 0) {
            throw new IllegalArgumentException("planBytes must not be empty");
        }
        inputColumns = List.copyOf(Objects.requireNonNull(inputColumns, "inputColumns"));
    }

    public ArrowBatchSourcePlan(StreamInput input) throws IOException {
        this(input.readString(), input.readByteArray(), input.readList(ArrowBatchSourcePlan::readInputColumn));
    }

    @Override
    public byte[] planBytes() {
        return planBytes.clone();
    }

    /** Arrow schema declared by this plan's input columns. */
    public Schema inputSchema() {
        return schemaFor(inputColumns);
    }

    /** Builds the source schema for an input-column list. */
    public static Schema schemaFor(List<InputColumn> columns) {
        return new Schema(columns.stream().map(ArrowBatchSourcePlan::toField).toList());
    }

    @Override
    public void writeTo(StreamOutput output) throws IOException {
        output.writeString(inputId);
        output.writeByteArray(planBytes);
        output.writeCollection(inputColumns, ArrowBatchSourcePlan::writeInputColumn);
    }

    private static Field toField(InputColumn column) {
        ArrowType type = switch (column.kind()) {
            case LONG -> new ArrowType.Int(64, true);
            case KEYWORD -> new ArrowType.Utf8View();
            case TIMESTAMP -> new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
            case BOOLEAN -> ArrowType.Bool.INSTANCE;
            case FLOAT -> new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case DOUBLE -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case BINARY, IP -> new ArrowType.Binary();
        };
        if (column.multiValued()) {
            Field item = new Field("item", FieldType.nullable(type), null);
            return new Field(column.name(), FieldType.nullable(new ArrowType.List()), List.of(item));
        }
        return new Field(column.name(), FieldType.nullable(type), null);
    }

    private static InputColumn readInputColumn(StreamInput input) throws IOException {
        return new InputColumn(input.readString(), input.readEnum(ArrowBatchSourceFactory.ColumnKind.class), input.readBoolean());
    }

    private static void writeInputColumn(StreamOutput output, InputColumn column) throws IOException {
        output.writeString(column.name());
        output.writeEnum(column.kind());
        output.writeBoolean(column.multiValued());
    }
}
