/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.calcite.rel.type.RelDataType;

/**
 * Bidirectional Arrow ↔ Calcite type converter for single types.
 *
 * <p>Used by the QTF (late-materialization) Phase C in
 * {@code LateMaterializationStageExecution} to translate the above-anchor physical fields'
 * Calcite {@link RelDataType}s into Arrow {@link ArrowType}s for the fetch-stage output
 * schema. The {@code AggregateFunction.ArrowToCalciteTypeMapper} (in the SPI module) handles
 * the inverse direction for {@code IntermediateField} resolution; this class is kept as the
 * single authority for the Calcite→Arrow direction needed outside that resolver.
 *
 * <p>FIXME [FixBeforeMainMerge] coverage gaps: TIMESTAMP currently hardcodes MILLISECOND
 * (Calcite precision is ignored — see toArrow note), so date_nanos is not yet distinguished,
 * and several Calcite/Arrow types are still unmapped (TIMESTAMP_WITH_LOCAL_TIME_ZONE, DATE,
 * TIME, DECIMAL, Arrow Date/Time/Decimal/...). Audit and broaden before merge so the QTF path
 * tolerates non-keyword/non-date columns end-to-end.
 */
public final class ArrowCalciteTypes {

    private ArrowCalciteTypes() {}

    /**
     * Convert a Calcite {@link RelDataType} to the corresponding Arrow type.
     */
    public static ArrowType toArrow(RelDataType t) {
        return switch (t.getSqlTypeName()) {
            case BIGINT -> new ArrowType.Int(64, true);
            case INTEGER -> new ArrowType.Int(32, true);
            // Match the wire Arrow type the data node emits: ShortParquetField -> Int(16),
            // ByteParquetField -> Int(8). Keeps the Stitcher's copyFromSafe types aligned.
            case SMALLINT -> new ArrowType.Int(16, true);
            case TINYINT -> new ArrowType.Int(8, true);
            case DOUBLE -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case REAL, FLOAT -> new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            // Utf8View matches what the DataFusion/parquet path on the data node emits for
            // string columns; switching here keeps the coordinator-side Stitcher's pre-allocated
            // output type aligned so copyFromSafe doesn't trip on a VARCHAR/VIEWVARCHAR mismatch.
            case VARCHAR, CHAR -> ArrowType.Utf8View.INSTANCE;
            case VARBINARY, BINARY -> ArrowType.Binary.INSTANCE;
            case BOOLEAN -> ArrowType.Bool.INSTANCE;
            // TODO: TIMESTAMP_WITH_LOCAL_TIME_ZONE, DATE, TIME, DECIMAL still missing.
            // TODO: hardcoded MILLISECOND to match what DateParquetField emits on the data node;
            // Calcite's reported precision doesn't track the wire-level Arrow precision today, so
            // honouring t.getPrecision() here would break Stitcher copyFromSafe. Revisit when
            // Calcite types carry the data-node-side Arrow precision faithfully.
            case TIMESTAMP -> new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
            default -> throw new IllegalArgumentException("Unsupported Calcite type: " + t.getSqlTypeName());
        };
    }
}
