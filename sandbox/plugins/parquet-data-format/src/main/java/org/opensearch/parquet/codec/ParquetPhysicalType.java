/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec;

/**
 * The Parquet physical type of a column, as understood by the native column reader.
 *
 * <p>The {@link #code()} values are the discriminants exchanged with the Rust FFM
 * bridge ({@code parquet_open_column_reader}'s {@code expected_type} argument and the
 * physical-type validation it performs). They must stay in lock-step with the
 * {@code TYPE_*} constants in {@code ffm.rs}.
 *
 * <p>Parquet's {@code FIXED_LEN_BYTE_ARRAY} is folded into {@link #BYTE_ARRAY} because
 * the codec treats both as opaque byte sequences for {@code BinaryDocValues} purposes.
 */
public enum ParquetPhysicalType {
    /** 32-bit signed integer; raw bits sign-extended to a {@code long}. */
    INT32(0),
    /** 64-bit signed integer; raw bits verbatim. */
    INT64(1),
    /** 32-bit IEEE-754 float; raw bits via {@link Float#floatToRawIntBits}. */
    FLOAT(2),
    /** 64-bit IEEE-754 double; raw bits via {@link Double#doubleToRawLongBits}. */
    DOUBLE(3),
    /** Boolean; encoded as 0 or 1. */
    BOOL(4),
    /** Variable- or fixed-length byte array (UTF-8 strings, IP, binary). */
    BYTE_ARRAY(5);

    private final int code;

    ParquetPhysicalType(int code) {
        this.code = code;
    }

    /** The native discriminant exchanged with the Rust FFM bridge. */
    public int code() {
        return code;
    }

    /** True for the fixed-width primitive types whose values are exchanged as raw {@code long} bits. */
    public boolean isPrimitive() {
        return this != BYTE_ARRAY;
    }
}
