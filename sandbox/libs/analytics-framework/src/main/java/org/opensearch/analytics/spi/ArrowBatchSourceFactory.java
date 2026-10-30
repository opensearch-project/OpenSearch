/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

/**
 * Reusable factory for DataFusion-driven Arrow scans.
 *
 * <p>The projection contains zero-based indices into the factory's declared input
 * schema. Every open call returns an independent sequential cursor. Closing the factory
 * must be idempotent.
 *
 * @opensearch.internal
 */
public interface ArrowBatchSourceFactory extends AutoCloseable {

    /** Logical type of one source column. */
    enum ColumnKind {
        /** Signed 64-bit integer. */
        LONG,
        /** UTF-8 keyword. */
        KEYWORD,
        /** Millisecond timestamp. */
        TIMESTAMP,
        /** Boolean value encoded as zero or one. */
        BOOLEAN,
        /** IEEE 754 single-precision floating-point value. */
        FLOAT,
        /** IEEE 754 double-precision floating-point value. */
        DOUBLE,
        /** Opaque bytes. */
        BINARY,
        /** Encoded IP address bytes. */
        IP
    }

    /** One source column. Multi-valued columns use an Arrow list of the declared kind. */
    record InputColumn(String name, ColumnKind kind, boolean multiValued) {
        public InputColumn(String name, ColumnKind kind) {
            this(name, kind, false);
        }
    }

    ArrowBatchSource open(int[] projection) throws Exception;

    @Override
    void close();
}
