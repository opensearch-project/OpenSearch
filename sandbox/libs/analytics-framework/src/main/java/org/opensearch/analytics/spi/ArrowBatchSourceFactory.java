/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import java.util.Map;

/**
 * Reusable factory for DataFusion-driven Arrow scans.
 *
 * <p>The projection contains zero-based indices into the factory's declared input
 * schema. Every open call returns an independent sequential cursor.
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
        TIMESTAMP
    }

    /** One source column. */
    record InputColumn(String name, ColumnKind kind) {
    }

    ArrowBatchSource open(int[] projection) throws Exception;

    /** Query-scoped source metrics, safe to call before close. */
    default Map<String, Long> metrics() {
        return Map.of();
    }

    @Override
    void close();
}
