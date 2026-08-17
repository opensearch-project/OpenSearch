/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields;

/**
 * Naming and Arrow field-metadata conventions for the Engine-4 (parallel LIST columns + element
 * index) nested layout. See {@code MustangDevConfig design/nested-field-support/11} and {@code 12}.
 *
 * <p>Each {@code nested} leaf is stored as its own {@code LIST<primitive>} column on the parent row,
 * carrying the {@link #NESTED_PATH_META_KEY} field-metadata entry so the write side can group sibling
 * leaves of a path. Per path a pair of plain {@code long} bridge columns
 * ({@link #offsetColumn(String)} / {@link #countColumn(String)}) records where each row's elements
 * live in the path's value arrays — global element offset and count — so a nested filter can be
 * turned into a parent {@code RowSelection} without decoding the list columns.
 */
public final class NestedColumns {

    private NestedColumns() {}

    /** Field-metadata key on a {@code LIST} leaf column naming the nested path it belongs to. */
    public static final String NESTED_PATH_META_KEY = "mustang.nested.path";

    /** Field-metadata key on a bridge column naming the nested path it describes. */
    public static final String NESTED_BRIDGE_META_KEY = "mustang.nested.bridge";

    /** {@link #NESTED_BRIDGE_META_KEY} value for the offset column. */
    public static final String BRIDGE_OFFSET = "offset";

    /** {@link #NESTED_BRIDGE_META_KEY} value for the count column. */
    public static final String BRIDGE_COUNT = "count";

    private static final String OFFSET_SUFFIX = ".__nested_off__";
    private static final String COUNT_SUFFIX = ".__nested_cnt__";

    /** Name of the per-path bridge offset column (global element start offset for each row). */
    public static String offsetColumn(String nestedPath) {
        return nestedPath + OFFSET_SUFFIX;
    }

    /** Name of the per-path bridge count column (element count for each row). */
    public static String countColumn(String nestedPath) {
        return nestedPath + COUNT_SUFFIX;
    }

    /** True if {@code columnName} is a bridge offset column produced by {@link #offsetColumn}. */
    public static boolean isOffsetColumn(String columnName) {
        return columnName.endsWith(OFFSET_SUFFIX);
    }

    /** True if {@code columnName} is a bridge count column produced by {@link #countColumn}. */
    public static boolean isCountColumn(String columnName) {
        return columnName.endsWith(COUNT_SUFFIX);
    }

    /** The nested path a bridge column describes, i.e. the inverse of {@link #offsetColumn}/{@link #countColumn}. */
    public static String pathOfBridgeColumn(String columnName) {
        if (isOffsetColumn(columnName)) {
            return columnName.substring(0, columnName.length() - OFFSET_SUFFIX.length());
        }
        if (isCountColumn(columnName)) {
            return columnName.substring(0, columnName.length() - COUNT_SUFFIX.length());
        }
        throw new IllegalArgumentException("Not a bridge column: [" + columnName + "]");
    }
}
