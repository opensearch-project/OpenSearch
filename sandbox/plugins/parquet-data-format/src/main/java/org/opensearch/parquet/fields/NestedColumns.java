/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields;

/**
 * Arrow field-metadata convention for the Engine-4 (parallel LIST columns + element index) nested
 * layout. See {@code MustangDevConfig design/nested-field-support/11} and {@code 12}.
 *
 * <p>Each {@code nested} leaf is stored as its own {@code LIST<primitive>} column on the parent row,
 * carrying the {@link #NESTED_PATH_META_KEY} field-metadata entry so the write side can group sibling
 * leaves of a path. The element→row mapping is the element index's {@code __parent_row__} doc-value;
 * there are no per-row bridge columns on the parquet side.
 */
public final class NestedColumns {

    private NestedColumns() {}

    /** Field-metadata key on a {@code LIST} leaf column naming the nested path it belongs to. */
    public static final String NESTED_PATH_META_KEY = "mustang.nested.path";
}
