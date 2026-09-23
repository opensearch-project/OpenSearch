/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * One flattened leaf of an open key-space field (e.g. {@code flat_object}), as handed to
 * {@link DocumentInput#addField} by the field's mapper.
 * <p>
 * A field type whose value is an open, dynamically-keyed object cannot be represented as one
 * scalar per column. Its mapper instead emits the object's leaves one at a time, each as a
 * {@code FlattenedLeaf} carrying the leaf's dotted path <em>relative to the field</em>
 * ({@code http.method}, not {@code attributes.http.method} — the field's own name is carried by
 * {@code MappedFieldType#name()} on the same {@code addField} call) and the leaf's value,
 * stringified and already filtered/normalized per the field's mapping parameters. Duplicate
 * relative paths are legal and must be preserved in emission order.
 * <p>
 * How a format stores the leaves — a single map column, exploded per-key columns, a variant
 * encoding — is entirely the format's decision. A format that cannot represent the field simply
 * ignores these values via its capability self-filter.
 *
 * @param relativePath the leaf's dotted path relative to the emitting field, never null
 * @param value        the stringified leaf value, never null
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public record FlattenedLeaf(String relativePath, String value) {
    /**
     * Validates that both components are present.
     *
     * @param relativePath the leaf's dotted path relative to the emitting field
     * @param value        the stringified leaf value
     */
    public FlattenedLeaf {
        if (relativePath == null || value == null) {
            throw new IllegalArgumentException("relativePath and value must be non-null");
        }
    }
}
