/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.mapper.MappedFieldType;

/**
 * A {@link DocumentInput} that actually represents repeating groups ({@code nested} arrays) and open
 * key spaces ({@code flat_object} maps) — e.g. Parquet, which buffers a variable number of elements
 * into an in-memory tree as fields stream in. A format with no such notion (e.g. Lucene) implements
 * plain {@link DocumentInput} instead; the broadcaster that composes several {@code DocumentInput}s
 * checks for this interface before forwarding any nested-scope signal.
 *
 * @param <T> the type of the final input representation
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface NestedAwareDocumentInput<T> extends DocumentInput<T> {

    @Override
    void startNestedChild(String nestedPath);

    @Override
    void endNestedChild();

    @Override
    void addMapEntry(MappedFieldType mapField, String key, Object value);
}
