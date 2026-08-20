/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * SPI for plugins to register dynamic field type inference logic.
 *
 * <p>When DocumentParser encounters an unmapped field and no dynamic template matches, it buffers
 * the field content and calls {@link #inferFieldType} on each registered inferencer in order. The
 * first non-null config map wins; the type is read from the {@code "type"} key, the mapper is built,
 * and the field content is replayed through it. If no inferencer claims the field, existing fallback
 * behavior applies.
 *
 * <p>Rather than deserializing the field value into a fixed Java representation, core hands the
 * inferencer a {@link FieldValueParserSupplier} that produces a fresh {@link XContentParser} over the
 * buffered bytes. Each call to {@code fieldValueParser.get()} returns an independent parser positioned before
 * the field value, so the plugin can inspect the content however it needs — for example streaming
 * through tokens to count array elements. This keeps core free of any representation contract: each
 * plugin decides how to interpret the value.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface DynamicFieldTypeInferencer {

    /**
     * Inspect the buffered field value and decide whether to claim it.
     *
     * @param fieldValueParser produces a fresh {@link XContentParser} over the buffered field bytes;
     *                      call {@code get()} and advance the parser to read the value. The returned
     *                      parser should be closed by the caller (e.g. via try-with-resources).
     * @return a mutable mapping config map with at minimum a {@code "type"} key (e.g.
     *         {@code {"type": "my_type", "some_param": 384}}), or {@code null} to pass to the
     *         next inferencer. The map MUST be mutable — TypeParser implementations call
     *         {@code node.remove()} on it during parsing.
     * @throws IOException if reading from the parser fails
     */
    Map<String, Object> inferFieldType(FieldValueParserSupplier fieldValueParser) throws IOException;

    /**
     * The set of mapper type strings this inferencer is allowed to produce (the {@code "type"} values it
     * may return from {@link #inferFieldType}). Every declared type must be registered by the same
     * plugin via {@link org.opensearch.plugins.MapperPlugin#getMappers()}; core validates this at
     * startup and rejects an inferencer that declares a type its plugin does not register.
     *
     * <p>At document-parse time core enforces that a claim's {@code "type"} is in this set, so an
     * inferencer cannot produce a core built-in type, a type owned by another plugin, or a type a
     * <em>sibling</em> inferencer in the same plugin is responsible for.
     *
     * <p>The default is an empty set, which core treats as a configuration error at startup: an
     * inferencer must declare at least one supported type. This makes the "registered an inferencer but
     * declared nothing" case fail loudly rather than silently claiming and then dropping every field.
     *
     * @return the non-empty set of type strings this inferencer may produce
     */
    default Set<String> supportedTypes() {
        return Collections.emptySet();
    }
}
