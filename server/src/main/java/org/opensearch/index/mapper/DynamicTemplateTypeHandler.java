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
import java.util.Map;

/**
 * Handler for plugin-registered dynamic template types.
 * Called when a dynamic template matches on a plugin-registered type string
 * to allow the plugin to adjust the mapping configuration before the mapper is built.
 *
 * <p>Rather than deserializing the field value for the handler, core hands it a
 * {@link FieldValueParserSupplier} that produces a fresh {@link XContentParser} over the buffered
 * bytes. A handler whose template config is already complete never calls {@code get()},
 * so no parsing happens for fully-specified templates. A handler that needs a
 * data-derived parameter creates a parser and reads what it needs.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface DynamicTemplateTypeHandler {

    /**
     * Adjust the mapping configuration before the TypeParser builds the mapper.
     * Called when a dynamic template matches but before the mapper is constructed.
     *
     * @param mappingConfig the mutable mapping config from the template (modified in place)
     * @param fieldValueParser produces a fresh {@link XContentParser} over the buffered field bytes;
     *                      only call {@code get()} if the config is missing a parameter that must
     *                      be derived from the data. Close the returned parser (e.g. via
     *                      try-with-resources).
     * @throws IOException if reading from the parser fails
     */
    void adjustMappingConfig(Map<String, Object> mappingConfig, FieldValueParserSupplier fieldValueParser) throws IOException;

    /**
     * Returns {@code true} if the given template mapping config is fully specified — i.e. building a
     * mapper from it requires no parameter derived from a document ({@link #adjustMappingConfig} would
     * not need to read the field value). When this returns {@code true}, core validates the template
     * eagerly at index-creation time by handing the config to the {@link Mapper.TypeParser}, which
     * reports any invalid content. When it returns {@code false}, validation is deferred to
     * document-parse time, where the data-derived parameters become available.
     *
     * @param mappingConfig the mapping config from the matched template
     * @return whether the config can be validated without inspecting a document
     */
    default boolean isConfigComplete(Map<String, Object> mappingConfig) {
        return false;
    }
}
