/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;

/**
 * Supplies a fresh {@link XContentParser} positioned at the start of a buffered field value.
 *
 * <p>Handed to dynamic-mapping plugin extension points ({@link DynamicFieldTypeInferencer} and
 * {@link DynamicTemplateTypeHandler}) so they can inspect an unmapped field's value without core
 * committing to any deserialized representation. Each call to {@link #get()} creates an independent
 * parser over the same buffered bytes, so a plugin may read the value more than once. The caller
 * closes the returned parser (e.g. via try-with-resources).
 *
 * <p>The buffered content is a single field <em>value</em> (a scalar or an array/object of scalars),
 * never a document with named-XContent objects, so the parser is created with
 * {@link NamedXContentRegistry#EMPTY}: a plugin gets the raw tokens of the value and nothing else,
 * with no access to core's registry of named parsers.
 *
 * <p>Plugins whose configuration is already complete need not call {@link #get()} at all — index-creation
 * validation constructs the supplier with {@link #withoutValue()}, whose {@link #get()} throws, so a
 * complete config that never reads the value is validated without any field data being available.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class FieldValueParserSupplier {

    private final MediaType contentType;
    private final DeprecationHandler deprecationHandler;
    private final byte[] rawContent;

    /**
     * Creates a supplier over the buffered field value. {@link #get()} produces a fresh parser
     * positioned at the value's first token on each call.
     */
    public FieldValueParserSupplier(MediaType contentType, DeprecationHandler deprecationHandler, byte[] rawContent) {
        this.contentType = contentType;
        this.deprecationHandler = deprecationHandler;
        this.rawContent = rawContent;
    }

    /**
     * A supplier with no field value behind it — {@link #get()} throws. Used at index-creation time,
     * where a fully specified template config must be validated without reading any document.
     */
    public static FieldValueParserSupplier withoutValue() {
        return new FieldValueParserSupplier(null, null, null);
    }

    /**
     * Returns a fresh {@link XContentParser} positioned at the first token of the field value. The
     * caller is responsible for closing it (e.g. via try-with-resources). The parser uses
     * {@link NamedXContentRegistry#EMPTY} — a raw field value needs no named-object resolution.
     *
     * @throws IOException if the parser cannot be created
     * @throws IllegalStateException if this supplier has no field value ({@link #withoutValue()})
     */
    public XContentParser get() throws IOException {
        if (rawContent == null) {
            throw new IllegalStateException("No field value is available to parse");
        }
        XContentParser parser = contentType.xContent().createParser(NamedXContentRegistry.EMPTY, deprecationHandler, rawContent);
        parser.nextToken(); // position at the start of the value
        return parser;
    }
}
