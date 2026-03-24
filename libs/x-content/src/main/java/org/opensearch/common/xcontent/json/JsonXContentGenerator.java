/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common.xcontent.json;

import org.opensearch.common.util.io.Streams;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContent;
import org.opensearch.core.xcontent.XContentGenerator;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.filtering.FilterPathBasedFilter;
import org.opensearch.tools.jackson.core.JacksonExceptionTranslator;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Objects;
import java.util.Set;

import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.TokenStreamContext;
import tools.jackson.core.base.GeneratorBase;
import tools.jackson.core.filter.FilteringGeneratorDelegate;
import tools.jackson.core.filter.TokenFilter.Inclusion;
import tools.jackson.core.io.SerializedString;
import tools.jackson.core.json.JsonWriteContext;
import tools.jackson.core.util.JsonGeneratorDelegate;

public class JsonXContentGenerator implements XContentGenerator {

    /** Generator used to write content **/
    protected final JsonGenerator generator;

    /**
     * Reference to base generator because
     * writing raw values needs a specific method call.
     */
    private final GeneratorBase base;

    /**
     * Reference to filtering generator because
     * writing an empty object '{}' when everything is filtered
     * out needs a specific treatment
     */
    private final FilteringGeneratorDelegate filter;

    private final OutputStream os;

    private boolean writeLineFeedAtEnd;
    private static final SerializedString LF = new SerializedString("\n");
    private boolean prettyPrint = false;

    public JsonXContentGenerator(JsonGenerator jsonGenerator, OutputStream os, Set<String> includes, Set<String> excludes) {
        Objects.requireNonNull(includes, "Including filters must not be null");
        Objects.requireNonNull(excludes, "Excluding filters must not be null");
        this.os = os;
        if (jsonGenerator instanceof GeneratorBase generatorBase) {
            this.base = generatorBase;
        } else {
            this.base = null;
        }

        JsonGenerator generator = jsonGenerator;

        boolean hasExcludes = excludes.isEmpty() == false;
        if (hasExcludes) {
            generator = new FilteringGeneratorDelegate(
                generator,
                new FilterPathBasedFilter(excludes, false),
                Inclusion.INCLUDE_ALL_AND_PATH,
                true
            );
        }

        boolean hasIncludes = includes.isEmpty() == false;
        if (hasIncludes) {
            generator = new FilteringGeneratorDelegate(
                generator,
                new FilterPathBasedFilter(includes, true),
                Inclusion.INCLUDE_ALL_AND_PATH,
                true
            );
        }

        if (hasExcludes || hasIncludes) {
            this.filter = (FilteringGeneratorDelegate) generator;
        } else {
            this.filter = null;
        }
        this.generator = generator;
    }

    @Override
    public XContentType contentType() {
        return XContentType.JSON;
    }

    @Override
    public boolean isPrettyPrint() {
        return this.prettyPrint;
    }

    @Override
    public void usePrintLineFeedAtEnd() {
        writeLineFeedAtEnd = true;
    }

    private boolean isFiltered() {
        return filter != null;
    }

    private JsonGenerator getLowLevelGenerator() {
        if (isFiltered()) {
            JsonGenerator delegate = filter.delegate();
            if (delegate instanceof JsonGeneratorDelegate jsonGeneratorDelegate) {
                // In case of combined inclusion and exclusion filters, we have one and only one another delegating level
                delegate = jsonGeneratorDelegate.delegate();
                assert delegate instanceof JsonGeneratorDelegate == false;
            }
            return delegate;
        }
        return generator;
    }

    private boolean inRoot() {
        TokenStreamContext context = generator.streamWriteContext();
        return ((context != null) && (context.inRoot() && context.currentName() == null));
    }

    @Override
    public void writeStartObject() throws IOException {
        try {
            if (inRoot()) {
                // Use the low level generator to write the startObject so that the root
                // start object is always written even if a filtered generator is used
                getLowLevelGenerator().writeStartObject();
                return;
            }
            generator.writeStartObject();
        } catch (final JacksonException ex) {
            JacksonExceptionTranslator.translateOrRethrow(ex);
        }
    }

    @Override
    public void writeEndObject() throws IOException {
        try {
            if (inRoot()) {
                // Use the low level generator to write the startObject so that the root
                // start object is always written even if a filtered generator is used
                getLowLevelGenerator().writeEndObject();
                return;
            }
            generator.writeEndObject();
        } catch (final JacksonException ex) {
            JacksonExceptionTranslator.translateOrRethrow(ex);
        }
    }

    @Override
    public void writeStartArray() throws IOException {
        try {
            generator.writeStartArray();
        } catch (final JacksonException ex) {
            JacksonExceptionTranslator.translateOrRethrow(ex);
        }
    }

    @Override
    public void writeEndArray() throws IOException {
        try {
            generator.writeEndArray();
        } catch (final JacksonException ex) {
            JacksonExceptionTranslator.translateOrRethrow(ex);
        }
    }

    @Override
    public void writeFieldName(String name) throws IOException {
        try {
            generator.writeName(name);
        } catch (final JacksonException ex) {
            JacksonExceptionTranslator.translateOrRethrow(ex);
        }
    }

    @Override
    public void writeNull() throws IOException {
        try {
            generator.writeNull();
        } catch (final JacksonException ex) {
            JacksonExceptionTranslator.translateOrRethrow(ex);
        }
    }

    @Override
    public void writeNullField(String name) throws IOException {
        try {
            generator.writeNullProperty(name);
        } catch (final JacksonException ex) {
            JacksonExceptionTranslator.translateOrRethrow(ex);
        }
    }

    @Override
    public void writeBooleanField(String name, boolean value) throws IOException {
        try {
            generator.writeBooleanProperty(name, value);
        } catch (final JacksonException ex) {
            JacksonExceptionTranslator.translateOrRethrow(ex);
        }
    }

    @Override
    public void writeBoolean(boolean value) throws IOException {
        try {
            generator.writeBoolean(value);
        } catch (final JacksonException ex) {
            JacksonExceptionTranslator.translateOrRethrow(ex);
        }
    }

    @Override
    public void writeNumberField(String name, double value) throws IOException {
        try {
            generator.writeNumberProperty(name, value);
        } catch (final JacksonException ex) {
            JacksonExceptionTranslator.translateOrRethrow(ex);
        }
    }

    @Override
    public void writeNumber(double value) throws IOException {
        try {
            generator.writeNumber(value);
        } catch (final JacksonException ex) {
            JacksonExceptionTranslator.translateOrRethrow(ex);
        }
    }

    @Override
    public void writeNumberField(String name, float value) throws IOException {
        try {
            generator.writeNumberProperty(name, value);
        } catch (final JacksonException ex) {
            JacksonExceptionTranslator.translateOrRethrow(ex);
        }
    }

    @Override
    public void writeNumber(float value) throws IOException {
        try {
            generator.writeNumber(value);
        } catch (final JacksonException ex) {
            JacksonExceptionTranslator.translateOrRethrow(ex);
        }
    }

    @Override
    public void writeNumberField(String name, int value) throws IOException {
        try {
            generator.writeNumberProperty(name, value);
        } catch (final JacksonException ex) {
            JacksonExceptionTranslator.translateOrRethrow(ex);
        }
    }

    @Override
    public void writeNumberField(String name, BigInteger value) throws IOException {
        try {
            // as jackson's JsonGenerator doesn't have this method for BigInteger
            // we have to implement it ourselves
            generator.writeName(name);
            generator.writeNumber(value);
        } catch (final JacksonException ex) {
            JacksonExceptionTranslator.translateOrRethrow(ex);
        }
    }

    @Override
    public void writeNumberField(String name, BigDecimal value) throws IOException {
        try {
            generator.writeNumberProperty(name, value);
        } catch (final JacksonException ex) {
            JacksonExceptionTranslator.translateOrRethrow(ex);
        }
    }

    @Override
    public void writeNumber(int value) throws IOException {
        try {
            generator.writeNumber(value);
        } catch (final JacksonException ex) {
            JacksonExceptionTranslator.translateOrRethrow(ex);
        }
    }

    @Override
    public void writeNumberField(String name, long value) throws IOException {
        try {
            generator.writeNumberProperty(name, value);
        } catch (final JacksonException ex) {
            JacksonExceptionTranslator.translateOrRethrow(ex);
        }
    }

    @Override
    public void writeNumber(long value) throws IOException {
        try {
            generator.writeNumber(value);
        } catch (final JacksonException ex) {
            JacksonExceptionTranslator.translateOrRethrow(ex);
        }
    }

    @Override
    public void writeNumber(short value) throws IOException {
        try {
            generator.writeNumber(value);
        } catch (final JacksonException ex) {
            JacksonExceptionTranslator.translateOrRethrow(ex);
        }
    }

    @Override
    public void writeNumber(BigInteger value) throws IOException {
        try {
            generator.writeNumber(value);
        } catch (final JacksonException ex) {
            JacksonExceptionTranslator.translateOrRethrow(ex);
        }
    }

    @Override
    public void writeNumber(BigDecimal value) throws IOException {
        try {
            generator.writeNumber(value);
        } catch (final JacksonException ex) {
            JacksonExceptionTranslator.translateOrRethrow(ex);
        }
    }

    @Override
    public void writeStringField(String name, String value) throws IOException {
        try {
            generator.writeStringProperty(name, value);
        } catch (final JacksonException ex) {
            JacksonExceptionTranslator.translateOrRethrow(ex);
        }
    }

    @Override
    public void writeString(String value) throws IOException {
        try {
            generator.writeString(value);
        } catch (final JacksonException ex) {
            JacksonExceptionTranslator.translateOrRethrow(ex);
        }
    }

    @Override
    public void writeString(char[] value, int offset, int len) throws IOException {
        try {
            generator.writeString(value, offset, len);
        } catch (final JacksonException ex) {
            JacksonExceptionTranslator.translateOrRethrow(ex);
        }
    }

    @Override
    public void writeUTF8String(byte[] value, int offset, int length) throws IOException {
        try {
            generator.writeUTF8String(value, offset, length);
        } catch (final JacksonException ex) {
            JacksonExceptionTranslator.translateOrRethrow(ex);
        }
    }

    @Override
    public void writeBinaryField(String name, byte[] value) throws IOException {
        try {
            generator.writeBinaryProperty(name, value);
        } catch (final JacksonException ex) {
            JacksonExceptionTranslator.translateOrRethrow(ex);
        }
    }

    @Override
    public void writeBinary(byte[] value) throws IOException {
        try {
            generator.writeBinary(value);
        } catch (final JacksonException ex) {
            JacksonExceptionTranslator.translateOrRethrow(ex);
        }
    }

    @Override
    public void writeBinary(byte[] value, int offset, int len) throws IOException {
        try {
            generator.writeBinary(value, offset, len);
        } catch (final JacksonException ex) {
            JacksonExceptionTranslator.translateOrRethrow(ex);
        }
    }

    private void writeStartRaw(String name) throws IOException {
        try {
            writeFieldName(name);
            generator.writeRaw(':');
        } catch (final JacksonException ex) {
            JacksonExceptionTranslator.translateOrRethrow(ex);
        }
    }

    public void writeEndRaw() {
        assert base != null : "JsonGenerator should be of instance GeneratorBase but was: " + generator.getClass();
        if (base != null) {
            TokenStreamContext context = base.streamWriteContext();
            assert (context instanceof JsonWriteContext) : "Expected an instance of JsonWriteContext but was: " + context.getClass();
            ((JsonWriteContext) context).writeValue();
        }
    }

    @Override
    public void writeRawField(String name, InputStream content) throws IOException {
        if (content.markSupported() == false) {
            // needed for the XContentFactory.xContentType call
            content = new BufferedInputStream(content);
        }
        MediaType contentType = MediaTypeRegistry.xContentType(content);
        if (contentType == null) {
            throw new IllegalArgumentException("Can't write raw bytes whose xcontent-type can't be guessed");
        }
        writeRawField(name, content, contentType);
    }

    /**
     * Writes a raw field with the value taken from the bytes in the stream
     */
    @Override
    public void writeRawField(String name, InputStream content, MediaType mediaType) throws IOException {
        if (mayWriteRawData(mediaType) == false) {
            // EMPTY is safe here because we never call namedObject when writing raw data
            try (
                XContentParser parser = mediaType.xContent()
                    // It's okay to pass the throwing deprecation handler
                    // because we should not be writing raw fields when
                    // generating JSON
                    .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, content)
            ) {
                parser.nextToken();
                writeFieldName(name);
                copyCurrentStructure(parser);
            }
        } else {
            writeStartRaw(name);
            flush();
            Streams.copy(content, os);
            writeEndRaw();
        }
    }

    /**
     * Writes the raw value to the stream
     */
    @Override
    public void writeRawValue(InputStream stream, MediaType mediaType) throws IOException {
        if (mayWriteRawData(mediaType) == false) {
            copyRawValue(stream, mediaType.xContent());
        } else {
            if (generator.streamWriteContext().currentName() != null) {
                // If we've just started a field we'll need to add the separator
                generator.writeRaw(':');
            }
            flush();
            Streams.copy(stream, os, false);
            writeEndRaw();
        }
    }

    /**
     * possibly copy the whole structure to correctly filter
     *
     * @deprecated use {@link #mayWriteRawData(MediaType)} instead
     */
    @Deprecated
    private boolean mayWriteRawData(XContentType contentType) {
        // When the current generator is filtered (ie filter != null)
        // or the content is in a different format than the current generator,
        // we need to copy the whole structure so that it will be correctly
        // filtered or converted
        return supportsRawWrites() && isFiltered() == false && contentType == contentType() && prettyPrint == false;
    }

    /**
     * possibly copy the whole structure to correctly filter
     */
    private boolean mayWriteRawData(MediaType contentType) {
        // When the current generator is filtered (ie filter != null)
        // or the content is in a different format than the current generator,
        // we need to copy the whole structure so that it will be correctly
        // filtered or converted
        return supportsRawWrites() && isFiltered() == false && contentType == contentType() && prettyPrint == false;
    }

    /** Whether this generator supports writing raw data directly */
    protected boolean supportsRawWrites() {
        return true;
    }

    protected void copyRawValue(InputStream stream, XContent xContent) throws IOException {
        // EMPTY is safe here because we never call namedObject
        try (
            XContentParser parser = xContent
                // It's okay to pass the throwing deprecation handler because we
                // should not be writing raw fields when generating JSON
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, stream)
        ) {
            copyCurrentStructure(parser);
        }
    }

    @Override
    public void copyCurrentStructure(XContentParser parser) throws IOException {
        // the start of the parser
        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        try {
            if (parser instanceof JsonXContentParser jsonXContentParser) {
                generator.copyCurrentStructure(jsonXContentParser.parser);
            } else {
                copyCurrentStructure(this, parser);
            }
        } catch (final JacksonException ex) {
            JacksonExceptionTranslator.translateOrRethrow(ex);
        }
    }

    /**
     * Low level implementation detail of {@link XContentGenerator#copyCurrentStructure(XContentParser)}.
     */
    private static void copyCurrentStructure(XContentGenerator destination, XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();

        try {
            // Let's handle field-name separately first
            if (token == XContentParser.Token.FIELD_NAME) {
                destination.writeFieldName(parser.currentName());
                token = parser.nextToken();
                // fall-through to copy the associated value
            }

            switch (token) {
                case START_ARRAY:
                    destination.writeStartArray();
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        copyCurrentStructure(destination, parser);
                    }
                    destination.writeEndArray();
                    break;
                case START_OBJECT:
                    destination.writeStartObject();
                    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                        copyCurrentStructure(destination, parser);
                    }
                    destination.writeEndObject();
                    break;
                default: // others are simple:
                    destination.copyCurrentEvent(parser);
            }
        } catch (final JacksonException ex) {
            JacksonExceptionTranslator.translateOrRethrow(ex);
        }
    }

    @Override
    public void flush() throws IOException {
        generator.flush();
    }

    @Override
    public void close() throws IOException {
        if (generator.isClosed()) {
            return;
        }
        TokenStreamContext context = generator.streamWriteContext();
        if ((context != null) && (context.inRoot() == false)) {
            throw new IOException("Unclosed object or array found");
        }
        if (writeLineFeedAtEnd) {
            flush();
            // Bypass generator to always write the line feed
            getLowLevelGenerator().writeRaw(LF);
        }
        generator.close();
    }

    @Override
    public boolean isClosed() {
        return generator.isClosed();
    }
}
