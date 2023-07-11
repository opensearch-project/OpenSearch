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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.common.xcontent;

import org.opensearch.OpenSearchParseException;
import org.opensearch.common.Strings;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.compress.Compressor;
import org.opensearch.common.compress.CompressorFactory;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContent.Params;
import org.opensearch.core.xcontent.XContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParseException;
import org.opensearch.core.xcontent.XContentParser;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Helper for xcontent utilities.
 *
 * @opensearch.internal
 */
@SuppressWarnings("unchecked")
public class XContentHelper {

    /**
     * Creates a parser based on the bytes provided
     * @deprecated use {@link #createParser(NamedXContentRegistry, DeprecationHandler, BytesReference, MediaType)}
     * to avoid content type auto-detection
     */
    @Deprecated
    public static XContentParser createParser(
        NamedXContentRegistry xContentRegistry,
        DeprecationHandler deprecationHandler,
        BytesReference bytes
    ) throws IOException {
        Compressor compressor = CompressorFactory.compressor(bytes);
        if (compressor != null) {
            InputStream compressedInput = null;
            try {
                compressedInput = compressor.threadLocalInputStream(bytes.streamInput());
                if (compressedInput.markSupported() == false) {
                    compressedInput = new BufferedInputStream(compressedInput);
                }
                final MediaType contentType = MediaTypeRegistry.xContentType(compressedInput);
                return contentType.xContent().createParser(xContentRegistry, deprecationHandler, compressedInput);
            } catch (Exception e) {
                if (compressedInput != null) compressedInput.close();
                throw e;
            }
        } else {
            return MediaTypeRegistry.xContentType(bytes).xContent().createParser(xContentRegistry, deprecationHandler, bytes.streamInput());
        }
    }

    /**
     * Creates a parser for the bytes using the supplied content-type
     */
    public static XContentParser createParser(
        NamedXContentRegistry xContentRegistry,
        DeprecationHandler deprecationHandler,
        BytesReference bytes,
        MediaType mediaType
    ) throws IOException {
        Objects.requireNonNull(mediaType);
        Compressor compressor = CompressorFactory.compressor(bytes);
        if (compressor != null) {
            InputStream compressedInput = null;
            try {
                compressedInput = compressor.threadLocalInputStream(bytes.streamInput());
                if (compressedInput.markSupported() == false) {
                    compressedInput = new BufferedInputStream(compressedInput);
                }
                return mediaType.xContent().createParser(xContentRegistry, deprecationHandler, compressedInput);
            } catch (Exception e) {
                if (compressedInput != null) compressedInput.close();
                throw e;
            }
        } else {
            if (bytes instanceof BytesArray) {
                final BytesArray array = (BytesArray) bytes;
                return mediaType.xContent()
                    .createParser(xContentRegistry, deprecationHandler, array.array(), array.offset(), array.length());
            }
            return mediaType.xContent().createParser(xContentRegistry, deprecationHandler, bytes.streamInput());
        }
    }

    /**
     * Converts the given bytes into a map that is optionally ordered.
     * @deprecated this method relies on auto-detection of content type. Use {@link #convertToMap(BytesReference, boolean, MediaType)}
     *             instead with the proper {@link XContentType}
     */
    @Deprecated
    public static Tuple<XContentType, Map<String, Object>> convertToMap(BytesReference bytes, boolean ordered)
        throws OpenSearchParseException {
        return convertToMap(bytes, ordered, null);
    }

    /**
     * Converts the given bytes into a map that is optionally ordered. The provided {@link XContentType} must be non-null.
     *
     * @deprecated use {@link #convertToMap(BytesReference, boolean, MediaType)} instead
     */
    @Deprecated
    public static Tuple<XContentType, Map<String, Object>> convertToMap(BytesReference bytes, boolean ordered, XContentType xContentType) {
        if (Objects.isNull(xContentType) == false && xContentType instanceof XContentType == false) {
            throw new IllegalArgumentException(
                "XContentHelper.convertToMap does not support media type [" + xContentType.getClass().getName() + "]"
            );
        }
        return (Tuple<XContentType, Map<String, Object>>) convertToMap(bytes, ordered, (MediaType) xContentType);
    }

    /**
     * Converts the given bytes into a map that is optionally ordered. The provided {@link XContentType} must be non-null.
     */
    public static Tuple<? extends MediaType, Map<String, Object>> convertToMap(
        BytesReference bytes,
        boolean ordered,
        MediaType xContentType
    ) throws OpenSearchParseException {
        try {
            final MediaType contentType;
            InputStream input;
            Compressor compressor = CompressorFactory.compressor(bytes);
            if (compressor != null) {
                InputStream compressedStreamInput = compressor.threadLocalInputStream(bytes.streamInput());
                if (compressedStreamInput.markSupported() == false) {
                    compressedStreamInput = new BufferedInputStream(compressedStreamInput);
                }
                input = compressedStreamInput;
            } else if (bytes instanceof BytesArray) {
                final BytesArray arr = (BytesArray) bytes;
                final byte[] raw = arr.array();
                final int offset = arr.offset();
                final int length = arr.length();
                contentType = xContentType != null ? xContentType : MediaTypeRegistry.mediaTypeFromBytes(raw, offset, length);
                return new Tuple<>(Objects.requireNonNull(contentType), convertToMap(contentType.xContent(), raw, offset, length, ordered));
            } else {
                input = bytes.streamInput();
            }
            try (InputStream stream = input) {
                contentType = xContentType != null ? xContentType : MediaTypeRegistry.xContentType(stream);
                return new Tuple<>(Objects.requireNonNull(contentType), convertToMap(contentType.xContent(), stream, ordered));
            }
        } catch (IOException e) {
            throw new OpenSearchParseException("Failed to parse content to map", e);
        }
    }

    /**
     * Convert a string in some {@link XContent} format to a {@link Map}. Throws an {@link OpenSearchParseException} if there is any
     * error.
     */
    public static Map<String, Object> convertToMap(XContent xContent, String string, boolean ordered) throws OpenSearchParseException {
        // It is safe to use EMPTY here because this never uses namedObject
        try (
            XContentParser parser = xContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                string
            )
        ) {
            return ordered ? parser.mapOrdered() : parser.map();
        } catch (IOException e) {
            throw new OpenSearchParseException("Failed to parse content to map", e);
        }
    }

    /**
     * Convert a string in some {@link XContent} format to a {@link Map}. Throws an {@link OpenSearchParseException} if there is any
     * error. Note that unlike {@link #convertToMap(BytesReference, boolean)}, this doesn't automatically uncompress the input.
     */
    public static Map<String, Object> convertToMap(XContent xContent, InputStream input, boolean ordered) throws OpenSearchParseException {
        // It is safe to use EMPTY here because this never uses namedObject
        try (
            XContentParser parser = xContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                input
            )
        ) {
            return ordered ? parser.mapOrdered() : parser.map();
        } catch (IOException e) {
            throw new OpenSearchParseException("Failed to parse content to map", e);
        }
    }

    /**
     * Convert a byte array in some {@link XContent} format to a {@link Map}. Throws an {@link OpenSearchParseException} if there is any
     * error. Note that unlike {@link #convertToMap(BytesReference, boolean)}, this doesn't automatically uncompress the input.
     */
    public static Map<String, Object> convertToMap(XContent xContent, byte[] bytes, int offset, int length, boolean ordered)
        throws OpenSearchParseException {
        // It is safe to use EMPTY here because this never uses namedObject
        try (
            XContentParser parser = xContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                bytes,
                offset,
                length
            )
        ) {
            return ordered ? parser.mapOrdered() : parser.map();
        } catch (IOException e) {
            throw new OpenSearchParseException("Failed to parse content to map", e);
        }
    }

    @Deprecated
    public static String convertToJson(BytesReference bytes, boolean reformatJson) throws IOException {
        return convertToJson(bytes, reformatJson, false);
    }

    @Deprecated
    public static String convertToJson(BytesReference bytes, boolean reformatJson, boolean prettyPrint) throws IOException {
        return convertToJson(bytes, reformatJson, prettyPrint, MediaTypeRegistry.xContent(bytes.toBytesRef().bytes));
    }

    public static String convertToJson(BytesReference bytes, boolean reformatJson, MediaType xContentType) throws IOException {
        return convertToJson(bytes, reformatJson, false, xContentType);
    }

    /**
     * Accepts a JSON string, parses it and prints it without pretty-printing it. This is useful
     * where a piece of JSON is formatted for legibility, but needs to be stripped of unnecessary
     * whitespace e.g. for comparison in a test.
     *
     * @param json the JSON to format
     * @return reformatted JSON
     * @throws IOException if the reformatting fails, e.g. because the JSON is not well-formed
     */
    public static String stripWhitespace(String json) throws IOException {
        return convertToJson(new BytesArray(json), true, XContentType.JSON);
    }

    /**
     * Converts the XContentType to a json string
     *
     * @deprecated use {@link #convertToJson(BytesReference, boolean, boolean, MediaType)} instead
     */
    @Deprecated
    public static String convertToJson(BytesReference bytes, boolean reformatJson, boolean prettyPrint, XContentType xContentType)
        throws IOException {
        return convertToJson(bytes, reformatJson, prettyPrint, (MediaType) xContentType);
    }

    /**
     * Converts the given {@link MediaType} to a json string
     */
    public static String convertToJson(BytesReference bytes, boolean reformatJson, boolean prettyPrint, MediaType mediaType)
        throws IOException {
        Objects.requireNonNull(mediaType);
        if (mediaType == XContentType.JSON && !reformatJson) {
            return bytes.utf8ToString();
        }

        // It is safe to use EMPTY here because this never uses namedObject
        if (bytes instanceof BytesArray) {
            final BytesArray array = (BytesArray) bytes;
            try (
                XContentParser parser = mediaType.xContent()
                    .createParser(
                        NamedXContentRegistry.EMPTY,
                        DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                        array.array(),
                        array.offset(),
                        array.length()
                    )
            ) {
                return toJsonString(prettyPrint, parser);
            }
        } else {
            try (
                InputStream stream = bytes.streamInput();
                XContentParser parser = mediaType.xContent()
                    .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, stream)
            ) {
                return toJsonString(prettyPrint, parser);
            }
        }
    }

    private static String toJsonString(boolean prettyPrint, XContentParser parser) throws IOException {
        parser.nextToken();
        XContentBuilder builder = XContentFactory.jsonBuilder();
        if (prettyPrint) {
            builder.prettyPrint();
        }
        builder.copyCurrentStructure(parser);
        return Strings.toString(builder);
    }

    /**
     * Updates the provided changes into the source. If the key exists in the changes, it overrides the one in source
     * unless both are Maps, in which case it recursively updated it.
     *
     * @param source                 the original map to be updated
     * @param changes                the changes to update into updated
     * @param checkUpdatesAreUnequal should this method check if updates to the same key (that are not both maps) are
     *                               unequal?  This is just a .equals check on the objects, but that can take some time on long strings.
     * @return true if the source map was modified
     */
    public static boolean update(Map<String, Object> source, Map<String, Object> changes, boolean checkUpdatesAreUnequal) {
        boolean modified = false;
        for (Map.Entry<String, Object> changesEntry : changes.entrySet()) {
            if (!source.containsKey(changesEntry.getKey())) {
                // safe to copy, change does not exist in source
                source.put(changesEntry.getKey(), changesEntry.getValue());
                modified = true;
                continue;
            }
            Object old = source.get(changesEntry.getKey());
            if (old instanceof Map && changesEntry.getValue() instanceof Map) {
                // recursive merge maps
                modified |= update(
                    (Map<String, Object>) source.get(changesEntry.getKey()),
                    (Map<String, Object>) changesEntry.getValue(),
                    checkUpdatesAreUnequal && !modified
                );
                continue;
            }
            // update the field
            source.put(changesEntry.getKey(), changesEntry.getValue());
            if (modified) {
                continue;
            }
            if (!checkUpdatesAreUnequal) {
                modified = true;
                continue;
            }
            modified = !Objects.equals(old, changesEntry.getValue());
        }
        return modified;
    }

    /**
     * Merges the defaults provided as the second parameter into the content of the first. Only does recursive merge
     * for inner maps.
     */
    public static void mergeDefaults(Map<String, Object> content, Map<String, Object> defaults) {
        for (Map.Entry<String, Object> defaultEntry : defaults.entrySet()) {
            if (!content.containsKey(defaultEntry.getKey())) {
                // copy it over, it does not exists in the content
                content.put(defaultEntry.getKey(), defaultEntry.getValue());
            } else {
                // in the content and in the default, only merge compound ones (maps)
                if (content.get(defaultEntry.getKey()) instanceof Map && defaultEntry.getValue() instanceof Map) {
                    mergeDefaults((Map<String, Object>) content.get(defaultEntry.getKey()), (Map<String, Object>) defaultEntry.getValue());
                } else if (content.get(defaultEntry.getKey()) instanceof List && defaultEntry.getValue() instanceof List) {
                    List defaultList = (List) defaultEntry.getValue();
                    List contentList = (List) content.get(defaultEntry.getKey());

                    List mergedList = new ArrayList();
                    if (allListValuesAreMapsOfOne(defaultList) && allListValuesAreMapsOfOne(contentList)) {
                        // all are in the form of [ {"key1" : {}}, {"key2" : {}} ], merge based on keys
                        Map<String, Map<String, Object>> processed = new LinkedHashMap<>();
                        for (Object o : contentList) {
                            Map<String, Object> map = (Map<String, Object>) o;
                            Map.Entry<String, Object> entry = map.entrySet().iterator().next();
                            processed.put(entry.getKey(), map);
                        }
                        for (Object o : defaultList) {
                            Map<String, Object> map = (Map<String, Object>) o;
                            Map.Entry<String, Object> entry = map.entrySet().iterator().next();
                            if (processed.containsKey(entry.getKey())) {
                                mergeDefaults(processed.get(entry.getKey()), map);
                            } else {
                                // put the default entries after the content ones.
                                processed.put(entry.getKey(), map);
                            }
                        }
                        for (Map<String, Object> map : processed.values()) {
                            mergedList.add(map);
                        }
                    } else {
                        // if both are lists, simply combine them, first the defaults, then the content
                        // just make sure not to add the same value twice
                        mergedList.addAll(defaultList);
                        for (Object o : contentList) {
                            if (!mergedList.contains(o)) {
                                mergedList.add(o);
                            }
                        }
                    }
                    content.put(defaultEntry.getKey(), mergedList);
                }
            }
        }
    }

    private static boolean allListValuesAreMapsOfOne(List list) {
        for (Object o : list) {
            if (!(o instanceof Map)) {
                return false;
            }
            if (((Map) o).size() != 1) {
                return false;
            }
        }
        return true;
    }

    /**
     * Writes a "raw" (bytes) field, handling cases where the bytes are compressed, and tries to optimize writing using
     * {@link XContentBuilder#rawField(String, InputStream)}.
     * @deprecated use {@link #writeRawField(String, BytesReference, XContentType, XContentBuilder, Params)} to avoid content type
     * auto-detection
     */
    @Deprecated
    public static void writeRawField(String field, BytesReference source, XContentBuilder builder, Params params) throws IOException {
        Compressor compressor = CompressorFactory.compressor(source);
        if (compressor != null) {
            try (InputStream compressedStreamInput = compressor.threadLocalInputStream(source.streamInput())) {
                builder.rawField(field, compressedStreamInput);
            }
        } else {
            try (InputStream stream = source.streamInput()) {
                builder.rawField(field, stream);
            }
        }
    }

    /**
     * Writes a "raw" (bytes) field, handling cases where the bytes are compressed, and tries to optimize writing using
     * {@link XContentBuilder#rawField(String, InputStream, MediaType)}.
     */
    public static void writeRawField(String field, BytesReference source, XContentType xContentType, XContentBuilder builder, Params params)
        throws IOException {
        Objects.requireNonNull(xContentType);
        Compressor compressor = CompressorFactory.compressor(source);
        if (compressor != null) {
            try (InputStream compressedStreamInput = compressor.threadLocalInputStream(source.streamInput())) {
                builder.rawField(field, compressedStreamInput, xContentType);
            }
        } else {
            try (InputStream stream = source.streamInput()) {
                builder.rawField(field, stream, xContentType);
            }
        }
    }

    /**
     * Returns the bytes that represent the XContent output of the provided {@link ToXContent} object, using the provided
     * {@link XContentType}. Wraps the output into a new anonymous object according to the value returned
     * by the {@link ToXContent#isFragment()} method returns.
     */
    @Deprecated
    public static BytesReference toXContent(ToXContent toXContent, XContentType xContentType, boolean humanReadable) throws IOException {
        return toXContent(toXContent, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);
    }

    /**
     * Returns the bytes that represent the XContent output of the provided {@link ToXContent} object, using the provided
     * {@link XContentType}. Wraps the output into a new anonymous object according to the value returned
     * by the {@link ToXContent#isFragment()} method returns.
     *
     * @deprecated use {@link #toXContent(ToXContent, MediaType, Params, boolean)} instead
     */
    @Deprecated
    public static BytesReference toXContent(ToXContent toXContent, XContentType xContentType, Params params, boolean humanReadable)
        throws IOException {
        try (XContentBuilder builder = XContentBuilder.builder(xContentType.xContent())) {
            builder.humanReadable(humanReadable);
            if (toXContent.isFragment()) {
                builder.startObject();
            }
            toXContent.toXContent(builder, params);
            if (toXContent.isFragment()) {
                builder.endObject();
            }
            return BytesReference.bytes(builder);
        }
    }

    /**
     * Returns the bytes that represent the XContent output of the provided {@link ToXContent} object, using the provided
     * {@link XContentType}. Wraps the output into a new anonymous object according to the value returned
     * by the {@link ToXContent#isFragment()} method returns.
     */
    public static BytesReference toXContent(ToXContent toXContent, MediaType mediaType, Params params, boolean humanReadable)
        throws IOException {
        try (XContentBuilder builder = XContentBuilder.builder(mediaType.xContent())) {
            builder.humanReadable(humanReadable);
            if (toXContent.isFragment()) {
                builder.startObject();
            }
            toXContent.toXContent(builder, params);
            if (toXContent.isFragment()) {
                builder.endObject();
            }
            return BytesReference.bytes(builder);
        }
    }

    /**
     * Returns the contents of an object as an unparsed BytesReference
     *
     * This is useful for things like mappings where we're copying bytes around but don't
     * actually need to parse their contents, and so avoids building large maps of maps
     * unnecessarily
     */
    public static BytesReference childBytes(XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new XContentParseException(
                    parser.getTokenLocation(),
                    "Expected [START_OBJECT] but got [" + parser.currentToken() + "]"
                );
            }
        }
        XContentBuilder builder = XContentBuilder.builder(parser.contentType().xContent());
        builder.copyCurrentStructure(parser);
        return BytesReference.bytes(builder);
    }
}
