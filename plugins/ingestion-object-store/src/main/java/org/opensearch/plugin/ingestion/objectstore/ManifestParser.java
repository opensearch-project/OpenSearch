/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Parses the explicit v0 {@code manifest} ingestion-source parameter into an {@link ObjectStoreManifest}.
 * <p>
 * The parameter has to be a JSON string rather than a JSON array, because an ingestion-source param is an index
 * setting and {@code Settings} cannot hold structure: an array of objects is rejected outright by
 * {@code Settings.fromXContent} with {@code "only value lists are allowed in serialized settings"}. The value
 * therefore reaches the plugin as one opaque string and is parsed here.
 * <p>
 * Each entry is {@code {"key": <string>, "size": <long>}} with a repository-relative key. Every shard parses the same
 * string and builds an identical, immutable {@code key -> size} map, which is what lets each shard compute its own
 * slices with no coordination and no repository listing.
 */
final class ManifestParser {

    private static final String KEY_FIELD = "key";
    private static final String SIZE_FIELD = "size";

    private ManifestParser() {}

    /**
     * Parses the manifest JSON into an immutable {@link ObjectStoreManifest}.
     *
     * @param manifestJson the raw {@code manifest} parameter value; a JSON array of {@code {"key","size"}} entries
     * @param maxObjects   the hard cap on manifest cardinality; a larger manifest is rejected
     * @return the parsed, immutable manifest
     * @throws IllegalArgumentException if the JSON is {@code null}, blank, or malformed; if anything follows the closing
     *                                  bracket; if any size is negative; if a key is duplicated; or if the entry count
     *                                  exceeds {@code maxObjects}
     */
    static ObjectStoreManifest parse(final String manifestJson, final long maxObjects) {
        if (manifestJson == null || manifestJson.isBlank()) {
            throw new IllegalArgumentException("[" + ObjectStoreSourceConfig.MANIFEST + "] is required and must not be blank");
        }
        requireWithinByteCap(manifestJson);
        final Map<String, Long> entries = new LinkedHashMap<>();
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                manifestJson
            )
        ) {
            XContentParser.Token token = parser.nextToken();
            if (token != XContentParser.Token.START_ARRAY) {
                throw new IllegalArgumentException(
                    "[" + ObjectStoreSourceConfig.MANIFEST + "] must be a JSON array of {\"key\",\"size\"} entries"
                );
            }
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                if (token != XContentParser.Token.START_OBJECT) {
                    throw new IllegalArgumentException(
                        "[" + ObjectStoreSourceConfig.MANIFEST + "] entries must be JSON objects with [key] and [size]"
                    );
                }
                parseEntry(parser, entries);
                if (entries.size() > maxObjects) {
                    throw new IllegalArgumentException(
                        "["
                            + ObjectStoreSourceConfig.MANIFEST
                            + "] cardinality exceeds ["
                            + ObjectStoreSourceConfig.MAX_OBJECTS
                            + "="
                            + maxObjects
                            + "]"
                    );
                }
            }
            if (parser.nextToken() != null) {
                throw new IllegalArgumentException(
                    "[" + ObjectStoreSourceConfig.MANIFEST + "] must contain a single JSON array and nothing after it"
                );
            }
        } catch (final IOException e) {
            throw new IllegalArgumentException("[" + ObjectStoreSourceConfig.MANIFEST + "] is not valid JSON", e);
        }
        return new ObjectStoreManifest(entries);
    }

    /**
     * Rejects a manifest larger than {@link ObjectStoreSourceConfig#MAX_MANIFEST_BYTES} before it is parsed, since the
     * point of the cap is to avoid building the map for a manifest that is going to be refused.
     *
     * @param manifestJson the raw manifest JSON
     */
    private static void requireWithinByteCap(final String manifestJson) {
        final long bytes = manifestJson.getBytes(StandardCharsets.UTF_8).length;
        if (bytes > ObjectStoreSourceConfig.MAX_MANIFEST_BYTES) {
            throw new IllegalArgumentException(
                "["
                    + ObjectStoreSourceConfig.MANIFEST
                    + "] is ["
                    + bytes
                    + "] bytes, over the ["
                    + ObjectStoreSourceConfig.MAX_MANIFEST_BYTES
                    + "] byte cap; it is carried in the cluster state, so use ["
                    + ObjectStoreSourceConfig.DISCOVERY
                    + "="
                    + Discovery.LIST.getName()
                    + "] for an object set this large"
            );
        }
    }

    private static void parseEntry(final XContentParser parser, final Map<String, Long> entries) throws IOException {
        String key = null;
        Long size = null;
        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (KEY_FIELD.equals(fieldName)) {
                if (token != XContentParser.Token.VALUE_STRING) {
                    throw new IllegalArgumentException(
                        "[" + ObjectStoreSourceConfig.MANIFEST + "] [" + KEY_FIELD + "] must be a JSON string"
                    );
                }
                key = parser.text();
                if (key.isBlank()) {
                    throw new IllegalArgumentException(
                        "[" + ObjectStoreSourceConfig.MANIFEST + "] [" + KEY_FIELD + "] must not be empty or blank"
                    );
                }
            } else if (SIZE_FIELD.equals(fieldName)) {
                if (token != XContentParser.Token.VALUE_NUMBER
                    || (parser.numberType() != XContentParser.NumberType.INT && parser.numberType() != XContentParser.NumberType.LONG)) {
                    throw new IllegalArgumentException(
                        "[" + ObjectStoreSourceConfig.MANIFEST + "] [" + SIZE_FIELD + "] must be a non-negative integer"
                    );
                }
                size = parser.longValue();
            } else {
                parser.skipChildren();
            }
        }
        if (key == null || size == null) {
            throw new IllegalArgumentException(
                "[" + ObjectStoreSourceConfig.MANIFEST + "] entry is missing [" + KEY_FIELD + "] or [" + SIZE_FIELD + "]"
            );
        }
        if (size < 0L) {
            throw new IllegalArgumentException(
                "["
                    + ObjectStoreSourceConfig.MANIFEST
                    + "] size for key ["
                    + LogSanitizer.sanitize(key)
                    + "] must be >= 0 but was ["
                    + size
                    + "]"
            );
        }
        if (entries.containsKey(key)) {
            throw new IllegalArgumentException(
                "[" + ObjectStoreSourceConfig.MANIFEST + "] contains a duplicate key [" + LogSanitizer.sanitize(key) + "]"
            );
        }
        entries.put(key, size);
    }
}
