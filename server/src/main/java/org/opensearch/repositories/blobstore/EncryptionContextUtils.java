/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.blobstore;

import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.Nullable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Utility class for encryption context operations.
 * Provides methods for format conversion, merging contexts,
 * and Base64 encoding for S3 operations.
 */
public class EncryptionContextUtils {

    /**
     * Merges index-level and repository-level encryption contexts, converts to JSON format if needed,
     * and Base64 encodes for S3.
     * <p>
     * Merging strategy: Repository context provides baseline, index context keys override on conflict.
     * This ensures KMS grants created with combined context work correctly.
     *
     * @param indexEncContext Index-level encryption context - can be cryptofs or JSON format
     * @param repoEncContext  Repository-level encryption context - already Base64 encoded JSON
     * @return Base64 encoded merged JSON encryption context, or null if both are null
     */
    public static String mergeAndEncodeEncryptionContexts(@Nullable String indexEncContext, @Nullable String repoEncContext) {
        // If both null, return null
        if ((indexEncContext == null || indexEncContext.isEmpty()) && (repoEncContext == null || repoEncContext.isEmpty())) {
            return null;
        }

        // Parse index context to JSON
        String indexJson = null;
        if (indexEncContext != null && !indexEncContext.isEmpty()) {
            String trimmed = indexEncContext.trim();
            if (trimmed.startsWith("{")) {
                indexJson = trimmed;
            } else {
                indexJson = cryptofsToJson(trimmed);
            }
        }

        // Decode repository context from Base64
        String repoJson = null;
        if (repoEncContext != null && !repoEncContext.isEmpty()) {
            try {
                byte[] decoded = Base64.getDecoder().decode(repoEncContext);
                repoJson = new String(decoded, StandardCharsets.UTF_8);
            } catch (IllegalArgumentException e) {
                repoJson = repoEncContext;
            }
        }

        String mergedJson = mergeJson(repoJson, indexJson);

        // Base64 encode the merged result
        return Base64.getEncoder().encodeToString(mergedJson.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Merges index-level and repository-level CryptoMetadata.
     * Priority: Index key greater than Repository key
     * Context: Index context merged with repository context (EncA + EncB)
     *
     * @param indexMetadata            The index-level CryptoMetadata
     * @param repositoryCryptoMetadata The repository-level CryptoMetadata
     * @return Merged CryptoMetadata with combined key and context
     */
    public static CryptoMetadata mergeCryptoMetadata(CryptoMetadata indexMetadata, CryptoMetadata repositoryCryptoMetadata) {
        if (indexMetadata == null && repositoryCryptoMetadata == null) {
            return null;
        }
        if (indexMetadata == null) {
            return repositoryCryptoMetadata;
        }
        if (repositoryCryptoMetadata == null) {
            return indexMetadata;
        }

        // Use index key provider if present, else repository
        String keyProviderName = (indexMetadata.keyProviderName() != null)
            ? indexMetadata.keyProviderName()
            : repositoryCryptoMetadata.keyProviderName();

        String keyProviderType = (indexMetadata.keyProviderType() != null)
            ? indexMetadata.keyProviderType()
            : repositoryCryptoMetadata.keyProviderType();

        // Merge settings: start with repo, overlay index settings
        Settings.Builder settingsBuilder = Settings.builder().put(repositoryCryptoMetadata.settings()).put(indexMetadata.settings());

        // Merge encryption contexts from settings
        String indexCtx = indexMetadata.settings().get("kms.encryption_context");
        String repoCtx = repositoryCryptoMetadata.settings().get("kms.encryption_context");

        if (indexCtx != null && !indexCtx.isEmpty()) {
            // Convert cryptofs format to JSON if needed
            String indexJson = indexCtx.trim().startsWith("{") ? indexCtx : cryptofsToJson(indexCtx);

            if (repoCtx != null && !repoCtx.isEmpty()) {
                String repoJson = repoCtx.trim().startsWith("{") ? repoCtx : cryptofsToJson(repoCtx);
                String mergedJson = mergeJson(repoJson, indexJson);
                String mergedCryptofs = jsonToCryptofs(mergedJson);
                settingsBuilder.put("kms.encryption_context", mergedCryptofs);
            } else {
                settingsBuilder.put("kms.encryption_context", indexCtx);  // Keep original format
            }
        } else if (repoCtx != null && !repoCtx.isEmpty()) {
            settingsBuilder.put("kms.encryption_context", repoCtx);  // Keep original format
        }

        return new CryptoMetadata(keyProviderName, keyProviderType, settingsBuilder.build());
    }

    /**
     * Converts cryptofs format to JSON.
     * Input: "key1=value1,key2=value2"
     * Output: {"key1":"value1","key2":"value2"}
     */
    public static String cryptofsToJson(String cryptofs) {
        if (cryptofs == null || cryptofs.isEmpty()) {
            return "{}";
        }

        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();

            for (String pair : cryptofs.split(",")) {
                String[] kv = pair.trim().split("=", 2);
                if (kv.length == 2) {
                    builder.field(kv[0].trim(), kv[1].trim());
                }
            }

            builder.endObject();
            return builder.toString();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to convert cryptofs format to JSON", e);
        }
    }

    /**
     * Converts JSON format back to cryptofs format.
     * Input: {"key1":"value1","key2":"value2"}
     * Output: "key1=value1,key2=value2"
     *
     * <p><b>IMPORTANT LIMITATION:</b> This format uses comma (,) as a delimiter between
     * key-value pairs and equals (=) as a delimiter between keys and values. Therefore,
     * keys and values <b>MUST NOT contain</b> these characters, or data corruption will occur
     * when converting back to JSON.</p>
     */
    public static String jsonToCryptofs(String json) {
        if (json == null || json.isEmpty()) {
            return "";
        }

        try {
            Map<String, String> map = parseJsonToMap(json);

            StringBuilder sb = new StringBuilder();
            boolean first = true;
            for (Map.Entry<String, String> e : map.entrySet()) {
                if (!first) sb.append(",");
                sb.append(e.getKey()).append("=").append(e.getValue());
                first = false;
            }
            return sb.toString();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to parse JSON for cryptofs conversion", e);
        }
    }

    /**
     * Merges two JSON strings.
     * Base provides baseline, override takes precedence on conflicts.
     *
     * @param base Base context in JSON format
     * @param override Override context in JSON format
     * @return Merged JSON string
     */
    public static String mergeJson(String base, String override) {
        if (base == null || base.isEmpty()) {
            return override != null ? override : "{}";
        }
        if (override == null || override.isEmpty()) {
            return base;
        }

        try {
            Map<String, String> map = new LinkedHashMap<>();
            map.putAll(parseJsonToMap(base));
            map.putAll(parseJsonToMap(override));

            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();

            for (Map.Entry<String, String> e : map.entrySet()) {
                builder.field(e.getKey(), e.getValue());
            }

            builder.endObject();
            return builder.toString();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to merge JSON contexts", e);
        }
    }

    /**
     * Simple JSON parser for string-string maps.
     * Parses JSON format into Map using XContentParser for robust handling.
     * This method is kept for backward compatibility with existing tests and code.
     *
     * @param json JSON string to parse
     * @param map Map to populate with parsed key-value pairs
     */
    public static void parseSimpleJson(String json, Map<String, String> map) {
        try {
            map.putAll(parseJsonToMap(json));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to parse JSON", e);
        }
    }

    /**
     * Parses JSON string into a map using XContentParser for robust parsing.
     * Handles all JSON escape sequences correctly including quotes, backslashes, and control characters.
     *
     * @param json JSON string to parse
     * @return Map of key-value pairs
     * @throws IOException if JSON parsing fails
     */
    private static Map<String, String> parseJsonToMap(String json) throws IOException {
        if (json == null || json.trim().isEmpty()) {
            return new LinkedHashMap<>();
        }

        Map<String, String> map = new LinkedHashMap<>();

        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json)
        ) {

            XContentParser.Token token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("Expected JSON object, got: " + token);
            }

            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String key = parser.currentName();
                    parser.nextToken(); // Move to value
                    String value = parser.text();
                    map.put(key, value);
                }
            }
        }

        return map;
    }

}
