/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.common;

import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Processor;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.opensearch.ingest.ConfigurationUtils.newConfigurationException;

/**
 * Processor that splits a URI string into its parts and stores them as an object.
 * <p>
 * The input is parsed with {@link URI}, so it must follow RFC 2396 and, apart from relative references, it must be hierarchical. That is
 * strict: characters such as a space, {@code |}, <code>&#123;</code>, <code>&#125;</code> or {@code ^} must be percent-encoded,
 * {@code [} and {@code ]} are only accepted in the query, a host name may only contain letters, digits and hyphens (no underscore, no non-ASCII characters) and its
 * last label must not start with a digit. Parts that are absent or empty are left out of the result. The path, query, fragment and user
 * info are reported exactly as written, percent-encoding included. No network access is performed, and the input is not repeated in
 * error messages because it may contain credentials.
 */
public final class UriPartsProcessor extends AbstractProcessor {

    public static final String TYPE = "uri_parts";

    static final String DEFAULT_TARGET_FIELD = "url";

    private static final int MAX_PORT = 65535;

    private final String field;
    private final String targetField;
    private final boolean keepOriginal;
    private final boolean removeIfSuccessful;
    private final boolean ignoreMissing;

    UriPartsProcessor(
        String tag,
        String description,
        String field,
        String targetField,
        boolean keepOriginal,
        boolean removeIfSuccessful,
        boolean ignoreMissing
    ) {
        super(tag, description);
        if (removeIfSuccessful && (field.equals(targetField) || field.startsWith(targetField + "."))) {
            throw new IllegalArgumentException(
                "[remove_if_successful] cannot be true when [field] ["
                    + field
                    + "] is the same as, or nested in, [target_field] ["
                    + targetField
                    + "], because the parsed output would be removed"
            );
        }
        this.field = field;
        this.targetField = targetField;
        this.keepOriginal = keepOriginal;
        this.removeIfSuccessful = removeIfSuccessful;
        this.ignoreMissing = ignoreMissing;
    }

    String getField() {
        return field;
    }

    String getTargetField() {
        return targetField;
    }

    boolean isKeepOriginal() {
        return keepOriginal;
    }

    boolean isRemoveIfSuccessful() {
        return removeIfSuccessful;
    }

    boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    @Override
    public IngestDocument execute(IngestDocument document) {
        String input = document.getFieldValue(field, String.class, ignoreMissing);
        if (input == null) {
            if (ignoreMissing) {
                return document;
            }
            throw new IllegalArgumentException("field [" + field + "] is null, cannot parse it as a URI");
        }
        if (input.isEmpty()) {
            throw new IllegalArgumentException("field [" + field + "] is empty, cannot parse it as a URI");
        }

        // The input is parsed before the document is modified, so an invalid value leaves the document as it was.
        Map<String, Object> parts = parse(input);
        document.setFieldValue(targetField, parts);
        if (removeIfSuccessful) {
            document.removeField(field);
        }
        return document;
    }

    private Map<String, Object> parse(String input) {
        final URI uri;
        try {
            // parseServerAuthority() makes an authority that is not a valid host[:port], such as one with an underscore in the host name,
            // an error instead of silently dropping the host.
            uri = new URI(input).parseServerAuthority();
        } catch (URISyntaxException e) {
            // The syntax exception is deliberately not chained as the cause: on_failure exposes only the message of the root cause, which
            // would then lack the field name. Its own message is not reused either, because it repeats the whole input, and a URI can
            // carry credentials or tokens that must not end up in error responses and logs.
            throw new IllegalArgumentException(
                "unable to parse field [" + field + "] as a URI: " + e.getReason() + (e.getIndex() >= 0 ? " at index " + e.getIndex() : "")
            );
        }
        if (uri.isOpaque()) {
            throw new IllegalArgumentException(
                "unable to parse field ["
                    + field
                    + "] as a URI: it is an opaque URI (its scheme is not followed by '/'), only hierarchical URIs are supported"
            );
        }
        if (uri.getPort() > MAX_PORT) {
            throw new IllegalArgumentException(
                "unable to parse field ["
                    + field
                    + "] as a URI: port ["
                    + uri.getPort()
                    + "] is out of range, it must be at most "
                    + MAX_PORT
            );
        }

        Map<String, Object> parts = new LinkedHashMap<>();
        if (keepOriginal) {
            parts.put("original", input);
        }
        putIfNotEmpty(parts, "scheme", uri.getScheme());
        // For an IPv6 literal the host keeps its brackets, as it is written in the URI.
        putIfNotEmpty(parts, "domain", uri.getHost());
        if (uri.getPort() != -1) {
            parts.put("port", uri.getPort());
        }
        // The raw accessors are used on purpose: the decoding accessors would turn %2F, %26 or %3D into characters that change the
        // structure of the path or query.
        String path = uri.getRawPath();
        putIfNotEmpty(parts, "path", path);
        putIfNotEmpty(parts, "extension", extension(path));
        putIfNotEmpty(parts, "query", uri.getRawQuery());
        putIfNotEmpty(parts, "fragment", uri.getRawFragment());
        String userInfo = uri.getRawUserInfo();
        if (userInfo != null && userInfo.isEmpty() == false) {
            parts.put("user_info", userInfo);
            int colon = userInfo.indexOf(':');
            putIfNotEmpty(parts, "username", colon < 0 ? userInfo : userInfo.substring(0, colon));
            putIfNotEmpty(parts, "password", colon < 0 ? null : userInfo.substring(colon + 1));
        }
        return parts;
    }

    /**
     * Returns the extension of the last segment of the path, or {@code null} if there is none. Path parameters ({@code ;name=value}) are
     * not part of the file name. A segment has no extension when it is empty (the path ends with a slash), when its only dot is the first
     * character (a dotfile such as {@code .htaccess}) or when its last dot is the last character.
     */
    private static String extension(String path) {
        if (path == null) {
            return null;
        }
        String segment = path.substring(path.lastIndexOf('/') + 1);
        int parameters = segment.indexOf(';');
        if (parameters >= 0) {
            segment = segment.substring(0, parameters);
        }
        int dot = segment.lastIndexOf('.');
        if (dot <= 0 || dot == segment.length() - 1) {
            return null;
        }
        return segment.substring(dot + 1);
    }

    private static void putIfNotEmpty(Map<String, Object> parts, String key, String value) {
        if (value != null && value.isEmpty() == false) {
            parts.put(key, value);
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    /**
     * Factory for {@link UriPartsProcessor}.
     */
    public static final class Factory implements Processor.Factory {

        @Override
        public UriPartsProcessor create(Map<String, Processor.Factory> registry, String tag, String description, Map<String, Object> config)
            throws Exception {
            String field = readNonEmptyString(tag, config, "field", null);
            String targetField = readNonEmptyString(tag, config, "target_field", DEFAULT_TARGET_FIELD);
            boolean keepOriginal = ConfigurationUtils.readBooleanProperty(TYPE, tag, config, "keep_original", true);
            boolean removeIfSuccessful = ConfigurationUtils.readBooleanProperty(TYPE, tag, config, "remove_if_successful", false);
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, tag, config, "ignore_missing", false);
            return new UriPartsProcessor(tag, description, field, targetField, keepOriginal, removeIfSuccessful, ignoreMissing);
        }

        private static String readNonEmptyString(String tag, Map<String, Object> config, String property, String defaultValue) {
            String value = ConfigurationUtils.readStringProperty(TYPE, tag, config, property, defaultValue);
            if (value.isEmpty()) {
                throw newConfigurationException(TYPE, tag, property, "cannot be empty");
            }
            return value;
        }
    }
}
