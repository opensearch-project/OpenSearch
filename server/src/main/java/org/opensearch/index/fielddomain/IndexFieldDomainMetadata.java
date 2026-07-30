/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.fielddomain;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.annotation.ExperimentalApi;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Codec for the {@code index_field_domains} index custom metadata.
 *
 * The metadata is stored as a flat {@code Map<String, String>} under {@link IndexMetadata#getCustomData(String)}.
 * This class is the single place that translates between that wire shape and typed {@link FieldDomain} objects.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class IndexFieldDomainMetadata {
    /**
     * Custom metadata key used on {@link IndexMetadata}.
     */
    public static final String CUSTOM_KEY = "index_field_domains";

    private static final IndexFieldDomainMetadata INSTANCE = new IndexFieldDomainMetadata();

    private static final String FIELDS_PREFIX = "fields.";
    private static final String KEY_TYPE = "type";

    private final FieldDomainParserRegistry parserRegistry;

    private IndexFieldDomainMetadata() {
        this(FieldDomainParserRegistry.defaultRegistry());
    }

    IndexFieldDomainMetadata(FieldDomainParserRegistry parserRegistry) {
        this.parserRegistry = Objects.requireNonNull(parserRegistry, "parserRegistry must not be null");
    }

    /**
     * Returns the built-in metadata codec.
     */
    public static IndexFieldDomainMetadata getInstance() {
        return INSTANCE;
    }

    /**
     * Parses field domains for a single field from index custom metadata.
     *
     * This method is intentionally lenient for search-time consumers: missing, malformed, or unsupported metadata for
     * the requested field returns {@link Optional#empty()} so callers can fall back to conservative behavior.
     *
     * @param customData custom metadata map stored under {@link #CUSTOM_KEY}
     * @param field field name to parse
     * @return parsed domain when metadata is present, complete, and supported; otherwise empty
     */
    public Optional<FieldDomain> fromCustomData(Map<String, String> customData, String field) {
        if (customData == null || customData.isEmpty() || field == null || field.isEmpty()) {
            return Optional.empty();
        }

        String prefix = fieldPrefix(field);
        String type = customData.get(prefix + KEY_TYPE);
        if (type == null || type.isEmpty()) {
            return Optional.empty();
        }

        return parserRegistry.fromCustomData(type, field, customData, prefix);
    }

    /**
     * Encodes one field-domain object into a custom metadata map containing only that field.
     */
    public Map<String, String> toCustomData(FieldDomain domain) {
        Objects.requireNonNull(domain, "domain must not be null");

        Map<String, String> customData = new HashMap<>();
        String prefix = fieldPrefix(domain.field());
        customData.put(prefix + KEY_TYPE, domain.type());
        parserRegistry.writeToCustomData(domain, customData, prefix);
        return Map.copyOf(customData);
    }

    /**
     * Returns updated index metadata with the supplied field domain inserted under {@link #CUSTOM_KEY}.
     *
     * Existing metadata for other fields is preserved. Existing metadata for the same field and known parser keys is
     * replaced so stale values do not survive type changes.
     */
    public IndexMetadata putFieldDomain(IndexMetadata metadata, FieldDomain domain) {
        Objects.requireNonNull(metadata, "metadata must not be null");
        Objects.requireNonNull(domain, "domain must not be null");

        Map<String, String> existing = metadata.getCustomData(CUSTOM_KEY);
        Map<String, String> updated = existing == null ? new HashMap<>() : new HashMap<>(existing);

        String prefix = fieldPrefix(domain.field());
        removeKnownFieldKeys(updated, prefix);
        updated.putAll(toCustomData(domain));

        return IndexMetadata.builder(metadata).putCustom(CUSTOM_KEY, updated).build();
    }

    private void removeKnownFieldKeys(Map<String, String> target, String prefix) {
        target.remove(prefix + KEY_TYPE);
        parserRegistry.removeFieldKeys(target, prefix);
    }

    private static String fieldPrefix(String field) {
        if (field == null || field.isEmpty()) {
            throw new IllegalArgumentException("field must not be null or empty");
        }
        return FIELDS_PREFIX + field + ".";
    }
}
