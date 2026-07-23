/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.fielddomain;

import org.apache.lucene.util.UnicodeUtil;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.annotation.ExperimentalApi;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

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

    static final int MAX_CUSTOM_DATA_BYTES = 16 * 1024; // 16 KiB

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
     * Validates and parses all field domains declared in an {@code index_field_domains} custom metadata map.
     *
     * This method is intentionally strict for producer-side writes: a request that declares an unsupported or malformed
     * field domain is rejected instead of being silently stored. Search-time consumers should use
     * {@link #fromCustomData(Map, String)} to read one field conservatively.
     *
     * @param customData custom metadata map stored under {@link #CUSTOM_KEY}
     * @return parsed field domains in an unspecified order
     * @throws IllegalArgumentException when the metadata declares an unsupported or malformed field domain
     */
    public List<FieldDomain> validateAndParseCustomData(Map<String, String> customData) {
        if (customData == null || customData.isEmpty()) {
            return List.of();
        }

        Set<String> fields = declaredFields(customData);
        if (fields.isEmpty()) {
            throw new IllegalArgumentException("field domain metadata contains no field declarations");
        }

        List<FieldDomain> domains = new ArrayList<>(fields.size());
        for (String field : fields) {
            String type = customData.get(fieldPrefix(field) + KEY_TYPE);
            if (parserRegistry.contains(type) == false) {
                throw new IllegalArgumentException("unsupported field domain type [" + type + "] for field [" + field + "]");
            }

            Optional<FieldDomain> domain = fromCustomData(customData, field);
            if (domain.isEmpty()) {
                throw new IllegalArgumentException("invalid field domain metadata for field [" + field + "]");
            }
            domains.add(domain.get());
        }
        return List.copyOf(domains);
    }

    /**
     * Encodes field-domain objects into a custom metadata map.
     */
    public Map<String, String> toCustomData(Collection<? extends FieldDomain> domains) {
        Objects.requireNonNull(domains, "domains must not be null");

        Map<String, String> customData = new HashMap<>();
        Set<String> fields = new HashSet<>();
        for (FieldDomain domain : domains) {
            Objects.requireNonNull(domain, "domain must not be null");
            if (fields.add(domain.field()) == false) {
                throw new IllegalArgumentException("duplicate field domain for field [" + domain.field() + "]");
            }
            customData.putAll(toCustomData(domain));
        }
        ensureCustomDataWithinLimit(customData);
        return Map.copyOf(customData);
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
        ensureCustomDataWithinLimit(customData);
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
        ensureCustomDataWithinLimit(updated);

        return IndexMetadata.builder(metadata).putCustom(CUSTOM_KEY, updated).build();
    }

    /**
     * Returns updated index metadata with the supplied field domains inserted under {@link #CUSTOM_KEY}.
     *
     * Existing metadata for other fields is preserved. Existing metadata for the same fields and known parser keys is
     * replaced so stale values do not survive type changes.
     */
    public IndexMetadata putFieldDomains(IndexMetadata metadata, Collection<? extends FieldDomain> domains) {
        Objects.requireNonNull(metadata, "metadata must not be null");
        Objects.requireNonNull(domains, "domains must not be null");
        if (domains.isEmpty()) {
            return metadata;
        }

        Map<String, String> existing = metadata.getCustomData(CUSTOM_KEY);
        Map<String, String> updated = existing == null ? new HashMap<>() : new HashMap<>(existing);

        for (FieldDomain domain : domains) {
            Objects.requireNonNull(domain, "domain must not be null");
            removeKnownFieldKeys(updated, fieldPrefix(domain.field()));
        }
        updated.putAll(toCustomData(domains));
        ensureCustomDataWithinLimit(updated);

        return IndexMetadata.builder(metadata).putCustom(CUSTOM_KEY, updated).build();
    }

    /**
     * Validates and inserts encoded field-domain metadata under {@link #CUSTOM_KEY}.
     */
    public IndexMetadata putFieldDomains(IndexMetadata metadata, Map<String, String> customData) {
        Objects.requireNonNull(customData, "customData must not be null");
        if (customData.isEmpty()) {
            throw new IllegalArgumentException("field domain metadata is required");
        }
        return putFieldDomains(metadata, validateAndParseCustomData(customData));
    }

    private void removeKnownFieldKeys(Map<String, String> target, String prefix) {
        target.remove(prefix + KEY_TYPE);
        parserRegistry.removeFieldKeys(target, prefix);
    }

    private static Set<String> declaredFields(Map<String, String> customData) {
        Set<String> fields = new LinkedHashSet<>();
        String typeSuffix = "." + KEY_TYPE;
        for (String key : customData.keySet()) {
            if (key == null) {
                throw new IllegalArgumentException("field domain metadata keys must not be null");
            }
            if (key.startsWith(FIELDS_PREFIX) && key.endsWith(typeSuffix)) {
                int fieldStart = FIELDS_PREFIX.length();
                int fieldEnd = key.length() - typeSuffix.length();
                if (fieldEnd <= fieldStart) {
                    throw new IllegalArgumentException("field domain metadata field name must not be empty");
                }
                String field = key.substring(fieldStart, fieldEnd);
                fields.add(field);
            }
        }
        return fields;
    }

    private static void ensureCustomDataWithinLimit(Map<String, String> customData) {
        long sizeInBytes = 0L;
        for (Map.Entry<String, String> entry : customData.entrySet()) {
            sizeInBytes += utf8Length(entry.getKey());
            sizeInBytes += utf8Length(entry.getValue());
            if (sizeInBytes > MAX_CUSTOM_DATA_BYTES) {
                throw new IllegalArgumentException(
                    "index field domain metadata is too large, size must be less than or equal to [" + MAX_CUSTOM_DATA_BYTES + "] bytes"
                );
            }
        }
    }

    private static int utf8Length(String value) {
        return value == null ? 0 : UnicodeUtil.calcUTF16toUTF8Length(value, 0, value.length());
    }

    private static String fieldPrefix(String field) {
        if (field == null || field.isEmpty()) {
            throw new IllegalArgumentException("field must not be null or empty");
        }
        return FIELDS_PREFIX + field + ".";
    }
}
