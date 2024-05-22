/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.common;

import org.opensearch.common.Nullable;
import org.opensearch.common.hash.MessageDigests;
import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Processor;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.ingest.ConfigurationUtils.newConfigurationException;

/**
 * Processor that generating hash value for the specified fields or all fields in a document
 */
public class FingerprintProcessor extends AbstractProcessor {
    public static final String TYPE = "fingerprint";
    private static final Set<String> HASH_METHODS = Set.of("MD5", "SHA-1", "SHA-256", "SHA3-256");

    // fields used to generate hash value
    private final List<String> fields;
    // whether generate hash value for all fields in the document or not
    private final boolean includeAllFields;
    // the target field to store the hash value, defaults to fingerprint
    private final String targetField;
    // hash method used to generate the hash value, defaults to SHA-1
    private final String hashMethod;
    private final boolean ignoreMissing;

    FingerprintProcessor(
        String tag,
        String description,
        @Nullable List<String> fields,
        boolean includeAllFields,
        String targetField,
        String hashMethod,
        boolean ignoreMissing
    ) {
        super(tag, description);
        if (fields != null) {
            if (fields.isEmpty()) {
                throw new IllegalArgumentException("fields cannot be empty");
            }
            if (fields.stream().anyMatch(Objects::isNull)) {
                throw new IllegalArgumentException("field path cannot be null nor empty");
            }
            if (includeAllFields) {
                throw new IllegalArgumentException("either fields or include_all_fields can be set");
            }
        } else if (!includeAllFields) {
            throw new IllegalArgumentException("either fields or include_all_fields must be set");
        }

        if (!HASH_METHODS.contains(hashMethod.toUpperCase(Locale.ROOT))) {
            throw new IllegalArgumentException("hash method must be MD5, SHA-1 or SHA-256 or SHA3-256");
        }
        this.fields = fields;
        this.includeAllFields = includeAllFields;
        this.targetField = targetField;
        this.hashMethod = hashMethod;
        this.ignoreMissing = ignoreMissing;
    }

    public List<String> getFields() {
        return fields;
    }

    public boolean getIncludeAllFields() {
        return includeAllFields;
    }

    public String getTargetField() {
        return targetField;
    }

    public String getHashMethod() {
        return hashMethod;
    }

    public boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    @Override
    public IngestDocument execute(IngestDocument document) {
        // we should deduplicate and sort the field names to make sure we can get consistent hash value
        final List<String> sortedFields;
        Set<String> metadataFields = document.getMetadata()
            .keySet()
            .stream()
            .map(IngestDocument.Metadata::getFieldName)
            .collect(Collectors.toSet());
        // metadata fields such as _index, _id and _routing are ignored
        if (includeAllFields) {
            Set<String> existingFields = new HashSet<>(document.getSourceAndMetadata().keySet());
            sortedFields = existingFields.stream().filter(field -> !metadataFields.contains(field)).sorted().collect(Collectors.toList());
        } else {
            sortedFields = fields.stream()
                .distinct()
                .filter(field -> !metadataFields.contains(field))
                .sorted()
                .collect(Collectors.toList());
        }
        assert (!sortedFields.isEmpty());

        final StringBuilder concatenatedFields = new StringBuilder();
        sortedFields.forEach(field -> {
            if (!document.hasField(field)) {
                if (ignoreMissing) {
                    return;
                } else {
                    throw new IllegalArgumentException("field [" + field + "] doesn't exist");
                }
            }

            final Object value = document.getFieldValue(field, Object.class);
            if (value instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> flattenedMap = toFlattenedMap((Map<String, Object>) value);
                flattenedMap.entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(
                        entry -> concatenatedFields.append("|")
                            .append(field)
                            .append(".")
                            .append(entry.getKey())
                            .append("|")
                            .append(entry.getValue())
                    );
            } else {
                concatenatedFields.append("|").append(field).append("|").append(value);
            }
        });
        // if all specified fields don't exist and ignore_missing is true, then do nothing
        if (concatenatedFields.length() == 0) {
            return document;
        }
        concatenatedFields.append("|");

        MessageDigest messageDigest = HashMethod.fromMethodName(hashMethod);
        assert (messageDigest != null);
        messageDigest.update(concatenatedFields.toString().getBytes(StandardCharsets.UTF_8));
        document.setFieldValue(targetField, Base64.getEncoder().encodeToString(messageDigest.digest()));

        return document;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    /**
     * Convert a map containing nested fields to a flattened map,
     * for example, if the original map is
     * {
     *     "a": {
     *         "b": 1,
     *         "c": 2
     *     }
     * }, then the converted map is
     * {
     *     "a.b": 1,
     *     "a.c": 2
     * }
     * @param map the original map which may contain nested fields
     * @return a flattened map which has only one level fields
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> toFlattenedMap(Map<String, Object> map) {
        Map<String, Object> flattenedMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (entry.getValue() instanceof Map) {
                toFlattenedMap((Map<String, Object>) entry.getValue()).forEach(
                    (key, value) -> flattenedMap.put(entry.getKey() + "." + key, value)
                );
            } else {
                flattenedMap.put(entry.getKey(), entry.getValue());
            }
        }
        return flattenedMap;
    }

    /**
     * The supported hash methods used to generate hash value
     */
    enum HashMethod {
        MD5(MessageDigests.md5()),
        SHA1(MessageDigests.sha1()),
        SHA256(MessageDigests.sha256()),
        SHA3256(MessageDigests.sha3256());

        private final MessageDigest messageDigest;

        HashMethod(MessageDigest messageDigest) {
            this.messageDigest = messageDigest;
        }

        public static MessageDigest fromMethodName(String methodName) {
            String name = methodName.toUpperCase(Locale.ROOT);
            switch (name) {
                case "MD5":
                    return MD5.messageDigest;
                case "SHA-1":
                    return SHA1.messageDigest;
                case "SHA-256":
                    return SHA256.messageDigest;
                case "SHA3-256":
                    return SHA3256.messageDigest;
                default:
                    return null;
            }
        }
    }

    public static final class Factory implements Processor.Factory {
        @Override
        public FingerprintProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            List<String> fields = ConfigurationUtils.readOptionalList(TYPE, processorTag, config, "fields");
            boolean includeAllFields = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "include_all_fields", false);
            if (fields != null) {
                if (fields.isEmpty()) {
                    throw newConfigurationException(TYPE, processorTag, "fields", "fields cannot be empty");
                }
                if (fields.stream().anyMatch(Objects::isNull)) {
                    throw newConfigurationException(TYPE, processorTag, "fields", "field path cannot be null nor empty");
                }
                if (includeAllFields) {
                    throw newConfigurationException(TYPE, processorTag, "fields", "either fields or include_all_fields can be set");
                }
            } else if (!includeAllFields) {
                throw newConfigurationException(TYPE, processorTag, "fields", "either fields or include_all_fields must be set");
            }

            String targetField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "target_field", "fingerprint");
            String hashMethod = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "hash_method", "SHA-1");
            if (!HASH_METHODS.contains(hashMethod.toUpperCase(Locale.ROOT))) {
                throw newConfigurationException(TYPE, processorTag, "hash_method", "hash method must be MD5, SHA-1, SHA-256 or SHA3-256");
            }
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);
            return new FingerprintProcessor(processorTag, description, fields, includeAllFields, targetField, hashMethod, ignoreMissing);
        }
    }
}
