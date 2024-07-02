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
import org.opensearch.core.common.Strings;
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
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.ingest.ConfigurationUtils.newConfigurationException;

/**
 * Processor that generating hash value for the specified fields or fields not in the specified excluded list
 */
public final class FingerprintProcessor extends AbstractProcessor {
    public static final String TYPE = "fingerprint";
    // this processor is introduced in 2.16.0, we append the OpenSearch version to the hash method name to ensure
    // that this processor always generates same hash value based on a specific hash method, if the processing logic
    // of this processor changes in future version, the version number in the hash method should be increased correspondingly.
    private static final Set<String> HASH_METHODS = Set.of("MD5@2.16.0", "SHA-1@2.16.0", "SHA-256@2.16.0", "SHA3-256@2.16.0");

    // fields used to generate hash value
    private final List<String> fields;
    // all fields other than the excluded fields are used to generate hash value
    private final List<String> excludeFields;
    // the target field to store the hash value, defaults to fingerprint
    private final String targetField;
    // hash method used to generate the hash value, defaults to SHA-1
    private final String hashMethod;
    private final boolean ignoreMissing;

    FingerprintProcessor(
        String tag,
        String description,
        @Nullable List<String> fields,
        @Nullable List<String> excludeFields,
        String targetField,
        String hashMethod,
        boolean ignoreMissing
    ) {
        super(tag, description);
        if (fields != null && !fields.isEmpty()) {
            if (fields.stream().anyMatch(Strings::isNullOrEmpty)) {
                throw new IllegalArgumentException("field name in [fields] cannot be null nor empty");
            }
            if (excludeFields != null && !excludeFields.isEmpty()) {
                throw new IllegalArgumentException("either fields or exclude_fields can be set");
            }
        }
        if (excludeFields != null && !excludeFields.isEmpty() && excludeFields.stream().anyMatch(Strings::isNullOrEmpty)) {
            throw new IllegalArgumentException("field name in [exclude_fields] cannot be null nor empty");
        }

        if (!HASH_METHODS.contains(hashMethod.toUpperCase(Locale.ROOT))) {
            throw new IllegalArgumentException("hash method must be MD5@2.16.0, SHA-1@2.16.0 or SHA-256@2.16.0 or SHA3-256@2.16.0");
        }
        this.fields = fields;
        this.excludeFields = excludeFields;
        this.targetField = targetField;
        this.hashMethod = hashMethod;
        this.ignoreMissing = ignoreMissing;
    }

    public List<String> getFields() {
        return fields;
    }

    public List<String> getExcludeFields() {
        return excludeFields;
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
        Set<String> existingFields = new HashSet<>(document.getSourceAndMetadata().keySet());
        Set<String> metadataFields = document.getMetadata()
            .keySet()
            .stream()
            .map(IngestDocument.Metadata::getFieldName)
            .collect(Collectors.toSet());
        // metadata fields such as _index, _id and _routing are ignored
        if (fields != null && !fields.isEmpty()) {
            sortedFields = fields.stream()
                .distinct()
                .filter(field -> !metadataFields.contains(field))
                .sorted()
                .collect(Collectors.toList());
        } else if (excludeFields != null && !excludeFields.isEmpty()) {
            sortedFields = existingFields.stream()
                .filter(field -> !metadataFields.contains(field) && !excludeFields.contains(field))
                .sorted()
                .collect(Collectors.toList());
        } else {
            sortedFields = existingFields.stream().filter(field -> !metadataFields.contains(field)).sorted().collect(Collectors.toList());
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
                flattenedMap.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry -> {
                    String fieldValue = String.valueOf(entry.getValue());
                    concatenatedFields.append("|")
                        .append(field)
                        .append(".")
                        .append(entry.getKey())
                        .append("|")
                        .append(fieldValue.length())
                        .append(":")
                        .append(fieldValue);
                });
            } else {
                String fieldValue = String.valueOf(value);
                concatenatedFields.append("|").append(field).append("|").append(fieldValue.length()).append(":").append(fieldValue);
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
        document.setFieldValue(targetField, hashMethod + ":" + Base64.getEncoder().encodeToString(messageDigest.digest()));

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
                case "MD5@2.16.0":
                    return MD5.messageDigest;
                case "SHA-1@2.16.0":
                    return SHA1.messageDigest;
                case "SHA-256@2.16.0":
                    return SHA256.messageDigest;
                case "SHA3-256@2.16.0":
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
            List<String> excludeFields = ConfigurationUtils.readOptionalList(TYPE, processorTag, config, "exclude_fields");
            if (fields != null && !fields.isEmpty()) {
                if (fields.stream().anyMatch(Strings::isNullOrEmpty)) {
                    throw newConfigurationException(TYPE, processorTag, "fields", "field name cannot be null nor empty");
                }
                if (excludeFields != null && !excludeFields.isEmpty()) {
                    throw newConfigurationException(TYPE, processorTag, "fields", "either fields or exclude_fields can be set");
                }
            }
            if (excludeFields != null && !excludeFields.isEmpty() && excludeFields.stream().anyMatch(Strings::isNullOrEmpty)) {
                throw newConfigurationException(TYPE, processorTag, "exclude_fields", "field name cannot be null nor empty");
            }

            String targetField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "target_field", "fingerprint");
            String hashMethod = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "hash_method", "SHA-1@2.16.0");
            if (!HASH_METHODS.contains(hashMethod.toUpperCase(Locale.ROOT))) {
                throw newConfigurationException(
                    TYPE,
                    processorTag,
                    "hash_method",
                    "hash method must be MD5@2.16.0, SHA-1@2.16.0, SHA-256@2.16.0 or SHA3-256@2.16.0"
                );
            }
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);
            return new FingerprintProcessor(processorTag, description, fields, excludeFields, targetField, hashMethod, ignoreMissing);
        }
    }
}
