/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.common;

import org.opensearch.common.Nullable;
import org.opensearch.common.ValidationException;
import org.opensearch.common.regex.Regex;
import org.opensearch.core.common.Strings;
import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Processor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.ingest.ConfigurationUtils.newConfigurationException;

/**
 * Processor that removes existing fields by field patterns or excluding field patterns.
 */
public final class RemoveByPatternProcessor extends AbstractProcessor {

    public static final String TYPE = "remove_by_pattern";
    private final List<String> fieldPatterns;
    private final List<String> excludeFieldPatterns;

    RemoveByPatternProcessor(
        String tag,
        String description,
        @Nullable List<String> fieldPatterns,
        @Nullable List<String> excludeFieldPatterns
    ) {
        super(tag, description);
        if (fieldPatterns != null && excludeFieldPatterns != null || fieldPatterns == null && excludeFieldPatterns == null) {
            throw new IllegalArgumentException("either fieldPatterns and excludeFieldPatterns must be set");
        }
        if (fieldPatterns == null) {
            this.fieldPatterns = null;
            this.excludeFieldPatterns = new ArrayList<>(excludeFieldPatterns);
        } else {
            this.fieldPatterns = new ArrayList<>(fieldPatterns);
            this.excludeFieldPatterns = null;
        }
    }

    public List<String> getFieldPatterns() {
        return fieldPatterns;
    }

    public List<String> getExcludeFieldPatterns() {
        return excludeFieldPatterns;
    }

    @Override
    public IngestDocument execute(IngestDocument document) {
        Set<String> existingFields = new HashSet<>(document.getSourceAndMetadata().keySet());
        Set<String> metadataFields = document.getMetadata()
            .keySet()
            .stream()
            .map(IngestDocument.Metadata::getFieldName)
            .collect(Collectors.toSet());

        if (fieldPatterns != null && !fieldPatterns.isEmpty()) {
            existingFields.forEach(field -> {
                // ignore metadata fields such as _index, _id, etc.
                if (!metadataFields.contains(field)) {
                    final boolean matched = fieldPatterns.stream().anyMatch(pattern -> Regex.simpleMatch(pattern, field));
                    if (matched) {
                        document.removeField(field);
                    }
                }
            });
        }

        if (excludeFieldPatterns != null && !excludeFieldPatterns.isEmpty()) {
            existingFields.forEach(field -> {
                // ignore metadata fields such as _index, _id, etc.
                if (!metadataFields.contains(field)) {
                    final boolean matched = excludeFieldPatterns.stream().anyMatch(pattern -> Regex.simpleMatch(pattern, field));
                    if (!matched) {
                        document.removeField(field);
                    }
                }
            });
        }

        return document;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {

        public Factory() {}

        @Override
        public RemoveByPatternProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            final List<String> fieldPatterns = new ArrayList<>();
            final List<String> excludeFieldPatterns = new ArrayList<>();
            final Object fieldPattern = ConfigurationUtils.readOptionalObject(config, "field_pattern");
            final Object excludeFieldPattern = ConfigurationUtils.readOptionalObject(config, "exclude_field_pattern");

            if (fieldPattern == null && excludeFieldPattern == null || fieldPattern != null && excludeFieldPattern != null) {
                throw newConfigurationException(
                    TYPE,
                    processorTag,
                    "field_pattern",
                    "either field_pattern or exclude_field_pattern must be set"
                );
            }

            if (fieldPattern != null) {
                if (fieldPattern instanceof List) {
                    @SuppressWarnings("unchecked")
                    List<String> fieldPatternList = (List<String>) fieldPattern;
                    fieldPatterns.addAll(fieldPatternList);
                } else {
                    fieldPatterns.add((String) fieldPattern);
                }
                validateFieldPatterns(processorTag, fieldPatterns, "field_pattern");
                return new RemoveByPatternProcessor(processorTag, description, fieldPatterns, null);
            } else {
                if (excludeFieldPattern instanceof List) {
                    @SuppressWarnings("unchecked")
                    List<String> excludeFieldPatternList = (List<String>) excludeFieldPattern;
                    excludeFieldPatterns.addAll(excludeFieldPatternList);
                } else {
                    excludeFieldPatterns.add((String) excludeFieldPattern);
                }
                validateFieldPatterns(processorTag, excludeFieldPatterns, "exclude_field_pattern");
                return new RemoveByPatternProcessor(processorTag, description, null, excludeFieldPatterns);
            }
        }

        private void validateFieldPatterns(String processorTag, List<String> patterns, String patternKey) {
            List<String> validationErrors = new ArrayList<>();
            for (String fieldPattern : patterns) {
                if (fieldPattern.contains("#")) {
                    validationErrors.add(patternKey + " [" + fieldPattern + "] must not contain a '#'");
                }
                if (fieldPattern.contains(":")) {
                    validationErrors.add(patternKey + " [" + fieldPattern + "] must not contain a ':'");
                }
                if (fieldPattern.startsWith("_")) {
                    validationErrors.add(patternKey + " [" + fieldPattern + "] must not start with '_'");
                }
                if (Strings.validFileNameExcludingAstrix(fieldPattern) == false) {
                    validationErrors.add(
                        patternKey + " [" + fieldPattern + "] must not contain the following characters " + Strings.INVALID_FILENAME_CHARS
                    );
                }
            }

            if (validationErrors.size() > 0) {
                ValidationException validationException = new ValidationException();
                validationException.addValidationErrors(validationErrors);
                throw newConfigurationException(TYPE, processorTag, patternKey, validationException.getMessage());
            }
        }
    }
}
