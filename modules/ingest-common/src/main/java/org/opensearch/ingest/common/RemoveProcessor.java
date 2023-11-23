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

package org.opensearch.ingest.common;

import org.opensearch.common.ValidationException;
import org.opensearch.common.regex.Regex;
import org.opensearch.core.common.Strings;
import org.opensearch.index.VersionType;
import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Processor;
import org.opensearch.script.ScriptService;
import org.opensearch.script.TemplateScript;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.opensearch.ingest.ConfigurationUtils.newConfigurationException;

/**
 * Processor that removes existing fields. Nothing happens if the field is not present.
 */
public final class RemoveProcessor extends AbstractProcessor {

    public static final String TYPE = "remove";

    private final List<TemplateScript.Factory> fields;
    private final List<String> fieldPatterns;
    private final List<TemplateScript.Factory> excludeFields;
    private final List<String> excludeFieldPatterns;
    private final boolean ignoreMissing;

    RemoveProcessor(
        String tag,
        String description,
        List<TemplateScript.Factory> fields,
        List<String> fieldPatterns,
        List<TemplateScript.Factory> excludeFields,
        List<String> excludeFieldPatterns,
        boolean ignoreMissing
    ) {
        super(tag, description);
        this.fields = new ArrayList<>(fields);
        this.fieldPatterns = new ArrayList<>(fieldPatterns);
        this.excludeFields = new ArrayList<>(excludeFields);
        this.excludeFieldPatterns = new ArrayList<>(excludeFieldPatterns);
        this.ignoreMissing = ignoreMissing;
    }

    public List<TemplateScript.Factory> getFields() {
        return fields;
    }

    public List<String> getFieldPatterns() {
        return fieldPatterns;
    }

    public List<TemplateScript.Factory> getExcludeFields() {
        return excludeFields;
    }

    public List<String> getExcludeFieldPatterns() {
        return excludeFieldPatterns;
    }

    @Override
    public IngestDocument execute(IngestDocument document) {
        if (!fields.isEmpty()) {
            fields.forEach(field -> {
                String path = document.renderTemplate(field);
                final boolean fieldPathIsNullOrEmpty = Strings.isNullOrEmpty(path);
                if (fieldPathIsNullOrEmpty || document.hasField(path) == false) {
                    if (ignoreMissing) {
                        return;
                    } else if (fieldPathIsNullOrEmpty) {
                        throw new IllegalArgumentException("field path cannot be null nor empty");
                    } else {
                        throw new IllegalArgumentException("field [" + path + "] doesn't exist");
                    }
                }

                // cannot remove _index, _version and _version_type.
                if (path.equals(IngestDocument.Metadata.INDEX.getFieldName())
                    || path.equals(IngestDocument.Metadata.VERSION.getFieldName())
                    || path.equals(IngestDocument.Metadata.VERSION_TYPE.getFieldName())) {
                    throw new IllegalArgumentException("cannot remove metadata field [" + path + "]");
                }
                // removing _id is disallowed when there's an external version specified in the request
                String versionType = document.getFieldValue(IngestDocument.Metadata.VERSION_TYPE.getFieldName(), String.class);
                if (path.equals(IngestDocument.Metadata.ID.getFieldName())
                    && !Objects.equals(versionType, VersionType.toString(VersionType.INTERNAL))) {
                    Long version = document.getFieldValue(IngestDocument.Metadata.VERSION.getFieldName(), Long.class);
                    throw new IllegalArgumentException(
                        "cannot remove metadata field [_id] when specifying external version for the document, version: "
                            + version
                            + ", version_type: "
                            + versionType
                    );
                }
                document.removeField(path);
            });
        }

        if (!fieldPatterns.isEmpty()) {
            Set<String> existingFields = new HashSet<>(document.getSourceAndMetadata().keySet());
            Set<String> metadataFields = document.getMetadata()
                .keySet()
                .stream()
                .map(IngestDocument.Metadata::getFieldName)
                .collect(Collectors.toSet());
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

        Set<String> excludeFieldSet = new HashSet<>();
        if (!excludeFields.isEmpty()) {
            excludeFields.forEach(field -> {
                String path = document.renderTemplate(field);
                // ignore the empty or null field path
                if (!Strings.isNullOrEmpty(path)) {
                    excludeFieldSet.add(path);
                }
            });
        }

        if (!excludeFieldSet.isEmpty() || !excludeFieldPatterns.isEmpty()) {
            Set<String> existingFields = new HashSet<>(document.getSourceAndMetadata().keySet());
            Set<String> metadataFields = document.getMetadata()
                .keySet()
                .stream()
                .map(IngestDocument.Metadata::getFieldName)
                .collect(Collectors.toSet());
            existingFields.forEach(field -> {
                // ignore metadata fields such as _index, _id, etc.
                if (!metadataFields.contains(field)) {
                    // when both exclude_field and exclude_field_pattern are not empty, remove the field if it doesn't exist in both of them
                    // if not, remove the field if it doesn't exist in the non-empty one
                    if (!excludeFieldPatterns.isEmpty()) {
                        final boolean matched = excludeFieldPatterns.stream().anyMatch(pattern -> Regex.simpleMatch(pattern, field));
                        if (!excludeFieldSet.isEmpty() && !excludeFieldSet.contains(field) && !matched
                            || excludeFieldSet.isEmpty() && !matched) {
                            document.removeField(field);
                        }
                    } else if (!excludeFieldSet.isEmpty() && !excludeFieldSet.contains(field)) {
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

        private final ScriptService scriptService;

        public Factory(ScriptService scriptService) {
            this.scriptService = scriptService;
        }

        @Override
        public RemoveProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            final List<String> fields = new ArrayList<>();
            final List<String> fieldPatterns = new ArrayList<>();
            final List<String> excludeFields = new ArrayList<>();
            final List<String> excludeFieldPatterns = new ArrayList<>();

            final Object field = ConfigurationUtils.readOptionalObject(config, "field");
            final Object fieldPattern = ConfigurationUtils.readOptionalObject(config, "field_pattern");
            final Object excludeField = ConfigurationUtils.readOptionalObject(config, "exclude_field");
            final Object excludeFieldPattern = ConfigurationUtils.readOptionalObject(config, "exclude_field_pattern");

            if (field == null && fieldPattern == null && excludeField == null && excludeFieldPattern == null) {
                throw newConfigurationException(
                    TYPE,
                    processorTag,
                    "field",
                    "at least one of the parameters field, field_pattern, exclude_field and exclude_field_pattern need to be set"
                );
            }

            if ((field != null || fieldPattern != null) && (excludeField != null || excludeFieldPattern != null)) {
                throw newConfigurationException(
                    TYPE,
                    processorTag,
                    "field",
                    "ether (field,field_pattern) or (exclude_field,exclude_field_pattern) can be set"
                );
            }

            List<TemplateScript.Factory> fieldCompiledTemplates = new ArrayList<>();
            if (field != null) {
                if (field instanceof List) {
                    @SuppressWarnings("unchecked")
                    List<String> stringList = (List<String>) field;
                    fields.addAll(stringList);
                } else {
                    fields.add((String) field);
                }
                fieldCompiledTemplates = fields.stream()
                    .map(f -> ConfigurationUtils.compileTemplate(TYPE, processorTag, "field", f, scriptService))
                    .collect(Collectors.toList());
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
            }

            List<TemplateScript.Factory> excludeFieldCompiledTemplates = new ArrayList<>();
            if (excludeField != null) {
                if (excludeField instanceof List) {
                    @SuppressWarnings("unchecked")
                    List<String> stringList = (List<String>) excludeField;
                    excludeFields.addAll(stringList);
                } else {
                    excludeFields.add((String) excludeField);
                }
                excludeFieldCompiledTemplates = excludeFields.stream()
                    .map(f -> ConfigurationUtils.compileTemplate(TYPE, processorTag, "exclude_field", f, scriptService))
                    .collect(Collectors.toList());
            }

            if (excludeFieldPattern != null) {
                if (excludeFieldPattern instanceof List) {
                    @SuppressWarnings("unchecked")
                    List<String> excludeFieldPatternList = (List<String>) excludeFieldPattern;
                    excludeFieldPatterns.addAll(excludeFieldPatternList);
                } else {
                    excludeFieldPatterns.add((String) excludeFieldPattern);
                }
                validateFieldPatterns(processorTag, excludeFieldPatterns, "exclude_field_pattern");
            }

            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);
            return new RemoveProcessor(
                processorTag,
                description,
                fieldCompiledTemplates,
                fieldPatterns,
                excludeFieldCompiledTemplates,
                excludeFieldPatterns,
                ignoreMissing
            );
        }

        private void validateFieldPatterns(String processorTag, List<String> patterns, String patternKey) {
            List<String> validationErrors = new ArrayList<>();
            for (String fieldPattern : patterns) {
                if (fieldPattern.contains(" ")) {
                    validationErrors.add(patternKey + " [" + fieldPattern + "] must not contain a space");
                }
                if (fieldPattern.contains(",")) {
                    validationErrors.add(patternKey + " [" + fieldPattern + "] must not contain a ','");
                }
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
