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

import org.opensearch.common.Nullable;
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
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.ingest.ConfigurationUtils.newConfigurationException;

/**
 * Processor that removes existing fields. Nothing happens if the field is not present.
 */
public final class RemoveProcessor extends AbstractProcessor {

    public static final String TYPE = "remove";

    private final List<TemplateScript.Factory> fields;
    private final List<TemplateScript.Factory> excludeFields;
    private final boolean ignoreMissing;

    RemoveProcessor(
        String tag,
        String description,
        @Nullable List<TemplateScript.Factory> fields,
        @Nullable List<TemplateScript.Factory> excludeFields,
        boolean ignoreMissing
    ) {
        super(tag, description);
        if (fields == null && excludeFields == null || fields != null && excludeFields != null) {
            throw new IllegalArgumentException("either fields or excludeFields must be set");
        }
        if (fields != null) {
            this.fields = new ArrayList<>(fields);
            this.excludeFields = null;
        } else {
            this.fields = null;
            this.excludeFields = new ArrayList<>(excludeFields);
        }

        this.ignoreMissing = ignoreMissing;
    }

    public List<TemplateScript.Factory> getFields() {
        return fields;
    }

    public List<TemplateScript.Factory> getExcludeFields() {
        return excludeFields;
    }

    @Override
    public IngestDocument execute(IngestDocument document) {
        if (fields != null && !fields.isEmpty()) {
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
                if (path.equals(IngestDocument.Metadata.ID.getFieldName())
                    && document.hasField(IngestDocument.Metadata.VERSION_TYPE.getFieldName())) {
                    String versionType = document.getFieldValue(IngestDocument.Metadata.VERSION_TYPE.getFieldName(), String.class);
                    if (!Objects.equals(versionType, VersionType.toString(VersionType.INTERNAL))) {
                        Long version = document.getFieldValue(IngestDocument.Metadata.VERSION.getFieldName(), Long.class, true);
                        throw new IllegalArgumentException(
                            "cannot remove metadata field [_id] when specifying external version for the document, version: "
                                + version
                                + ", version_type: "
                                + versionType
                        );
                    }
                }
                document.removeField(path);
            });
        }

        if (excludeFields != null && !excludeFields.isEmpty()) {
            Set<String> excludeFieldSet = new HashSet<>();
            excludeFields.forEach(field -> {
                String path = document.renderTemplate(field);
                // ignore the empty or null field path
                if (!Strings.isNullOrEmpty(path)) {
                    excludeFieldSet.add(path);
                }
            });

            if (!excludeFieldSet.isEmpty()) {
                Set<String> existingFields = new HashSet<>(document.getSourceAndMetadata().keySet());
                Set<String> metadataFields = document.getMetadata()
                    .keySet()
                    .stream()
                    .map(IngestDocument.Metadata::getFieldName)
                    .collect(Collectors.toSet());
                existingFields.forEach(field -> {
                    // ignore metadata fields such as _index, _id, etc.
                    if (!metadataFields.contains(field) && !excludeFieldSet.contains(field)) {
                        document.removeField(field);
                    }
                });
            }
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
            final List<String> excludeFields = new ArrayList<>();
            final Object field = ConfigurationUtils.readOptionalObject(config, "field");
            final Object excludeField = ConfigurationUtils.readOptionalObject(config, "exclude_field");

            if (field == null && excludeField == null || field != null && excludeField != null) {
                throw newConfigurationException(TYPE, processorTag, "field", "either field or exclude_field must be set");
            }

            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);

            if (field != null) {
                if (field instanceof List) {
                    @SuppressWarnings("unchecked")
                    List<String> stringList = (List<String>) field;
                    fields.addAll(stringList);
                } else {
                    fields.add((String) field);
                }
                List<TemplateScript.Factory> fieldCompiledTemplates = fields.stream()
                    .map(f -> ConfigurationUtils.compileTemplate(TYPE, processorTag, "field", f, scriptService))
                    .collect(Collectors.toList());
                return new RemoveProcessor(processorTag, description, fieldCompiledTemplates, null, ignoreMissing);
            } else {
                if (excludeField instanceof List) {
                    @SuppressWarnings("unchecked")
                    List<String> stringList = (List<String>) excludeField;
                    excludeFields.addAll(stringList);
                } else {
                    excludeFields.add((String) excludeField);
                }
                List<TemplateScript.Factory> excludeFieldCompiledTemplates = excludeFields.stream()
                    .map(f -> ConfigurationUtils.compileTemplate(TYPE, processorTag, "exclude_field", f, scriptService))
                    .collect(Collectors.toList());
                return new RemoveProcessor(processorTag, description, null, excludeFieldCompiledTemplates, ignoreMissing);
            }
        }
    }
}
