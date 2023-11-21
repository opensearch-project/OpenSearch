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

import org.opensearch.core.common.Strings;
import org.opensearch.index.VersionType;
import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Processor;
import org.opensearch.script.ScriptService;
import org.opensearch.script.TemplateScript;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Processor that removes existing fields. Nothing happens if the field is not present.
 */
public final class RemoveProcessor extends AbstractProcessor {

    public static final String TYPE = "remove";

    private final List<TemplateScript.Factory> fields;
    private final boolean ignoreMissing;

    RemoveProcessor(String tag, String description, List<TemplateScript.Factory> fields, boolean ignoreMissing) {
        super(tag, description);
        this.fields = new ArrayList<>(fields);
        this.ignoreMissing = ignoreMissing;
    }

    public List<TemplateScript.Factory> getFields() {
        return fields;
    }

    @Override
    public IngestDocument execute(IngestDocument document) {
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
            final Object field = ConfigurationUtils.readObject(TYPE, processorTag, config, "field");
            if (field instanceof List) {
                @SuppressWarnings("unchecked")
                List<String> stringList = (List<String>) field;
                fields.addAll(stringList);
            } else {
                fields.add((String) field);
            }

            final List<TemplateScript.Factory> compiledTemplates = fields.stream()
                .map(f -> ConfigurationUtils.compileTemplate(TYPE, processorTag, "field", f, scriptService))
                .collect(Collectors.toList());
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);
            return new RemoveProcessor(processorTag, description, compiledTemplates, ignoreMissing);
        }
    }
}
