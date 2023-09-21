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

import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Processor;
import org.opensearch.ingest.ValueSource;
import org.opensearch.script.ScriptService;
import org.opensearch.script.TemplateScript;

import java.util.Map;

import static org.opensearch.ingest.ConfigurationUtils.newConfigurationException;

/**
 * Processor that adds new fields with their corresponding values. If the field is already present, its value
 * will be replaced with the provided one.
 */
public final class SetProcessor extends AbstractProcessor {

    public static final String TYPE = "set";

    private final boolean overrideEnabled;
    private final TemplateScript.Factory field;
    private final ValueSource value;
    private final boolean ignoreEmptyValue;
    private final String copyFrom;

    SetProcessor(String tag, String description, TemplateScript.Factory field, ValueSource value, String copyFrom) {
        this(tag, description, field, value, true, false, copyFrom);
    }

    SetProcessor(
        String tag,
        String description,
        TemplateScript.Factory field,
        ValueSource value,
        boolean overrideEnabled,
        boolean ignoreEmptyValue,
        String copyFrom
    ) {
        super(tag, description);
        this.overrideEnabled = overrideEnabled;
        this.field = field;
        this.value = value;
        this.ignoreEmptyValue = ignoreEmptyValue;
        this.copyFrom = copyFrom;
    }

    public boolean isOverrideEnabled() {
        return overrideEnabled;
    }

    public TemplateScript.Factory getField() {
        return field;
    }

    public ValueSource getValue() {
        return value;
    }

    public boolean isIgnoreEmptyValue() {
        return ignoreEmptyValue;
    }

    public String getCopyFrom() {
        return copyFrom;
    }

    @Override
    public IngestDocument execute(IngestDocument document) {
        if (overrideEnabled || document.hasField(field) == false || document.getFieldValue(field, Object.class) == null) {
            if (copyFrom != null) {
                String path = document.renderTemplate(field);
                if (copyFrom.isEmpty()) {
                    throw new IllegalArgumentException("copy_from cannot be empty");
                }
                Object sourceFieldValue = document.getFieldValue(copyFrom, Object.class, ignoreEmptyValue);
                if (ignoreEmptyValue
                    && (sourceFieldValue == null || sourceFieldValue instanceof String && ((String) sourceFieldValue).isEmpty())) {
                    return document;
                }
                document.setFieldValue(path, IngestDocument.deepCopy(sourceFieldValue));
            } else {
                document.setFieldValue(field, value, ignoreEmptyValue);
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
        public SetProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            boolean overrideEnabled = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "override", true);
            TemplateScript.Factory compiledTemplate = ConfigurationUtils.compileTemplate(TYPE, processorTag, "field", field, scriptService);
            boolean ignoreEmptyValue = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_empty_value", false);
            String copyFrom = ConfigurationUtils.readOptionalStringProperty(TYPE, processorTag, config, "copy_from");

            ValueSource valueSource = null;
            if (copyFrom == null) {
                Object value = ConfigurationUtils.readObject(TYPE, processorTag, config, "value");
                valueSource = ValueSource.wrap(value, scriptService);
            } else if (config.get("value") != null) {
                throw newConfigurationException(TYPE, processorTag, "copy_from", "either copy_from or value can be set");
            }

            return new SetProcessor(processorTag, description, compiledTemplate, valueSource, overrideEnabled, ignoreEmptyValue, copyFrom);
        }
    }
}
