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
import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Processor;
import org.opensearch.script.ScriptService;
import org.opensearch.script.TemplateScript;

import java.util.Map;

/**
 * Processor that allows to rename existing fields. Will throw exception if the field is not present.
 */
public final class RenameProcessor extends AbstractProcessor {

    public static final String TYPE = "rename";

    private final TemplateScript.Factory field;
    private final TemplateScript.Factory targetField;
    private final boolean ignoreMissing;
    private final boolean overrideTarget;

    RenameProcessor(
        String tag,
        String description,
        TemplateScript.Factory field,
        TemplateScript.Factory targetField,
        boolean ignoreMissing,
        boolean overrideTarget
    ) {
        super(tag, description);
        this.field = field;
        this.targetField = targetField;
        this.ignoreMissing = ignoreMissing;
        this.overrideTarget = overrideTarget;
    }

    TemplateScript.Factory getField() {
        return field;
    }

    TemplateScript.Factory getTargetField() {
        return targetField;
    }

    boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    boolean isOverrideTarget() {
        return overrideTarget;
    }

    @Override
    public IngestDocument execute(IngestDocument document) {
        String path = document.renderTemplate(field);
        final boolean fieldPathIsNullOrEmpty = Strings.isNullOrEmpty(path);
        if (fieldPathIsNullOrEmpty || document.hasField(path, true) == false) {
            if (ignoreMissing) {
                return document;
            } else if (fieldPathIsNullOrEmpty) {
                throw new IllegalArgumentException("field path cannot be null nor empty");
            } else {
                throw new IllegalArgumentException("field [" + path + "] doesn't exist");
            }
        }
        // We fail here if the target field point to an array slot that is out of range.
        // If we didn't do this then we would fail if we set the value in the target_field
        // and then on failure processors would not see that value we tried to rename as we already
        // removed it. If the target field is out of range, we throw the exception no matter
        // what the parameter overrideTarget is.
        String target = document.renderTemplate(targetField);
        if (document.hasField(target, true) && !overrideTarget) {
            throw new IllegalArgumentException("field [" + target + "] already exists");
        }

        Object value = document.getFieldValue(path, Object.class);
        document.removeField(path);
        try {
            document.setFieldValue(target, value);
        } catch (Exception e) {
            // setting the value back to the original field shouldn't as we just fetched it from that field:
            document.setFieldValue(path, value);
            throw e;
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
        public RenameProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            TemplateScript.Factory fieldTemplate = ConfigurationUtils.compileTemplate(TYPE, processorTag, "field", field, scriptService);
            String targetField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "target_field");
            TemplateScript.Factory targetFieldTemplate = ConfigurationUtils.compileTemplate(
                TYPE,
                processorTag,
                "target_field",
                targetField,
                scriptService
            );
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);
            boolean overrideTarget = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "override_target", false);
            return new RenameProcessor(processorTag, description, fieldTemplate, targetFieldTemplate, ignoreMissing, overrideTarget);
        }
    }
}
