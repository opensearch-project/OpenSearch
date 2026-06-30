/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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

public final class CopyProcessor extends AbstractProcessor {
    public static final String TYPE = "copy";

    private final TemplateScript.Factory sourceField;
    private final TemplateScript.Factory targetField;

    private final boolean ignoreMissing;

    private final boolean removeSource;

    private final boolean overrideTarget;

    CopyProcessor(String tag, String description, TemplateScript.Factory sourceField, TemplateScript.Factory targetField) {
        this(tag, description, sourceField, targetField, false, false, false);
    }

    CopyProcessor(
        String tag,
        String description,
        TemplateScript.Factory sourceField,
        TemplateScript.Factory targetField,
        boolean ignoreMissing,
        boolean removeSource,
        boolean overrideTarget
    ) {
        super(tag, description);
        this.sourceField = sourceField;
        this.targetField = targetField;
        this.ignoreMissing = ignoreMissing;
        this.removeSource = removeSource;
        this.overrideTarget = overrideTarget;
    }

    public TemplateScript.Factory getSourceField() {
        return sourceField;
    }

    public TemplateScript.Factory getTargetField() {
        return targetField;
    }

    public boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    public boolean isRemoveSource() {
        return removeSource;
    }

    public boolean isOverrideTarget() {
        return overrideTarget;
    }

    @Override
    public IngestDocument execute(IngestDocument document) {
        String source = document.renderTemplate(sourceField);
        final boolean sourceFieldPathIsNullOrEmpty = Strings.isNullOrEmpty(source);
        if (sourceFieldPathIsNullOrEmpty || document.hasField(source, true) == false) {
            if (ignoreMissing) {
                return document;
            } else if (sourceFieldPathIsNullOrEmpty) {
                throw new IllegalArgumentException("source field path cannot be null nor empty");
            } else {
                throw new IllegalArgumentException("source field [" + source + "] doesn't exist");
            }
        }

        String target = document.renderTemplate(targetField);
        if (Strings.isNullOrEmpty(target)) {
            throw new IllegalArgumentException("target field path cannot be null nor empty");
        }
        if (source.equals(target)) {
            throw new IllegalArgumentException("source field path and target field path cannot be same");
        }

        if (overrideTarget || document.hasField(target, true) == false || document.getFieldValue(target, Object.class) == null) {
            Object sourceValue = document.getFieldValue(source, Object.class);
            document.setFieldValue(target, IngestDocument.deepCopy(sourceValue));
        } else {
            throw new IllegalArgumentException("target field [" + target + "] already exists");
        }

        if (removeSource) {
            document.removeField(source);
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
        public CopyProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            String sourceField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "source_field");
            TemplateScript.Factory sourceFieldTemplate = ConfigurationUtils.compileTemplate(
                TYPE,
                processorTag,
                "source_field",
                sourceField,
                scriptService
            );
            String targetField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "target_field");
            TemplateScript.Factory targetFieldTemplate = ConfigurationUtils.compileTemplate(
                TYPE,
                processorTag,
                "target_field",
                targetField,
                scriptService
            );
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);
            boolean removeSource = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "remove_source", false);
            boolean overrideTarget = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "override_target", false);

            return new CopyProcessor(
                processorTag,
                description,
                sourceFieldTemplate,
                targetFieldTemplate,
                ignoreMissing,
                removeSource,
                overrideTarget
            );
        }
    }
}
