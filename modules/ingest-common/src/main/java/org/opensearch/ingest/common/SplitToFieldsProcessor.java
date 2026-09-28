/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.common;

import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Processor;

import java.util.List;
import java.util.Map;

/**
 * Processor that splits a field's string value by a separator and distributes the resulting
 * parts into user-specified target fields. If more parts exist than target fields, the extra
 * parts are discarded. If fewer parts exist than target fields, the surplus target fields
 * are left untouched.
 */
public final class SplitToFieldsProcessor extends AbstractProcessor {

    public static final String TYPE = "split_to_fields";

    private final String field;
    private final String separator;
    private final List<String> targetFields;
    private final boolean ignoreMissing;
    private final boolean preserveTrailing;

    SplitToFieldsProcessor(
        String tag,
        String description,
        String field,
        String separator,
        List<String> targetFields,
        boolean ignoreMissing,
        boolean preserveTrailing
    ) {
        super(tag, description);
        this.field = field;
        this.separator = separator;
        this.targetFields = targetFields;
        this.ignoreMissing = ignoreMissing;
        this.preserveTrailing = preserveTrailing;
    }

    String getField() {
        return field;
    }

    String getSeparator() {
        return separator;
    }

    List<String> getTargetFields() {
        return targetFields;
    }

    boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    boolean isPreserveTrailing() {
        return preserveTrailing;
    }

    @Override
    public IngestDocument execute(IngestDocument document) {
        String oldVal = document.getFieldValue(field, String.class, ignoreMissing);

        if (oldVal == null && ignoreMissing) {
            return document;
        } else if (oldVal == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot split.");
        }

        String[] parts = oldVal.split(separator, preserveTrailing ? -1 : 0);
        int limit = Math.min(parts.length, targetFields.size());
        for (int i = 0; i < limit; i++) {
            document.setFieldValue(targetFields.get(i), parts[i]);
        }
        return document;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static class Factory implements Processor.Factory {
        @Override
        public SplitToFieldsProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            String separator = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "separator");
            List<String> targetFields = ConfigurationUtils.readList(TYPE, processorTag, config, "target_fields");
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);
            boolean preserveTrailing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "preserve_trailing", false);
            return new SplitToFieldsProcessor(processorTag, description, field, separator, targetFields, ignoreMissing, preserveTrailing);
        }
    }
}
