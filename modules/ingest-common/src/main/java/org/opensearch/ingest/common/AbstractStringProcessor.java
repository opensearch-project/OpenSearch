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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Base class for processors that manipulate source strings and require a single "fields" array config value, which
 * holds a list of field names in string format.
 *
 * @param <T> The resultant type for the target field
 */
abstract class AbstractStringProcessor<T> extends AbstractProcessor {
    private final String field;
    private final boolean ignoreMissing;
    private final String targetField;

    AbstractStringProcessor(String tag, String description, boolean ignoreMissing, String targetField, String field) {
        super(tag, description);
        this.field = field;
        this.ignoreMissing = ignoreMissing;
        this.targetField = targetField;
    }

    public String getField() {
        return field;
    }

    boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    String getTargetField() {
        return targetField;
    }

    @Override
    public final IngestDocument execute(IngestDocument document) {
        Object val = document.getFieldValue(field, Object.class, ignoreMissing);
        Object newValue;

        if (val == null && ignoreMissing) {
            return document;
        } else if (val == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot process it.");
        }

        if (val instanceof List<?> list) {
            List<Object> newList = new ArrayList<>(list.size());
            for (Object value : list) {
                if (value instanceof String stringValue) {
                    newList.add(process(stringValue));
                } else {
                    throw new IllegalArgumentException(
                        "value ["
                            + value
                            + "] of type ["
                            + value.getClass().getName()
                            + "] in list field ["
                            + field
                            + "] cannot be cast to ["
                            + String.class.getName()
                            + "]"
                    );
                }
            }
            newValue = newList;
        } else {
            if (val instanceof String stringVal) {
                newValue = process(stringVal);
            } else {
                throw new IllegalArgumentException(
                    "field [" + field + "] of type [" + val.getClass().getName() + "] cannot be cast to [" + String.class.getName() + "]"
                );
            }

        }

        document.setFieldValue(targetField, newValue);
        return document;
    }

    protected abstract T process(String value);

    abstract static class Factory implements Processor.Factory {
        final String processorType;

        protected Factory(String processorType) {
            this.processorType = processorType;
        }

        @Override
        public AbstractStringProcessor<?> create(
            Map<String, Processor.Factory> registry,
            String tag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            String field = ConfigurationUtils.readStringProperty(processorType, tag, config, "field");
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(processorType, tag, config, "ignore_missing", false);
            String targetField = ConfigurationUtils.readStringProperty(processorType, tag, config, "target_field", field);

            return newProcessor(tag, description, config, field, ignoreMissing, targetField);
        }

        protected abstract AbstractStringProcessor<?> newProcessor(
            String processorTag,
            String description,
            Map<String, Object> config,
            String field,
            boolean ignoreMissing,
            String targetField
        );
    }
}
