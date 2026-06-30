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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Processor that joins the different items of an array into a single string value using a separator between each item.
 * Throws exception is the specified field is not an array.
 */
public final class JoinProcessor extends AbstractProcessor {

    public static final String TYPE = "join";

    private final String field;
    private final String separator;
    private final String targetField;

    JoinProcessor(String tag, String description, String field, String separator, String targetField) {
        super(tag, description);
        this.field = field;
        this.separator = separator;
        this.targetField = targetField;
    }

    String getField() {
        return field;
    }

    String getSeparator() {
        return separator;
    }

    String getTargetField() {
        return targetField;
    }

    @Override
    public IngestDocument execute(IngestDocument document) {
        List<?> list = document.getFieldValue(field, List.class);
        if (list == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot join.");
        }
        String joined = list.stream().map(Object::toString).collect(Collectors.joining(separator));
        document.setFieldValue(targetField, joined);
        return document;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {
        @Override
        public JoinProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            String separator = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "separator");
            String targetField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "target_field", field);
            return new JoinProcessor(processorTag, description, field, separator, targetField);
        }
    }
}
