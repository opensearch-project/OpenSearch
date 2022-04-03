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

/**
 * Processor that appends value or values to existing lists. If the field is not present a new list holding the
 * provided values will be added. If the field is a scalar it will be converted to a single item list and the provided
 * values will be added to the newly created list.
 */
public final class AppendProcessor extends AbstractProcessor {

    public static final String TYPE = "append";

    private final TemplateScript.Factory field;
    private final ValueSource value;
    private final boolean allowDuplicates;

    AppendProcessor(String tag, String description, TemplateScript.Factory field, ValueSource value, boolean allowDuplicates) {
        super(tag, description);
        this.field = field;
        this.value = value;
        this.allowDuplicates = allowDuplicates;
    }

    public TemplateScript.Factory getField() {
        return field;
    }

    public ValueSource getValue() {
        return value;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        ingestDocument.appendFieldValue(field, value, allowDuplicates);
        return ingestDocument;
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
        public AppendProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            Object value = ConfigurationUtils.readObject(TYPE, processorTag, config, "value");
            boolean allowDuplicates = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "allow_duplicates", true);
            TemplateScript.Factory compiledTemplate = ConfigurationUtils.compileTemplate(TYPE, processorTag, "field", field, scriptService);
            return new AppendProcessor(
                processorTag,
                description,
                compiledTemplate,
                ValueSource.wrap(value, scriptService),
                allowDuplicates
            );
        }
    }
}
