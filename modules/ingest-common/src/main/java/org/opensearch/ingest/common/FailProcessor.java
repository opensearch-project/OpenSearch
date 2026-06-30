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
import org.opensearch.script.ScriptService;
import org.opensearch.script.TemplateScript;

import java.util.Map;

/**
 * Processor that raises a runtime exception with a provided
 * error message.
 */
public final class FailProcessor extends AbstractProcessor {

    public static final String TYPE = "fail";

    private final TemplateScript.Factory message;

    FailProcessor(String tag, String description, TemplateScript.Factory message) {
        super(tag, description);
        this.message = message;
    }

    public TemplateScript.Factory getMessage() {
        return message;
    }

    @Override
    public IngestDocument execute(IngestDocument document) {
        throw new FailProcessorException(document.renderTemplate(message));
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
        public FailProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            String message = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "message");
            TemplateScript.Factory compiledTemplate = ConfigurationUtils.compileTemplate(
                TYPE,
                processorTag,
                "message",
                message,
                scriptService
            );
            return new FailProcessor(processorTag, description, compiledTemplate);
        }
    }
}
