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

package org.opensearch.ingest;

import org.opensearch.script.TemplateScript;

import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Processes an ingest pipeline
 *
 * @opensearch.internal
 */
public class PipelineProcessor extends AbstractProcessor {

    public static final String TYPE = "pipeline";

    private final TemplateScript.Factory pipelineTemplate;
    private final IngestService ingestService;
    private final boolean ignoreMissingPipeline;

    PipelineProcessor(
        String tag,
        String description,
        TemplateScript.Factory pipelineTemplate,
        IngestService ingestService,
        boolean ignoreMissingPipeline
    ) {
        super(tag, description);
        this.pipelineTemplate = pipelineTemplate;
        this.ingestService = ingestService;
        this.ignoreMissingPipeline = ignoreMissingPipeline;
    }

    @Override
    public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
        String pipelineName = ingestDocument.renderTemplate(this.pipelineTemplate);
        Pipeline pipeline = ingestService.getPipeline(pipelineName);
        if (pipeline != null) {
            ingestDocument.executePipeline(pipeline, handler);
        } else if (!ignoreMissingPipeline) {
            handler.accept(
                null,
                new IllegalStateException("Pipeline processor configured for non-existent pipeline [" + pipelineName + ']')
            );
        } else {
            handler.accept(ingestDocument, null);
        }
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        throw new UnsupportedOperationException("this method should not get executed");
    }

    Pipeline getPipeline(IngestDocument ingestDocument) {
        return ingestService.getPipeline(getPipelineToCallName(ingestDocument));
    }

    String getPipelineToCallName(IngestDocument ingestDocument) {
        return ingestDocument.renderTemplate(this.pipelineTemplate);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    TemplateScript.Factory getPipelineTemplate() {
        return pipelineTemplate;
    }

    boolean isIgnoreMissingPipeline() {
        return ignoreMissingPipeline;
    }

    /**
     * Factory for the processor.
     *
     * @opensearch.internal
     */
    public static final class Factory implements Processor.Factory {

        private final IngestService ingestService;

        public Factory(IngestService ingestService) {
            this.ingestService = ingestService;
        }

        @Override
        public PipelineProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            TemplateScript.Factory pipelineTemplate = ConfigurationUtils.readTemplateProperty(
                TYPE,
                processorTag,
                config,
                "name",
                ingestService.getScriptService()
            );
            boolean ignoreMissingPipeline = ConfigurationUtils.readBooleanProperty(
                TYPE,
                processorTag,
                config,
                "ignore_missing_pipeline",
                false
            );
            return new PipelineProcessor(processorTag, description, pipelineTemplate, ingestService, ignoreMissingPipeline);
        }
    }
}
