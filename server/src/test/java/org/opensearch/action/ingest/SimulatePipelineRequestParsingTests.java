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

package org.opensearch.action.ingest;

import org.opensearch.OpenSearchParseException;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.index.VersionType;
import org.opensearch.ingest.CompoundProcessor;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.IngestService;
import org.opensearch.ingest.Pipeline;
import org.opensearch.ingest.Processor;
import org.opensearch.ingest.TestProcessor;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.opensearch.action.ingest.SimulatePipelineRequest.Fields;
import static org.opensearch.action.ingest.SimulatePipelineRequest.SIMULATED_PIPELINE_ID;
import static org.opensearch.ingest.IngestDocument.Metadata.ID;
import static org.opensearch.ingest.IngestDocument.Metadata.IF_PRIMARY_TERM;
import static org.opensearch.ingest.IngestDocument.Metadata.IF_SEQ_NO;
import static org.opensearch.ingest.IngestDocument.Metadata.INDEX;
import static org.opensearch.ingest.IngestDocument.Metadata.OP_TYPE;
import static org.opensearch.ingest.IngestDocument.Metadata.ROUTING;
import static org.opensearch.ingest.IngestDocument.Metadata.VERSION;
import static org.opensearch.ingest.IngestDocument.Metadata.VERSION_TYPE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SimulatePipelineRequestParsingTests extends OpenSearchTestCase {

    private IngestService ingestService;

    @Before
    public void init() throws IOException {
        TestProcessor processor = new TestProcessor(ingestDocument -> {});
        CompoundProcessor pipelineCompoundProcessor = new CompoundProcessor(processor);
        Pipeline pipeline = new Pipeline(SIMULATED_PIPELINE_ID, null, null, pipelineCompoundProcessor);
        Map<String, Processor.Factory> registry = Collections.singletonMap(
            "mock_processor",
            (factories, tag, description, config) -> processor
        );
        ingestService = mock(IngestService.class);
        when(ingestService.getPipeline(SIMULATED_PIPELINE_ID)).thenReturn(pipeline);
        when(ingestService.getProcessorFactories()).thenReturn(registry);
    }

    public void testParseUsingPipelineStore() throws Exception {
        int numDocs = randomIntBetween(1, 10);

        Map<String, Object> requestContent = new HashMap<>();
        List<Map<String, Object>> docs = new ArrayList<>();
        List<Map<String, Object>> expectedDocs = new ArrayList<>();
        requestContent.put(Fields.DOCS, docs);
        for (int i = 0; i < numDocs; i++) {
            Map<String, Object> doc = new HashMap<>();
            String index = randomAlphaOfLengthBetween(1, 10);
            String id = randomAlphaOfLengthBetween(1, 10);
            doc.put(INDEX.getFieldName(), index);
            doc.put(ID.getFieldName(), id);
            String fieldName = randomAlphaOfLengthBetween(1, 10);
            String fieldValue = randomAlphaOfLengthBetween(1, 10);
            doc.put(Fields.SOURCE, Collections.singletonMap(fieldName, fieldValue));
            docs.add(doc);
            Map<String, Object> expectedDoc = new HashMap<>();
            expectedDoc.put(INDEX.getFieldName(), index);
            expectedDoc.put(ID.getFieldName(), id);
            expectedDoc.put(Fields.SOURCE, Collections.singletonMap(fieldName, fieldValue));
            expectedDocs.add(expectedDoc);
        }

        SimulatePipelineRequest.Parsed actualRequest = SimulatePipelineRequest.parseWithPipelineId(
            SIMULATED_PIPELINE_ID,
            requestContent,
            false,
            ingestService
        );
        assertThat(actualRequest.isVerbose(), equalTo(false));
        assertThat(actualRequest.getDocuments().size(), equalTo(numDocs));
        Iterator<Map<String, Object>> expectedDocsIterator = expectedDocs.iterator();
        for (IngestDocument ingestDocument : actualRequest.getDocuments()) {
            Map<String, Object> expectedDocument = expectedDocsIterator.next();
            Map<IngestDocument.Metadata, Object> metadataMap = ingestDocument.extractMetadata();
            assertThat(metadataMap.get(INDEX), equalTo(expectedDocument.get(INDEX.getFieldName())));
            assertThat(metadataMap.get(ID), equalTo(expectedDocument.get(ID.getFieldName())));
            assertThat(ingestDocument.getSourceAndMetadata(), equalTo(expectedDocument.get(Fields.SOURCE)));
        }

        assertThat(actualRequest.getPipeline().getId(), equalTo(SIMULATED_PIPELINE_ID));
        assertThat(actualRequest.getPipeline().getDescription(), nullValue());
        assertThat(actualRequest.getPipeline().getProcessors().size(), equalTo(1));
    }

    public void testParseWithProvidedPipeline() throws Exception {
        int numDocs = randomIntBetween(1, 10);

        Map<String, Object> requestContent = new HashMap<>();
        List<Map<String, Object>> docs = new ArrayList<>();
        List<Map<String, Object>> expectedDocs = new ArrayList<>();
        requestContent.put(Fields.DOCS, docs);
        for (int i = 0; i < numDocs; i++) {
            Map<String, Object> doc = new HashMap<>();
            Map<String, Object> expectedDoc = new HashMap<>();
            List<IngestDocument.Metadata> fields = Arrays.asList(
                INDEX,
                ID,
                ROUTING,
                VERSION,
                VERSION_TYPE,
                IF_SEQ_NO,
                IF_PRIMARY_TERM,
                OP_TYPE
            );
            for (IngestDocument.Metadata field : fields) {
                if (field == VERSION) {
                    if (randomBoolean()) {
                        Long value = randomLong();
                        doc.put(field.getFieldName(), value);
                        expectedDoc.put(field.getFieldName(), value);
                    } else {
                        int value = randomIntBetween(1, 1000000);
                        doc.put(field.getFieldName(), value);
                        expectedDoc.put(field.getFieldName(), (long) value);
                    }
                } else if (field == VERSION_TYPE) {
                    String value = VersionType.toString(randomFrom(VersionType.INTERNAL, VersionType.EXTERNAL, VersionType.EXTERNAL_GTE));
                    doc.put(field.getFieldName(), value);
                    expectedDoc.put(field.getFieldName(), value);
                } else if (field == IF_SEQ_NO || field == IF_PRIMARY_TERM) {
                    if (randomBoolean()) {
                        Long value = randomNonNegativeLong();
                        doc.put(field.getFieldName(), value);
                        expectedDoc.put(field.getFieldName(), value);
                    } else {
                        int value = randomIntBetween(1, 1000000);
                        doc.put(field.getFieldName(), value);
                        expectedDoc.put(field.getFieldName(), (long) value);
                    }
                } else if (field == OP_TYPE) {
                    String value = randomFrom(DocWriteRequest.OpType.values()).getLowercase();
                    doc.put(field.getFieldName(), value);
                    expectedDoc.put(field.getFieldName(), value);
                } else {
                    if (randomBoolean()) {
                        String value = randomAlphaOfLengthBetween(1, 10);
                        doc.put(field.getFieldName(), value);
                        expectedDoc.put(field.getFieldName(), value);
                    } else {
                        Integer value = randomIntBetween(1, 1000000);
                        doc.put(field.getFieldName(), value);
                        expectedDoc.put(field.getFieldName(), String.valueOf(value));
                    }
                }
            }
            String fieldName = randomAlphaOfLengthBetween(1, 10);
            String fieldValue = randomAlphaOfLengthBetween(1, 10);
            doc.put(Fields.SOURCE, Collections.singletonMap(fieldName, fieldValue));
            docs.add(doc);
            expectedDoc.put(Fields.SOURCE, Collections.singletonMap(fieldName, fieldValue));
            expectedDocs.add(expectedDoc);
        }

        Map<String, Object> pipelineConfig = new HashMap<>();
        List<Map<String, Object>> processors = new ArrayList<>();
        int numProcessors = randomIntBetween(1, 10);
        for (int i = 0; i < numProcessors; i++) {
            Map<String, Object> processorConfig = new HashMap<>();
            List<Map<String, Object>> onFailureProcessors = new ArrayList<>();
            int numOnFailureProcessors = randomIntBetween(0, 1);
            for (int j = 0; j < numOnFailureProcessors; j++) {
                onFailureProcessors.add(Collections.singletonMap("mock_processor", Collections.emptyMap()));
            }
            if (numOnFailureProcessors > 0) {
                processorConfig.put("on_failure", onFailureProcessors);
            }
            processors.add(Collections.singletonMap("mock_processor", processorConfig));
        }
        pipelineConfig.put("processors", processors);

        List<Map<String, Object>> onFailureProcessors = new ArrayList<>();
        int numOnFailureProcessors = randomIntBetween(0, 1);
        for (int i = 0; i < numOnFailureProcessors; i++) {
            onFailureProcessors.add(Collections.singletonMap("mock_processor", Collections.emptyMap()));
        }
        if (numOnFailureProcessors > 0) {
            pipelineConfig.put("on_failure", onFailureProcessors);
        }

        requestContent.put(Fields.PIPELINE, pipelineConfig);

        SimulatePipelineRequest.Parsed actualRequest = SimulatePipelineRequest.parse(requestContent, false, ingestService);
        assertThat(actualRequest.isVerbose(), equalTo(false));
        assertThat(actualRequest.getDocuments().size(), equalTo(numDocs));
        Iterator<Map<String, Object>> expectedDocsIterator = expectedDocs.iterator();
        for (IngestDocument ingestDocument : actualRequest.getDocuments()) {
            Map<String, Object> expectedDocument = expectedDocsIterator.next();
            Map<IngestDocument.Metadata, Object> metadataMap = ingestDocument.extractMetadata();
            assertThat(metadataMap.get(INDEX), equalTo(expectedDocument.get(INDEX.getFieldName())));
            assertThat(metadataMap.get(ID), equalTo(expectedDocument.get(ID.getFieldName())));
            assertThat(metadataMap.get(ROUTING), equalTo(expectedDocument.get(ROUTING.getFieldName())));
            assertThat(metadataMap.get(VERSION), equalTo(expectedDocument.get(VERSION.getFieldName())));
            assertThat(metadataMap.get(VERSION_TYPE), equalTo(expectedDocument.get(VERSION_TYPE.getFieldName())));
            assertThat(metadataMap.get(IF_SEQ_NO), equalTo(expectedDocument.get(IF_SEQ_NO.getFieldName())));
            assertThat(metadataMap.get(IF_PRIMARY_TERM), equalTo(expectedDocument.get(IF_PRIMARY_TERM.getFieldName())));
            assertThat(metadataMap.get(OP_TYPE), equalTo(expectedDocument.get(OP_TYPE.getFieldName())));
            assertThat(ingestDocument.getSourceAndMetadata(), equalTo(expectedDocument.get(Fields.SOURCE)));
        }

        assertThat(actualRequest.getPipeline().getId(), equalTo(SIMULATED_PIPELINE_ID));
        assertThat(actualRequest.getPipeline().getDescription(), nullValue());
        assertThat(actualRequest.getPipeline().getProcessors().size(), equalTo(numProcessors));
    }

    public void testNullPipelineId() {
        Map<String, Object> requestContent = new HashMap<>();
        List<Map<String, Object>> docs = new ArrayList<>();
        requestContent.put(Fields.DOCS, docs);
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> SimulatePipelineRequest.parseWithPipelineId(null, requestContent, false, ingestService)
        );
        assertThat(e.getMessage(), equalTo("param [pipeline] is null"));
    }

    public void testNonExistentPipelineId() {
        String pipelineId = randomAlphaOfLengthBetween(1, 10);
        Map<String, Object> requestContent = new HashMap<>();
        List<Map<String, Object>> docs = new ArrayList<>();
        requestContent.put(Fields.DOCS, docs);
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> SimulatePipelineRequest.parseWithPipelineId(pipelineId, requestContent, false, ingestService)
        );
        assertThat(e.getMessage(), equalTo("pipeline [" + pipelineId + "] does not exist"));
    }

    public void testNotValidDocs() {
        Map<String, Object> requestContent = new HashMap<>();
        List<Map<String, Object>> docs = new ArrayList<>();
        Map<String, Object> pipelineConfig = new HashMap<>();
        List<Map<String, Object>> processors = new ArrayList<>();
        pipelineConfig.put("processors", processors);
        requestContent.put(Fields.DOCS, docs);
        requestContent.put(Fields.PIPELINE, pipelineConfig);
        Exception e1 = expectThrows(
            IllegalArgumentException.class,
            () -> SimulatePipelineRequest.parse(requestContent, false, ingestService)
        );
        assertThat(e1.getMessage(), equalTo("must specify at least one document in [docs]"));

        List<String> stringList = new ArrayList<>();
        stringList.add("test");
        pipelineConfig.put("processors", processors);
        requestContent.put(Fields.DOCS, stringList);
        requestContent.put(Fields.PIPELINE, pipelineConfig);
        Exception e2 = expectThrows(
            IllegalArgumentException.class,
            () -> SimulatePipelineRequest.parse(requestContent, false, ingestService)
        );
        assertThat(e2.getMessage(), equalTo("malformed [docs] section, should include an inner object"));

        docs.add(new HashMap<>());
        requestContent.put(Fields.DOCS, docs);
        requestContent.put(Fields.PIPELINE, pipelineConfig);
        Exception e3 = expectThrows(
            OpenSearchParseException.class,
            () -> SimulatePipelineRequest.parse(requestContent, false, ingestService)
        );
        assertThat(e3.getMessage(), containsString("required property is missing"));
    }

    public void testNotValidMetadataFields() {
        List<IngestDocument.Metadata> fields = Arrays.asList(VERSION, IF_SEQ_NO, IF_PRIMARY_TERM);
        for (IngestDocument.Metadata field : fields) {
            String metadataFieldName = field.getFieldName();
            Map<String, Object> requestContent = new HashMap<>();
            List<Map<String, Object>> docs = new ArrayList<>();
            requestContent.put(Fields.DOCS, docs);
            Map<String, Object> doc = new HashMap<>();
            doc.put(metadataFieldName, randomAlphaOfLengthBetween(1, 10));
            doc.put(Fields.SOURCE, Collections.singletonMap(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10)));
            docs.add(doc);

            Map<String, Object> pipelineConfig = new HashMap<>();
            List<Map<String, Object>> processors = new ArrayList<>();
            Map<String, Object> processorConfig = new HashMap<>();
            List<Map<String, Object>> onFailureProcessors = new ArrayList<>();
            int numOnFailureProcessors = randomIntBetween(0, 1);
            for (int j = 0; j < numOnFailureProcessors; j++) {
                onFailureProcessors.add(Collections.singletonMap("mock_processor", Collections.emptyMap()));
            }
            if (numOnFailureProcessors > 0) {
                processorConfig.put("on_failure", onFailureProcessors);
            }
            processors.add(Collections.singletonMap("mock_processor", processorConfig));
            pipelineConfig.put("processors", processors);

            requestContent.put(Fields.PIPELINE, pipelineConfig);

            assertThrows(
                "Failed to parse parameter [" + metadataFieldName + "], only int or long is accepted",
                IllegalArgumentException.class,
                () -> SimulatePipelineRequest.parse(requestContent, false, ingestService)
            );
        }

        {
            String metadataFieldName = OP_TYPE.getFieldName();
            Map<String, Object> requestContent = new HashMap<>();
            List<Map<String, Object>> docs = new ArrayList<>();
            requestContent.put(Fields.DOCS, docs);
            Map<String, Object> doc = new HashMap<>();
            String metadataFieldValue = randomAlphaOfLengthBetween(1, 10);
            doc.put(metadataFieldName, metadataFieldValue);
            doc.put(Fields.SOURCE, Collections.singletonMap(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10)));
            docs.add(doc);

            Map<String, Object> pipelineConfig = new HashMap<>();
            List<Map<String, Object>> processors = new ArrayList<>();
            Map<String, Object> processorConfig = new HashMap<>();
            List<Map<String, Object>> onFailureProcessors = new ArrayList<>();
            int numOnFailureProcessors = randomIntBetween(0, 1);
            for (int j = 0; j < numOnFailureProcessors; j++) {
                onFailureProcessors.add(Collections.singletonMap("mock_processor", Collections.emptyMap()));
            }
            if (numOnFailureProcessors > 0) {
                processorConfig.put("on_failure", onFailureProcessors);
            }
            processors.add(Collections.singletonMap("mock_processor", processorConfig));
            pipelineConfig.put("processors", processors);

            requestContent.put(Fields.PIPELINE, pipelineConfig);

            assertThrows(
                "Unknown opType: [" + metadataFieldValue + "]",
                IllegalArgumentException.class,
                () -> SimulatePipelineRequest.parse(requestContent, false, ingestService)
            );
        }

        {
            String metadataFieldName = VERSION_TYPE.getFieldName();
            Map<String, Object> requestContent = new HashMap<>();
            List<Map<String, Object>> docs = new ArrayList<>();
            requestContent.put(Fields.DOCS, docs);
            Map<String, Object> doc = new HashMap<>();
            String metadataFieldValue = randomAlphaOfLengthBetween(1, 10);
            doc.put(metadataFieldName, metadataFieldValue);
            doc.put(Fields.SOURCE, Collections.singletonMap(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10)));
            docs.add(doc);

            Map<String, Object> pipelineConfig = new HashMap<>();
            List<Map<String, Object>> processors = new ArrayList<>();
            Map<String, Object> processorConfig = new HashMap<>();
            List<Map<String, Object>> onFailureProcessors = new ArrayList<>();
            int numOnFailureProcessors = randomIntBetween(0, 1);
            for (int j = 0; j < numOnFailureProcessors; j++) {
                onFailureProcessors.add(Collections.singletonMap("mock_processor", Collections.emptyMap()));
            }
            if (numOnFailureProcessors > 0) {
                processorConfig.put("on_failure", onFailureProcessors);
            }
            processors.add(Collections.singletonMap("mock_processor", processorConfig));
            pipelineConfig.put("processors", processors);

            requestContent.put(Fields.PIPELINE, pipelineConfig);

            assertThrows(
                "No version type match [" + metadataFieldValue + "]",
                IllegalArgumentException.class,
                () -> SimulatePipelineRequest.parse(requestContent, false, ingestService)
            );
        }
    }
}
