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

import org.opensearch.index.VersionType;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Processor;
import org.opensearch.ingest.RandomDocumentPicks;
import org.opensearch.ingest.TestTemplateService;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class RemoveProcessorTests extends OpenSearchTestCase {

    public void testRemoveFields() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String field = RandomDocumentPicks.addRandomField(random(), ingestDocument, randomAlphaOfLength(10));
        Processor processor = new RemoveProcessor(
            randomAlphaOfLength(10),
            null,
            Collections.singletonList(new TestTemplateService.MockTemplateScript.Factory(field)),
            false
        );
        processor.execute(ingestDocument);
        assertThat(ingestDocument.hasField(field), equalTo(false));
    }

    public void testRemoveNonExistingField() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Map<String, Object> config = new HashMap<>();
        config.put("field", fieldName);
        String processorTag = randomAlphaOfLength(10);
        Processor processor = new RemoveProcessor.Factory(TestTemplateService.instance()).create(null, processorTag, null, config);
        assertThrows(
            "field [" + fieldName + "] doesn't exist",
            IllegalArgumentException.class,
            () -> { processor.execute(ingestDocument); }
        );

        Map<String, Object> configWithEmptyField = new HashMap<>();
        configWithEmptyField.put("field", "");
        processorTag = randomAlphaOfLength(10);
        Processor removeProcessorWithEmptyField = new RemoveProcessor.Factory(TestTemplateService.instance()).create(
            null,
            processorTag,
            null,
            configWithEmptyField
        );
        assertThrows(
            "field path cannot be null nor empty",
            IllegalArgumentException.class,
            () -> removeProcessorWithEmptyField.execute(ingestDocument)
        );
    }

    public void testRemoveEmptyField() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        Map<String, Object> config = new HashMap<>();
        config.put("field", "");
        String processorTag = randomAlphaOfLength(10);
        Processor removeProcessorWithEmptyField = new RemoveProcessor.Factory(TestTemplateService.instance()).create(
            null,
            processorTag,
            null,
            config
        );
        assertThrows(
            "field path cannot be null nor empty",
            IllegalArgumentException.class,
            () -> removeProcessorWithEmptyField.execute(ingestDocument)
        );
    }

    public void testIgnoreMissing() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Map<String, Object> config = new HashMap<>();
        config.put("field", fieldName);
        config.put("ignore_missing", true);
        String processorTag = randomAlphaOfLength(10);
        Processor processor = new RemoveProcessor.Factory(TestTemplateService.instance()).create(null, processorTag, null, config);
        processor.execute(ingestDocument);

        // when using template snippet, the resolved field path maybe empty
        Map<String, Object> configWithEmptyField = new HashMap<>();
        configWithEmptyField.put("field", "");
        configWithEmptyField.put("ignore_missing", true);
        processorTag = randomAlphaOfLength(10);
        processor = new RemoveProcessor.Factory(TestTemplateService.instance()).create(null, processorTag, null, configWithEmptyField);
        processor.execute(ingestDocument);
    }

    public void testRemoveMetadataField() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        List<String> metadataFields = ingestDocument.getMetadata()
            .keySet()
            .stream()
            .map(IngestDocument.Metadata::getFieldName)
            .collect(Collectors.toList());

        for (String metadataFieldName : metadataFields) {
            Map<String, Object> config = new HashMap<>();
            config.put("field", metadataFieldName);
            String processorTag = randomAlphaOfLength(10);
            Processor processor = new RemoveProcessor.Factory(TestTemplateService.instance()).create(null, processorTag, null, config);
            // _if_seq_no and _if_primary_term do not exist in the enriched document, removing them will throw IllegalArgumentException
            if (metadataFieldName.equals(IngestDocument.Metadata.IF_SEQ_NO.getFieldName())
                || metadataFieldName.equals(IngestDocument.Metadata.IF_PRIMARY_TERM.getFieldName())) {
                assertThrows(
                    "field: [" + metadataFieldName + "] doesn't exist",
                    IllegalArgumentException.class,
                    () -> processor.execute(ingestDocument)
                );
            } else if (metadataFieldName.equals(IngestDocument.Metadata.INDEX.getFieldName())
                || metadataFieldName.equals(IngestDocument.Metadata.VERSION.getFieldName())
                || metadataFieldName.equals(IngestDocument.Metadata.VERSION_TYPE.getFieldName())) {
                    // _index, _version and _version_type cannot be removed
                    assertThrows(
                        "cannot remove metadata field [" + metadataFieldName + "]",
                        IllegalArgumentException.class,
                        () -> processor.execute(ingestDocument)
                    );
                } else if (metadataFieldName.equals(IngestDocument.Metadata.ID.getFieldName())) {
                    Long version = ingestDocument.getFieldValue(IngestDocument.Metadata.VERSION.getFieldName(), Long.class);
                    String versionType = ingestDocument.getFieldValue(IngestDocument.Metadata.VERSION_TYPE.getFieldName(), String.class);
                    if (!versionType.equals(VersionType.toString(VersionType.INTERNAL))) {
                        assertThrows(
                            "cannot remove metadata field [_id] when specifying external version for the document, version: "
                                + version
                                + ", version_type: "
                                + versionType,
                            IllegalArgumentException.class,
                            () -> processor.execute(ingestDocument)
                        );
                    } else {
                        processor.execute(ingestDocument);
                        assertThat(ingestDocument.hasField(metadataFieldName), equalTo(false));
                    }
                } else if (metadataFieldName.equals(IngestDocument.Metadata.ROUTING.getFieldName())
                    && ingestDocument.hasField(IngestDocument.Metadata.ROUTING.getFieldName())) {
                        processor.execute(ingestDocument);
                        assertThat(ingestDocument.hasField(metadataFieldName), equalTo(false));
                    }
        }
    }
}
