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

import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.IngestDocument.Metadata;
import org.opensearch.ingest.Processor;
import org.opensearch.ingest.RandomDocumentPicks;
import org.opensearch.ingest.TestTemplateService;
import org.opensearch.ingest.ValueSource;
import org.opensearch.test.OpenSearchTestCase;
import org.hamcrest.Matchers;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class SetProcessorTests extends OpenSearchTestCase {

    public void testSetExistingFields() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.randomExistingFieldName(random(), ingestDocument);
        Object fieldValue = RandomDocumentPicks.randomFieldValue(random());
        Processor processor = createSetProcessor(fieldName, fieldValue, true, false, null);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.hasField(fieldName), equalTo(true));
        assertThat(ingestDocument.getFieldValue(fieldName, Object.class), equalTo(fieldValue));
    }

    public void testSetNewFields() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        // used to verify that there are no conflicts between subsequent fields going to be added
        IngestDocument testIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        Object fieldValue = RandomDocumentPicks.randomFieldValue(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), testIngestDocument, fieldValue);
        Processor processor = createSetProcessor(fieldName, fieldValue, true, false, null);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.hasField(fieldName), equalTo(true));
        assertThat(ingestDocument.getFieldValue(fieldName, Object.class), equalTo(fieldValue));
    }

    public void testSetFieldsTypeMismatch() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        ingestDocument.setFieldValue("field", "value");
        Processor processor = createSetProcessor("field.inner", "value", true, false, null);
        try {
            processor.execute(ingestDocument);
            fail("processor execute should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(
                e.getMessage(),
                equalTo("cannot set [inner] with parent object of type [java.lang.String] as " + "part of path [field.inner]")
            );
        }
    }

    public void testSetNewFieldWithOverrideDisabled() throws Exception {
        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Object fieldValue = RandomDocumentPicks.randomFieldValue(random());
        Processor processor = createSetProcessor(fieldName, fieldValue, false, false, null);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.hasField(fieldName), equalTo(true));
        assertThat(ingestDocument.getFieldValue(fieldName, Object.class), equalTo(fieldValue));
    }

    public void testSetExistingFieldWithOverrideDisabled() throws Exception {
        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        Object fieldValue = "foo";
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);
        Processor processor = createSetProcessor(fieldName, "bar", false, false, null);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.hasField(fieldName), equalTo(true));
        assertThat(ingestDocument.getFieldValue(fieldName, Object.class), equalTo(fieldValue));
    }

    public void testSetExistingNullFieldWithOverrideDisabled() throws Exception {
        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        Object fieldValue = null;
        Object newValue = "bar";
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);
        Processor processor = createSetProcessor(fieldName, newValue, false, false, null);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.hasField(fieldName), equalTo(true));
        assertThat(ingestDocument.getFieldValue(fieldName, Object.class), equalTo(newValue));
    }

    public void testSetMetadataExceptVersion() throws Exception {
        Metadata randomMetadata = randomFrom(Metadata.INDEX, Metadata.ID, Metadata.ROUTING);
        Processor processor = createSetProcessor(randomMetadata.getFieldName(), "_value", true, false, null);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(randomMetadata.getFieldName(), String.class), Matchers.equalTo("_value"));
    }

    public void testSetMetadataVersion() throws Exception {
        long version = randomNonNegativeLong();
        Processor processor = createSetProcessor(Metadata.VERSION.getFieldName(), version, true, false, null);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(Metadata.VERSION.getFieldName(), Long.class), Matchers.equalTo(version));
    }

    public void testSetMetadataVersionType() throws Exception {
        String versionType = randomFrom("internal", "external", "external_gte");
        Processor processor = createSetProcessor(Metadata.VERSION_TYPE.getFieldName(), versionType, true, false, null);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(Metadata.VERSION_TYPE.getFieldName(), String.class), Matchers.equalTo(versionType));
    }

    public void testSetMetadataIfSeqNo() throws Exception {
        long ifSeqNo = randomNonNegativeLong();
        Processor processor = createSetProcessor(Metadata.IF_SEQ_NO.getFieldName(), ifSeqNo, true, false, null);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(Metadata.IF_SEQ_NO.getFieldName(), Long.class), Matchers.equalTo(ifSeqNo));
    }

    public void testSetMetadataIfPrimaryTerm() throws Exception {
        long ifPrimaryTerm = randomNonNegativeLong();
        Processor processor = createSetProcessor(Metadata.IF_PRIMARY_TERM.getFieldName(), ifPrimaryTerm, true, false, null);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(Metadata.IF_PRIMARY_TERM.getFieldName(), Long.class), Matchers.equalTo(ifPrimaryTerm));
    }

    public void testCopyFromWithIgnoreEmptyValue() throws Exception {
        // do nothing if copy_from field does not exist
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String newFieldName = RandomDocumentPicks.randomFieldName(random());
        Processor processor = createSetProcessor(newFieldName, null, true, true, RandomDocumentPicks.randomFieldName(random()));
        processor.execute(ingestDocument);
        assertFalse(ingestDocument.hasField(newFieldName));

        // throw illegalArgumentException if copy_from is empty string
        Processor processorWithEmptyCopyFrom = createSetProcessor(newFieldName, null, true, true, "");
        assertThrows("copy_from cannot be empty", IllegalArgumentException.class, () -> processorWithEmptyCopyFrom.execute(ingestDocument));
    }

    public void testCopyFromOtherField() throws Exception {
        // can copy different types of data from one field to another
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        Object existingFieldValue = RandomDocumentPicks.randomFieldValue(random());
        String existingFieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, existingFieldValue);
        String newFieldName = RandomDocumentPicks.randomFieldName(random());
        Processor processor = createSetProcessor(newFieldName, null, true, false, existingFieldName);
        processor.execute(ingestDocument);
        assertTrue(ingestDocument.hasField(newFieldName));
        assertDeepCopiedObjectEquals(ingestDocument.getFieldValue(newFieldName, Object.class), existingFieldValue);
    }

    @SuppressWarnings("unchecked")
    private static void assertDeepCopiedObjectEquals(Object expected, Object actual) {
        if (expected instanceof Map) {
            Map<String, Object> expectedMap = (Map<String, Object>) expected;
            Map<String, Object> actualMap = (Map<String, Object>) actual;
            assertEquals(expectedMap.size(), actualMap.size());
            for (Map.Entry<String, Object> expectedEntry : expectedMap.entrySet()) {
                assertDeepCopiedObjectEquals(expectedEntry.getValue(), actualMap.get(expectedEntry.getKey()));
            }
        } else if (expected instanceof List) {
            assertArrayEquals(((List<?>) expected).toArray(), ((List<?>) actual).toArray());
        } else if (expected instanceof byte[]) {
            assertArrayEquals((byte[]) expected, (byte[]) actual);
        } else {
            assertEquals(expected, actual);
        }
    }

    private static Processor createSetProcessor(
        String fieldName,
        Object fieldValue,
        boolean overrideEnabled,
        boolean ignoreEmptyValue,
        String copyFrom
    ) {
        return new SetProcessor(
            randomAlphaOfLength(10),
            null,
            new TestTemplateService.MockTemplateScript.Factory(fieldName),
            ValueSource.wrap(fieldValue, TestTemplateService.instance()),
            overrideEnabled,
            ignoreEmptyValue,
            copyFrom
        );
    }
}
