/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.common;

import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Processor;
import org.opensearch.ingest.RandomDocumentPicks;
import org.opensearch.ingest.TestTemplateService;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class CopyProcessorTests extends OpenSearchTestCase {

    public void testCopyExistingField() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String sourceFieldName = RandomDocumentPicks.randomExistingFieldName(random(), ingestDocument);
        String targetFieldName = RandomDocumentPicks.randomNonExistingFieldName(random(), ingestDocument);
        Processor processor = createCopyProcessor(sourceFieldName, targetFieldName, false, false, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.hasField(targetFieldName), equalTo(true));
        Object sourceValue = ingestDocument.getFieldValue(sourceFieldName, Object.class);
        assertDeepCopiedObjectEquals(ingestDocument.getFieldValue(targetFieldName, Object.class), sourceValue);

        Processor processorWithEmptyTarget = createCopyProcessor(sourceFieldName, "", false, false, false);
        assertThrows(
            "target field path cannot be null nor empty",
            IllegalArgumentException.class,
            () -> processorWithEmptyTarget.execute(ingestDocument)
        );

        Processor processorWithSameSourceAndTarget = createCopyProcessor(sourceFieldName, sourceFieldName, false, false, false);
        assertThrows(
            "source field path and target field path cannot be same",
            IllegalArgumentException.class,
            () -> processorWithSameSourceAndTarget.execute(ingestDocument)
        );
    }

    public void testCopyWithIgnoreMissing() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String targetFieldName = RandomDocumentPicks.randomFieldName(random());
        Processor processor = createCopyProcessor("non-existing-field", targetFieldName, false, false, false);
        assertThrows(
            "source field [non-existing-field] doesn't exist",
            IllegalArgumentException.class,
            () -> processor.execute(ingestDocument)
        );

        Processor processorWithEmptyFieldName = createCopyProcessor("", targetFieldName, false, false, false);
        assertThrows(
            "source field path cannot be null nor empty",
            IllegalArgumentException.class,
            () -> processorWithEmptyFieldName.execute(ingestDocument)
        );

        Processor processorWithIgnoreMissing = createCopyProcessor("non-existing-field", targetFieldName, true, false, false);
        processorWithIgnoreMissing.execute(ingestDocument);
        assertThat(ingestDocument.hasField(targetFieldName), equalTo(false));
    }

    public void testCopyWithRemoveSource() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String sourceFieldName = RandomDocumentPicks.randomExistingFieldName(random(), ingestDocument);
        String targetFieldName = RandomDocumentPicks.randomNonExistingFieldName(random(), ingestDocument);

        Object sourceValue = ingestDocument.getFieldValue(sourceFieldName, Object.class);

        Processor processor = createCopyProcessor(sourceFieldName, targetFieldName, false, true, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.hasField(targetFieldName), equalTo(true));
        assertDeepCopiedObjectEquals(ingestDocument.getFieldValue(targetFieldName, Object.class), sourceValue);
        assertThat(ingestDocument.hasField(sourceFieldName), equalTo(false));
    }

    public void testCopyToExistingField() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String targetFieldName = RandomDocumentPicks.randomExistingFieldName(random(), ingestDocument);
        Object sourceValue = RandomDocumentPicks.randomFieldValue(random());
        String sourceFieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, sourceValue);

        Processor processor = createCopyProcessor(sourceFieldName, targetFieldName, false, false, false);
        assertThrows(
            "target field [" + targetFieldName + "] already exists",
            IllegalArgumentException.class,
            () -> processor.execute(ingestDocument)
        );

        // if override_target is false but target field's value is null, copy can execute successfully
        String targetFieldWithNullValue = RandomDocumentPicks.addRandomField(random(), ingestDocument, null);
        Processor processorWithTargetNullValue = createCopyProcessor(sourceFieldName, targetFieldWithNullValue, false, false, false);
        processorWithTargetNullValue.execute(ingestDocument);
        assertThat(ingestDocument.hasField(targetFieldWithNullValue), equalTo(true));
        assertDeepCopiedObjectEquals(ingestDocument.getFieldValue(targetFieldWithNullValue, Object.class), sourceValue);

        Processor processorWithOverrideTargetIsTrue = createCopyProcessor(sourceFieldName, targetFieldName, false, false, true);
        processorWithOverrideTargetIsTrue.execute(ingestDocument);
        assertThat(ingestDocument.hasField(targetFieldName), equalTo(true));
        assertDeepCopiedObjectEquals(ingestDocument.getFieldValue(targetFieldName, Object.class), sourceValue);
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

    private static Processor createCopyProcessor(
        String sourceFieldName,
        String targetFieldName,
        boolean ignoreMissing,
        boolean removeSource,
        boolean overrideTarget
    ) {
        return new CopyProcessor(
            randomAlphaOfLength(10),
            null,
            new TestTemplateService.MockTemplateScript.Factory(sourceFieldName),
            new TestTemplateService.MockTemplateScript.Factory(targetFieldName),
            ignoreMissing,
            removeSource,
            overrideTarget
        );
    }
}
