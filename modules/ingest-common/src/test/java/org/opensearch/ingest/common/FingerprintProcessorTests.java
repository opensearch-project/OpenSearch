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
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class FingerprintProcessorTests extends OpenSearchTestCase {
    private final List<String> hashMethods = List.of("MD5@2.16.0", "SHA-1@2.16.0", "SHA-256@2.16.0", "SHA3-256@2.16.0");

    public void testGenerateFingerprint() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        List<String> fields = null;
        List<String> excludeFields = null;
        if (randomBoolean()) {
            fields = new ArrayList<>();
            for (int i = 0; i < randomIntBetween(1, 10); i++) {
                fields.add(RandomDocumentPicks.addRandomField(random(), ingestDocument, randomAlphaOfLength(10)));
            }
        } else {
            excludeFields = new ArrayList<>();
            for (int i = 0; i < randomIntBetween(1, 10); i++) {
                excludeFields.add(RandomDocumentPicks.addRandomField(random(), ingestDocument, randomAlphaOfLength(10)));
            }
        }

        String targetField = "fingerprint";
        if (randomBoolean()) {
            targetField = randomAlphaOfLength(10);
        }

        String hashMethod = randomFrom(hashMethods);
        Processor processor = createFingerprintProcessor(fields, excludeFields, targetField, hashMethod, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.hasField(targetField), equalTo(true));
    }

    public void testCreateFingerprintProcessorFailed() {
        List<String> fields = new ArrayList<>();
        if (randomBoolean()) {
            fields.add(null);
        } else {
            fields.add("");
        }
        fields.add(randomAlphaOfLength(10));

        assertThrows(
            "field name in [fields] cannot be null nor empty",
            IllegalArgumentException.class,
            () -> createFingerprintProcessor(fields, null, null, randomFrom(hashMethods), false)
        );

        List<String> excludeFields = new ArrayList<>();
        if (randomBoolean()) {
            excludeFields.add(null);
        } else {
            excludeFields.add("");
        }
        excludeFields.add(randomAlphaOfLength(10));

        assertThrows(
            "field name in [exclude_fields] cannot be null nor empty",
            IllegalArgumentException.class,
            () -> createFingerprintProcessor(null, excludeFields, null, randomFrom(hashMethods), false)
        );

        assertThrows(
            "either fields or exclude_fields can be set",
            IllegalArgumentException.class,
            () -> createFingerprintProcessor(
                List.of(randomAlphaOfLength(10)),
                List.of(randomAlphaOfLength(10)),
                null,
                randomFrom(hashMethods),
                false
            )
        );

        assertThrows(
            "hash method must be MD5@2.16.0, SHA-1@2.16.0, SHA-256@2.16.0 or SHA3-256@2.16.0",
            IllegalArgumentException.class,
            () -> createFingerprintProcessor(Collections.emptyList(), null, "fingerprint", randomAlphaOfLength(10), false)
        );
    }

    public void testEmptyFieldAndExcludeFields() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        List<String> fields = null;
        List<String> excludeFields = null;
        if (randomBoolean()) {
            fields = new ArrayList<>();
        } else {
            excludeFields = new ArrayList<>();
        }
        String targetField = "fingerprint";
        if (randomBoolean()) {
            targetField = randomAlphaOfLength(10);
        }

        String hashMethod = randomFrom(hashMethods);
        Processor processor = createFingerprintProcessor(fields, excludeFields, targetField, hashMethod, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.hasField(targetField), equalTo(true));
    }

    public void testIgnoreMissing() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String nonExistingFieldName = RandomDocumentPicks.randomNonExistingFieldName(random(), ingestDocument);
        List<String> nonExistingFields = List.of(nonExistingFieldName);
        Processor processor = createFingerprintProcessor(nonExistingFields, null, "fingerprint", randomFrom(hashMethods), false);
        assertThrows(
            "field [" + nonExistingFieldName + "] doesn't exist",
            IllegalArgumentException.class,
            () -> processor.execute(ingestDocument)
        );

        String targetField = "fingerprint";
        Processor processorWithIgnoreMissing = createFingerprintProcessor(
            nonExistingFields,
            null,
            "fingerprint",
            randomFrom(hashMethods),
            true
        );
        processorWithIgnoreMissing.execute(ingestDocument);
        assertThat(ingestDocument.hasField(targetField), equalTo(false));
    }

    public void testIgnoreMetadataFields() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        List<String> metadataFields = ingestDocument.getMetadata()
            .keySet()
            .stream()
            .map(IngestDocument.Metadata::getFieldName)
            .collect(Collectors.toList());

        String existingFieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, randomAlphaOfLength(10));
        List<String> fields = List.of(existingFieldName, metadataFields.get(randomIntBetween(0, metadataFields.size() - 1)));

        String targetField = "fingerprint";
        String algorithm = randomFrom(hashMethods);
        Processor processor = createFingerprintProcessor(fields, null, targetField, algorithm, false);

        processor.execute(ingestDocument);
        String fingerprint = ingestDocument.getFieldValue(targetField, String.class);

        processor = createFingerprintProcessor(List.of(existingFieldName), null, targetField, algorithm, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(targetField, String.class), equalTo(fingerprint));
    }

    private FingerprintProcessor createFingerprintProcessor(
        List<String> fields,
        List<String> excludeFields,
        String targetField,
        String hashMethod,
        boolean ignoreMissing
    ) {
        return new FingerprintProcessor(randomAlphaOfLength(10), null, fields, excludeFields, targetField, hashMethod, ignoreMissing);
    }
}
