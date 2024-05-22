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
    private final List<String> hashMethods = List.of("MD5", "SHA-1", "SHA-256", "SHA3-256");

    public void testGenerateFingerprint() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        List<String> fields = null;
        boolean includeAllFields = false;
        if (randomBoolean()) {
            includeAllFields = true;

        } else {
            fields = new ArrayList<>();
            for (int i = 0; i < randomIntBetween(1, 10); i++) {
                fields.add(RandomDocumentPicks.addRandomField(random(), ingestDocument, randomAlphaOfLength(10)));
            }
        }

        String targetField = "fingerprint";
        if (randomBoolean()) {
            targetField = randomAlphaOfLength(10);
        }

        String hashMethod = randomFrom(hashMethods);
        Processor processor = createFingerprintProcessor(fields, includeAllFields, targetField, hashMethod, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.hasField(targetField), equalTo(true));
    }

    public void testCreateFingerprintProcessorFailed() {
        assertThrows(
            "fields cannot be empty",
            IllegalArgumentException.class,
            () -> createFingerprintProcessor(Collections.emptyList(), false, "fingerprint", randomFrom(hashMethods), false)
        );

        List<String> fields = new ArrayList<>();
        fields.add(null);
        fields.add(randomAlphaOfLength(10));
        assertThrows(
            "field path cannot be null nor empty",
            IllegalArgumentException.class,
            () -> createFingerprintProcessor(fields, false, null, randomFrom(List.of("MD5", "SHA-1", "SHA-256", "SHA3-256")), false)
        );

        assertThrows(
            "hash method must be MD5, SHA-1, SHA-256 or SHA3-256",
            IllegalArgumentException.class,
            () -> createFingerprintProcessor(Collections.emptyList(), false, "fingerprint", randomFrom(hashMethods), false)
        );

        assertThrows(
            "either fields or include_all_fields can be set",
            IllegalArgumentException.class,
            () -> createFingerprintProcessor(Collections.emptyList(), true, "fingerprint", randomFrom(hashMethods), false)
        );

        assertThrows(
            "either fields or include_all_fields must be set",
            IllegalArgumentException.class,
            () -> createFingerprintProcessor(null, false, "fingerprint", randomFrom(hashMethods), false)
        );
    }

    public void testIncludeAllFields() {
        List<String> fields = new ArrayList<>();
        fields.add(null);
        fields.add(randomAlphaOfLength(10));
        assertThrows(
            "field path cannot be null nor empty",
            IllegalArgumentException.class,
            () -> createFingerprintProcessor(fields, false, null, randomFrom(hashMethods), false)
        );
    }

    public void testIgnoreMissing() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String nonExistingFieldName = RandomDocumentPicks.randomNonExistingFieldName(random(), ingestDocument);
        List<String> nonExistingFields = List.of(nonExistingFieldName);
        Processor processor = createFingerprintProcessor(nonExistingFields, false, "fingerprint", randomFrom(hashMethods), false);
        assertThrows(
            "field [" + nonExistingFieldName + "] doesn't exist",
            IllegalArgumentException.class,
            () -> processor.execute(ingestDocument)
        );

        String targetField = "fingerprint";
        Processor processorWithIgnoreMissing = createFingerprintProcessor(
            nonExistingFields,
            false,
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
        Processor processor = createFingerprintProcessor(fields, false, targetField, algorithm, false);

        processor.execute(ingestDocument);
        String fingerprint = ingestDocument.getFieldValue(targetField, String.class);

        processor = createFingerprintProcessor(List.of(existingFieldName), false, targetField, algorithm, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(targetField, String.class), equalTo(fingerprint));
    }

    private FingerprintProcessor createFingerprintProcessor(
        List<String> fields,
        boolean includeAllFields,
        String targetField,
        String hashMethod,
        boolean ignoreMissing
    ) {
        return new FingerprintProcessor(randomAlphaOfLength(10), null, fields, includeAllFields, targetField, hashMethod, ignoreMissing);
    }
}
