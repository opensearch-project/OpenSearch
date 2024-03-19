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
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class RemoveByPatternProcessorTests extends OpenSearchTestCase {

    public void testRemoveWithFieldPatterns() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        ingestDocument.setFieldValue("foo_1", "value");
        ingestDocument.setFieldValue("foo_2", "value");
        ingestDocument.setFieldValue("bar_1", "value");
        ingestDocument.setFieldValue("bar_2", "value");
        List<String> fieldPatterns = new ArrayList<>();
        fieldPatterns.add("foo*");
        fieldPatterns.add("_index*");
        fieldPatterns.add("_id*");
        fieldPatterns.add("_version*");
        Processor processor = new RemoveByPatternProcessor(randomAlphaOfLength(10), null, fieldPatterns, null);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.hasField("foo_1"), equalTo(false));
        assertThat(ingestDocument.hasField("foo_2"), equalTo(false));
        assertThat(ingestDocument.hasField("bar_1"), equalTo(true));
        assertThat(ingestDocument.hasField("bar_2"), equalTo(true));
        assertThat(ingestDocument.hasField(IngestDocument.Metadata.INDEX.getFieldName()), equalTo(true));
        assertThat(ingestDocument.hasField(IngestDocument.Metadata.ID.getFieldName()), equalTo(true));
        assertThat(ingestDocument.hasField(IngestDocument.Metadata.VERSION.getFieldName()), equalTo(true));
        assertThat(ingestDocument.hasField(IngestDocument.Metadata.VERSION_TYPE.getFieldName()), equalTo(true));
    }

    public void testRemoveWithExcludeFieldPatterns() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        ingestDocument.setFieldValue("foo_1", "value");
        ingestDocument.setFieldValue("foo_2", "value");
        ingestDocument.setFieldValue("foo_3", "value");
        List<String> excludeFieldPatterns = new ArrayList<>();
        excludeFieldPatterns.add("foo_3*");
        Processor processorWithExcludeFieldsAndPatterns = new RemoveByPatternProcessor(
            randomAlphaOfLength(10),
            null,
            null,
            excludeFieldPatterns
        );
        processorWithExcludeFieldsAndPatterns.execute(ingestDocument);
        assertThat(ingestDocument.hasField("foo_1"), equalTo(false));
        assertThat(ingestDocument.hasField("foo_2"), equalTo(false));
        assertThat(ingestDocument.hasField("foo_3"), equalTo(true));
        assertThat(ingestDocument.hasField(IngestDocument.Metadata.INDEX.getFieldName()), equalTo(true));
        assertThat(ingestDocument.hasField(IngestDocument.Metadata.ID.getFieldName()), equalTo(true));
        assertThat(ingestDocument.hasField(IngestDocument.Metadata.VERSION.getFieldName()), equalTo(true));
        assertThat(ingestDocument.hasField(IngestDocument.Metadata.VERSION_TYPE.getFieldName()), equalTo(true));
    }

    public void testCreateRemoveByPatternProcessorWithBothFieldsAndExcludeFields() throws Exception {
        assertThrows(
            "either fieldPatterns and excludeFieldPatterns must be set",
            IllegalArgumentException.class,
            () -> new RemoveByPatternProcessor(randomAlphaOfLength(10), null, null, null)
        );

        final List<String> fieldPatterns;
        if (randomBoolean()) {
            fieldPatterns = new ArrayList<>();
        } else {
            fieldPatterns = List.of("foo_1*");
        }

        final List<String> excludeFieldPatterns;
        if (randomBoolean()) {
            excludeFieldPatterns = new ArrayList<>();
        } else {
            excludeFieldPatterns = List.of("foo_2*");
        }

        assertThrows(
            "either fieldPatterns and excludeFieldPatterns must be set",
            IllegalArgumentException.class,
            () -> new RemoveByPatternProcessor(randomAlphaOfLength(10), null, fieldPatterns, excludeFieldPatterns)
        );
    }
}
