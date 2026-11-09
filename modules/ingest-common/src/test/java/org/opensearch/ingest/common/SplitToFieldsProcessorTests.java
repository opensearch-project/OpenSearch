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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SplitToFieldsProcessorTests extends OpenSearchTestCase {

    public void testSplitToFields() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "a>b>c");
        List<String> targetFields = Arrays.asList("out1", "out2", "out3");
        Processor processor = new SplitToFieldsProcessor(randomAlphaOfLength(10), null, fieldName, ">", targetFields, false, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("out1", String.class), equalTo("a"));
        assertThat(ingestDocument.getFieldValue("out2", String.class), equalTo("b"));
        assertThat(ingestDocument.getFieldValue("out3", String.class), equalTo("c"));
    }

    public void testSplitToFieldsWithRegexSeparator() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "127.0.0.1");
        List<String> targetFields = Arrays.asList("octet1", "octet2", "octet3", "octet4");
        Processor processor = new SplitToFieldsProcessor(randomAlphaOfLength(10), null, fieldName, "\\.", targetFields, false, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("octet1", String.class), equalTo("127"));
        assertThat(ingestDocument.getFieldValue("octet2", String.class), equalTo("0"));
        assertThat(ingestDocument.getFieldValue("octet3", String.class), equalTo("0"));
        assertThat(ingestDocument.getFieldValue("octet4", String.class), equalTo("1"));
    }

    public void testSplitToFieldsMorePartsThanFields() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "a>b>c>d>e");
        List<String> targetFields = Arrays.asList("out1", "out2", "out3");
        Processor processor = new SplitToFieldsProcessor(randomAlphaOfLength(10), null, fieldName, ">", targetFields, false, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("out1", String.class), equalTo("a"));
        assertThat(ingestDocument.getFieldValue("out2", String.class), equalTo("b"));
        assertThat(ingestDocument.getFieldValue("out3", String.class), equalTo("c"));
    }

    public void testSplitToFieldsFewerPartsThanFields() throws Exception {
        Map<String, Object> source = new HashMap<>();
        source.put("input", "a>b");
        IngestDocument ingestDocument = new IngestDocument(source, new HashMap<>());
        List<String> targetFields = Arrays.asList("out1", "out2", "out3");
        Processor processor = new SplitToFieldsProcessor(randomAlphaOfLength(10), null, "input", ">", targetFields, false, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("out1", String.class), equalTo("a"));
        assertThat(ingestDocument.getFieldValue("out2", String.class), equalTo("b"));
        assertFalse(ingestDocument.hasField("out3"));
    }

    public void testSplitToFieldsFieldNotFound() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        List<String> targetFields = Arrays.asList("out1", "out2");
        Processor processor = new SplitToFieldsProcessor(randomAlphaOfLength(10), null, fieldName, ">", targetFields, false, false);
        try {
            processor.execute(ingestDocument);
            fail("split_to_fields processor should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("not present as part of path [" + fieldName + "]"));
        }
    }

    public void testSplitToFieldsNullValue() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.singletonMap("field", null));
        List<String> targetFields = Arrays.asList("out1", "out2");
        Processor processor = new SplitToFieldsProcessor(randomAlphaOfLength(10), null, "field", ">", targetFields, false, false);
        try {
            processor.execute(ingestDocument);
            fail("split_to_fields processor should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [field] is null, cannot split."));
        }
    }

    public void testSplitToFieldsNullValueWithIgnoreMissing() throws Exception {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(
            random(),
            Collections.singletonMap(fieldName, null)
        );
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        List<String> targetFields = Arrays.asList("out1", "out2");
        Processor processor = new SplitToFieldsProcessor(randomAlphaOfLength(10), null, fieldName, ">", targetFields, true, false);
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testSplitToFieldsNonExistentWithIgnoreMissing() throws Exception {
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.emptyMap());
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        List<String> targetFields = Arrays.asList("out1", "out2");
        Processor processor = new SplitToFieldsProcessor(randomAlphaOfLength(10), null, "field", ">", targetFields, true, false);
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testSplitToFieldsNonStringValue() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        ingestDocument.setFieldValue(fieldName, randomInt());
        List<String> targetFields = Arrays.asList("out1", "out2");
        Processor processor = new SplitToFieldsProcessor(randomAlphaOfLength(10), null, fieldName, ">", targetFields, false, false);
        try {
            processor.execute(ingestDocument);
            fail("split_to_fields processor should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(
                e.getMessage(),
                equalTo("field [" + fieldName + "] of type [java.lang.Integer] cannot be cast to [java.lang.String]")
            );
        }
    }

    public void testSplitToFieldsSinglePart() throws Exception {
        Map<String, Object> source = new HashMap<>();
        source.put("input", "only_one");
        IngestDocument ingestDocument = new IngestDocument(source, new HashMap<>());
        List<String> targetFields = Arrays.asList("out1", "out2", "out3");
        Processor processor = new SplitToFieldsProcessor(randomAlphaOfLength(10), null, "input", ">", targetFields, false, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("out1", String.class), equalTo("only_one"));
        assertFalse(ingestDocument.hasField("out2"));
        assertFalse(ingestDocument.hasField("out3"));
    }

    public void testSplitToFieldsWithPreserveTrailing() throws Exception {
        Map<String, Object> source = new HashMap<>();
        source.put("input", "a|b|c||");
        IngestDocument ingestDocument = new IngestDocument(source, new HashMap<>());
        List<String> targetFields = Arrays.asList("out1", "out2", "out3", "out4", "out5");
        Processor processor = new SplitToFieldsProcessor(randomAlphaOfLength(10), null, "input", "\\|", targetFields, false, true);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("out1", String.class), equalTo("a"));
        assertThat(ingestDocument.getFieldValue("out2", String.class), equalTo("b"));
        assertThat(ingestDocument.getFieldValue("out3", String.class), equalTo("c"));
        assertThat(ingestDocument.getFieldValue("out4", String.class), equalTo(""));
        assertThat(ingestDocument.getFieldValue("out5", String.class), equalTo(""));
    }

    public void testSplitToFieldsWithoutPreserveTrailing() throws Exception {
        Map<String, Object> source = new HashMap<>();
        source.put("input", "a|b|c||");
        IngestDocument ingestDocument = new IngestDocument(source, new HashMap<>());
        List<String> targetFields = Arrays.asList("out1", "out2", "out3", "out4", "out5");
        Processor processor = new SplitToFieldsProcessor(randomAlphaOfLength(10), null, "input", "\\|", targetFields, false, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("out1", String.class), equalTo("a"));
        assertThat(ingestDocument.getFieldValue("out2", String.class), equalTo("b"));
        assertThat(ingestDocument.getFieldValue("out3", String.class), equalTo("c"));
        assertFalse(ingestDocument.hasField("out4"));
        assertFalse(ingestDocument.hasField("out5"));
    }
}
