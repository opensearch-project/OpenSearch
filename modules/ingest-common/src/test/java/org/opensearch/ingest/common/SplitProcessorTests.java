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

public class SplitProcessorTests extends OpenSearchTestCase {

    public void testSplit() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "127.0.0.1");
        Processor processor = new SplitProcessor(randomAlphaOfLength(10), null, fieldName, "\\.", false, false, fieldName);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, List.class), equalTo(Arrays.asList("127", "0", "0", "1")));
    }

    public void testSplitFieldNotFound() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Processor processor = new SplitProcessor(randomAlphaOfLength(10), null, fieldName, "\\.", false, false, fieldName);
        try {
            processor.execute(ingestDocument);
            fail("split processor should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("not present as part of path [" + fieldName + "]"));
        }
    }

    public void testSplitNullValue() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.singletonMap("field", null));
        Processor processor = new SplitProcessor(randomAlphaOfLength(10), null, "field", "\\.", false, false, "field");
        try {
            processor.execute(ingestDocument);
            fail("split processor should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [field] is null, cannot split."));
        }
    }

    public void testSplitNullValueWithIgnoreMissing() throws Exception {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(
            random(),
            Collections.singletonMap(fieldName, null)
        );
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        Processor processor = new SplitProcessor(randomAlphaOfLength(10), null, fieldName, "\\.", true, false, fieldName);
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testSplitNonExistentWithIgnoreMissing() throws Exception {
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.emptyMap());
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        Processor processor = new SplitProcessor(randomAlphaOfLength(10), null, "field", "\\.", true, false, "field");
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testSplitNonStringValue() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        ingestDocument.setFieldValue(fieldName, randomInt());
        Processor processor = new SplitProcessor(randomAlphaOfLength(10), null, fieldName, "\\.", false, false, fieldName);
        try {
            processor.execute(ingestDocument);
            fail("split processor should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(
                e.getMessage(),
                equalTo("field [" + fieldName + "] of type [java.lang.Integer] cannot be cast " + "to [java.lang.String]")
            );
        }
    }

    public void testSplitAppendable() throws Exception {
        Map<String, Object> splitConfig = new HashMap<>();
        splitConfig.put("field", "flags");
        splitConfig.put("separator", "\\|");
        Processor splitProcessor = (new SplitProcessor.Factory()).create(null, null, null, splitConfig);
        Map<String, Object> source = new HashMap<>();
        source.put("flags", "new|hot|super|fun|interesting");
        IngestDocument ingestDocument = new IngestDocument(source, new HashMap<>());
        splitProcessor.execute(ingestDocument);
        @SuppressWarnings("unchecked")
        List<String> flags = (List<String>) ingestDocument.getFieldValue("flags", List.class);
        assertThat(flags, equalTo(Arrays.asList("new", "hot", "super", "fun", "interesting")));
        ingestDocument.appendFieldValue("flags", "additional_flag");
        assertThat(
            ingestDocument.getFieldValue("flags", List.class),
            equalTo(Arrays.asList("new", "hot", "super", "fun", "interesting", "additional_flag"))
        );
    }

    public void testSplitWithTargetField() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, "127.0.0.1");
        String targetFieldName = fieldName + randomAlphaOfLength(5);
        Processor processor = new SplitProcessor(randomAlphaOfLength(10), null, fieldName, "\\.", false, false, targetFieldName);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(targetFieldName, List.class), equalTo(Arrays.asList("127", "0", "0", "1")));
    }

    public void testSplitWithPreserveTrailing() throws Exception {
        doTestSplitWithPreserveTrailing(true, "foo|bar|baz||", Arrays.asList("foo", "bar", "baz", "", ""));
    }

    public void testSplitWithoutPreserveTrailing() throws Exception {
        doTestSplitWithPreserveTrailing(false, "foo|bar|baz||", Arrays.asList("foo", "bar", "baz"));
    }

    private void doTestSplitWithPreserveTrailing(boolean preserveTrailing, String fieldValue, List<String> expected) throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);
        Processor processor = new SplitProcessor(randomAlphaOfLength(10), null, fieldName, "\\|", false, preserveTrailing, fieldName);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, List.class), equalTo(expected));
    }
}
