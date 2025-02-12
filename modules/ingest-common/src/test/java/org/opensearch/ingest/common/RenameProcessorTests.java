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
import org.opensearch.ingest.TestTemplateService;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class RenameProcessorTests extends OpenSearchTestCase {

    public void testRename() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String fieldName = RandomDocumentPicks.randomExistingFieldName(random(), ingestDocument);
        Object fieldValue = ingestDocument.getFieldValue(fieldName, Object.class);
        String newFieldName;
        do {
            newFieldName = RandomDocumentPicks.randomFieldName(random());
        } while (RandomDocumentPicks.canAddField(newFieldName, ingestDocument) == false || newFieldName.equals(fieldName));
        Processor processor = createRenameProcessor(fieldName, newFieldName, false, false);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(newFieldName, Object.class), equalTo(fieldValue));
    }

    public void testRenameArrayElement() throws Exception {
        Map<String, Object> document = new HashMap<>();
        List<String> list = new ArrayList<>();
        list.add("item1");
        list.add("item2");
        list.add("item3");
        document.put("list", list);
        List<Map<String, String>> one = new ArrayList<>();
        one.add(Collections.singletonMap("one", "one"));
        one.add(Collections.singletonMap("two", "two"));
        document.put("one", one);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        Processor processor = createRenameProcessor("list.0", "item", false, false);
        processor.execute(ingestDocument);
        Object actualObject = ingestDocument.getSourceAndMetadata().get("list");
        assertThat(actualObject, instanceOf(List.class));
        @SuppressWarnings("unchecked")
        List<String> actualList = (List<String>) actualObject;
        assertThat(actualList.size(), equalTo(2));
        assertThat(actualList.get(0), equalTo("item2"));
        assertThat(actualList.get(1), equalTo("item3"));
        actualObject = ingestDocument.getSourceAndMetadata().get("item");
        assertThat(actualObject, instanceOf(String.class));
        assertThat(actualObject, equalTo("item1"));

        processor = createRenameProcessor("list.0", "list.3", false, randomBoolean());
        try {
            processor.execute(ingestDocument);
            fail("processor execute should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("[3] is out of bounds for array with length [2] as part of path [list.3]"));
            assertThat(actualList.size(), equalTo(2));
            assertThat(actualList.get(0), equalTo("item2"));
            assertThat(actualList.get(1), equalTo("item3"));
        }
    }

    public void testRenameNonExistingField() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Processor processor = createRenameProcessor(fieldName, RandomDocumentPicks.randomFieldName(random()), false, false);
        try {
            processor.execute(ingestDocument);
            fail("processor execute should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [" + fieldName + "] doesn't exist"));
        }

        // when using template snippet, the resolved field path maybe empty
        processor = createRenameProcessor("", RandomDocumentPicks.randomFieldName(random()), false, false);
        try {
            processor.execute(ingestDocument);
            fail("processor execute should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field path cannot be null nor empty"));
        }
    }

    public void testRenameNonExistingFieldWithIgnoreMissing() throws Exception {
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Processor processor = createRenameProcessor(fieldName, RandomDocumentPicks.randomFieldName(random()), true, false);
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);

        // when using template snippet, the resolved field path maybe empty
        processor = createRenameProcessor("", RandomDocumentPicks.randomFieldName(random()), true, false);
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testRenameNewFieldAlreadyExists() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        String field = RandomDocumentPicks.randomExistingFieldName(random(), ingestDocument);
        Object fieldValue = ingestDocument.getFieldValue(field, Object.class);
        String targetField = RandomDocumentPicks.addRandomField(random(), ingestDocument, RandomDocumentPicks.randomFieldValue(random()));

        Processor processor = createRenameProcessor(field, targetField, false, false);
        try {
            processor.execute(ingestDocument);
            fail("processor execute should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [" + targetField + "] already exists"));
        }

        Processor processorWithOverrideTarget = createRenameProcessor(field, targetField, false, true);

        processorWithOverrideTarget.execute(ingestDocument);
        assertThat(ingestDocument.hasField(field), equalTo(false));
        assertThat(ingestDocument.hasField(targetField), equalTo(true));
        assertThat(ingestDocument.getFieldValue(targetField, Object.class), equalTo(fieldValue));
    }

    public void testRenameExistingFieldNullValue() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        ingestDocument.setFieldValue(fieldName, null);
        String newFieldName = randomValueOtherThanMany(ingestDocument::hasField, () -> RandomDocumentPicks.randomFieldName(random()));
        Processor processor = createRenameProcessor(fieldName, newFieldName, false, false);
        processor.execute(ingestDocument);
        if (newFieldName.startsWith(fieldName + '.')) {
            assertThat(ingestDocument.getFieldValue(fieldName, Object.class), instanceOf(Map.class));
        } else {
            assertThat(ingestDocument.hasField(fieldName), equalTo(false));
        }
        assertThat(ingestDocument.hasField(newFieldName), equalTo(true));
        assertThat(ingestDocument.getFieldValue(newFieldName, Object.class), nullValue());
    }

    public void testRenameAtomicOperationSetFails() throws Exception {
        Map<String, Object> source = new HashMap<String, Object>() {
            @Override
            public Object put(String key, Object value) {
                if (key.equals("new_field")) {
                    throw new UnsupportedOperationException();
                }
                return super.put(key, value);
            }
        };
        source.put("list", Collections.singletonList("item"));

        IngestDocument ingestDocument = new IngestDocument(source, Collections.emptyMap());
        Processor processor = createRenameProcessor("list", "new_field", false, false);
        try {
            processor.execute(ingestDocument);
            fail("processor execute should have failed");
        } catch (UnsupportedOperationException e) {
            // the set failed, the old field has not been removed
            assertThat(ingestDocument.getSourceAndMetadata().containsKey("list"), equalTo(true));
            assertThat(ingestDocument.getSourceAndMetadata().containsKey("new_field"), equalTo(false));
        }
    }

    public void testRenameAtomicOperationRemoveFails() throws Exception {
        Map<String, Object> source = new HashMap<String, Object>() {
            @Override
            public Object remove(Object key) {
                if (key.equals("list")) {
                    throw new UnsupportedOperationException();
                }
                return super.remove(key);
            }
        };
        source.put("list", Collections.singletonList("item"));

        IngestDocument ingestDocument = new IngestDocument(source, Collections.emptyMap());
        Processor processor = createRenameProcessor("list", "new_field", false, false);
        try {
            processor.execute(ingestDocument);
            fail("processor execute should have failed");
        } catch (UnsupportedOperationException e) {
            // the set failed, the old field has not been removed
            assertThat(ingestDocument.getSourceAndMetadata().containsKey("list"), equalTo(true));
            assertThat(ingestDocument.getSourceAndMetadata().containsKey("new_field"), equalTo(false));
        }
    }

    public void testRenameLeafIntoBranch() throws Exception {
        Map<String, Object> source = new HashMap<>();
        source.put("foo", "bar");
        IngestDocument ingestDocument = new IngestDocument(source, Collections.emptyMap());
        Processor processor1 = createRenameProcessor("foo", "foo.bar", false, false);
        processor1.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue("foo", Map.class), equalTo(Collections.singletonMap("bar", "bar")));
        assertThat(ingestDocument.getFieldValue("foo.bar", String.class), equalTo("bar"));

        Processor processor2 = createRenameProcessor("foo.bar", "foo.bar.baz", false, false);
        processor2.execute(ingestDocument);
        assertThat(
            ingestDocument.getFieldValue("foo", Map.class),
            equalTo(Collections.singletonMap("bar", Collections.singletonMap("baz", "bar")))
        );
        assertThat(ingestDocument.getFieldValue("foo.bar", Map.class), equalTo(Collections.singletonMap("baz", "bar")));
        assertThat(ingestDocument.getFieldValue("foo.bar.baz", String.class), equalTo("bar"));

        // for fun lets try to restore it (which don't allow today)
        Processor processor3 = createRenameProcessor("foo.bar.baz", "foo", false, false);
        Exception e = expectThrows(IllegalArgumentException.class, () -> processor3.execute(ingestDocument));
        assertThat(e.getMessage(), equalTo("field [foo] already exists"));
    }

    private RenameProcessor createRenameProcessor(String field, String targetField, boolean ignoreMissing, boolean overrideTarget) {
        return new RenameProcessor(
            randomAlphaOfLength(10),
            null,
            new TestTemplateService.MockTemplateScript.Factory(field),
            new TestTemplateService.MockTemplateScript.Factory(targetField),
            ignoreMissing,
            overrideTarget
        );
    }
}
