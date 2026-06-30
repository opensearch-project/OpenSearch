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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class JoinProcessorTests extends OpenSearchTestCase {

    private static final String[] SEPARATORS = new String[] { "-", "_", "." };

    public void testJoinStrings() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        int numItems = randomIntBetween(1, 10);
        String separator = randomFrom(SEPARATORS);
        List<String> fieldValue = new ArrayList<>(numItems);
        String expectedResult = "";
        for (int j = 0; j < numItems; j++) {
            String value = randomAlphaOfLengthBetween(1, 10);
            fieldValue.add(value);
            expectedResult += value;
            if (j < numItems - 1) {
                expectedResult += separator;
            }
        }
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);
        Processor processor = new JoinProcessor(randomAlphaOfLength(10), null, fieldName, separator, fieldName);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, String.class), equalTo(expectedResult));
    }

    public void testJoinIntegers() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        int numItems = randomIntBetween(1, 10);
        String separator = randomFrom(SEPARATORS);
        List<Integer> fieldValue = new ArrayList<>(numItems);
        String expectedResult = "";
        for (int j = 0; j < numItems; j++) {
            int value = randomInt();
            fieldValue.add(value);
            expectedResult += value;
            if (j < numItems - 1) {
                expectedResult += separator;
            }
        }
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);
        Processor processor = new JoinProcessor(randomAlphaOfLength(10), null, fieldName, separator, fieldName);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(fieldName, String.class), equalTo(expectedResult));
    }

    public void testJoinNonListField() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        ingestDocument.setFieldValue(fieldName, randomAlphaOfLengthBetween(1, 10));
        Processor processor = new JoinProcessor(randomAlphaOfLength(10), null, fieldName, "-", fieldName);
        try {
            processor.execute(ingestDocument);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [" + fieldName + "] of type [java.lang.String] cannot be cast to [java.util.List]"));
        }
    }

    public void testJoinNonExistingField() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        Processor processor = new JoinProcessor(randomAlphaOfLength(10), null, fieldName, "-", fieldName);
        try {
            processor.execute(ingestDocument);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("not present as part of path [" + fieldName + "]"));
        }
    }

    public void testJoinNullValue() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.singletonMap("field", null));
        Processor processor = new JoinProcessor(randomAlphaOfLength(10), null, "field", "-", "field");
        try {
            processor.execute(ingestDocument);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("field [field] is null, cannot join."));
        }
    }

    public void testJoinWithTargetField() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        int numItems = randomIntBetween(1, 10);
        String separator = randomFrom(SEPARATORS);
        List<String> fieldValue = new ArrayList<>(numItems);
        String expectedResult = "";
        for (int j = 0; j < numItems; j++) {
            String value = randomAlphaOfLengthBetween(1, 10);
            fieldValue.add(value);
            expectedResult += value;
            if (j < numItems - 1) {
                expectedResult += separator;
            }
        }
        String fieldName = RandomDocumentPicks.addRandomField(random(), ingestDocument, fieldValue);
        String targetFieldName = fieldName + randomAlphaOfLength(5);
        Processor processor = new JoinProcessor(randomAlphaOfLength(10), null, fieldName, separator, targetFieldName);
        processor.execute(ingestDocument);
        assertThat(ingestDocument.getFieldValue(targetFieldName, String.class), equalTo(expectedResult));
    }
}
