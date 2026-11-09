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

import org.opensearch.grok.MatcherWatchdog;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.RandomDocumentPicks;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class GrokProcessorTests extends OpenSearchTestCase {

    public void testMatch() throws Exception {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        IngestDocument doc = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        doc.setFieldValue(fieldName, "1");
        GrokProcessor processor = new GrokProcessor(
            randomAlphaOfLength(10),
            null,
            Collections.singletonMap("ONE", "1"),
            Collections.singletonList("%{ONE:one}"),
            fieldName,
            false,
            false,
            false,
            MatcherWatchdog.noop()
        );
        processor.execute(doc);
        assertThat(doc.getFieldValue("one", String.class), equalTo("1"));
    }

    public void testIgnoreCase() throws Exception {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        IngestDocument doc = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        doc.setFieldValue(fieldName, "A");
        GrokProcessor processor = new GrokProcessor(
            randomAlphaOfLength(10),
            null,
            Collections.emptyMap(),
            Collections.singletonList("(?<a>(?i)A)"),
            fieldName,
            false,
            false,
            false,
            MatcherWatchdog.noop()
        );
        processor.execute(doc);
        assertThat(doc.getFieldValue("a", String.class), equalTo("A"));
    }

    public void testNoMatch() {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        IngestDocument doc = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        doc.setFieldValue(fieldName, "23");
        GrokProcessor processor = new GrokProcessor(
            randomAlphaOfLength(10),
            null,
            Collections.singletonMap("ONE", "1"),
            Collections.singletonList("%{ONE:one}"),
            fieldName,
            false,
            false,
            false,
            MatcherWatchdog.noop()
        );
        Exception e = expectThrows(Exception.class, () -> processor.execute(doc));
        assertThat(e.getMessage(), equalTo("Provided Grok expressions do not match field value: [23]"));
    }

    public void testNoMatchingPatternName() {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        IngestDocument doc = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        doc.setFieldValue(fieldName, "23");
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> new GrokProcessor(
                randomAlphaOfLength(10),
                null,
                Collections.singletonMap("ONE", "1"),
                Collections.singletonList("%{NOTONE:not_one}"),
                fieldName,
                false,
                false,
                false,
                MatcherWatchdog.noop()
            )
        );
        assertThat(e.getMessage(), equalTo("Unable to find pattern [NOTONE] in Grok's pattern dictionary"));
    }

    public void testMatchWithoutCaptures() throws Exception {
        String fieldName = "value";
        IngestDocument originalDoc = new IngestDocument(new HashMap<>(), new HashMap<>());
        originalDoc.setFieldValue(fieldName, fieldName);
        IngestDocument doc = new IngestDocument(originalDoc);
        GrokProcessor processor = new GrokProcessor(
            randomAlphaOfLength(10),
            null,
            Collections.emptyMap(),
            Collections.singletonList(fieldName),
            fieldName,
            false,
            false,
            false,
            MatcherWatchdog.noop()
        );
        processor.execute(doc);
        assertThat(doc, equalTo(originalDoc));
    }

    public void testNullField() {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        IngestDocument doc = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        doc.setFieldValue(fieldName, null);
        GrokProcessor processor = new GrokProcessor(
            randomAlphaOfLength(10),
            null,
            Collections.singletonMap("ONE", "1"),
            Collections.singletonList("%{ONE:one}"),
            fieldName,
            false,
            false,
            false,
            MatcherWatchdog.noop()
        );
        Exception e = expectThrows(Exception.class, () -> processor.execute(doc));
        assertThat(e.getMessage(), equalTo("field [" + fieldName + "] is null, cannot process it."));
    }

    public void testNullFieldWithIgnoreMissing() throws Exception {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        originalIngestDocument.setFieldValue(fieldName, null);
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        GrokProcessor processor = new GrokProcessor(
            randomAlphaOfLength(10),
            null,
            Collections.singletonMap("ONE", "1"),
            Collections.singletonList("%{ONE:one}"),
            fieldName,
            false,
            true,
            false,
            MatcherWatchdog.noop()
        );
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testNotStringField() {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        IngestDocument doc = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        doc.setFieldValue(fieldName, 1);
        GrokProcessor processor = new GrokProcessor(
            randomAlphaOfLength(10),
            null,
            Collections.singletonMap("ONE", "1"),
            Collections.singletonList("%{ONE:one}"),
            fieldName,
            false,
            false,
            false,
            MatcherWatchdog.noop()
        );
        Exception e = expectThrows(Exception.class, () -> processor.execute(doc));
        assertThat(e.getMessage(), equalTo("field [" + fieldName + "] of type [java.lang.Integer] cannot be cast to [java.lang.String]"));
    }

    public void testNotStringFieldWithIgnoreMissing() {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        IngestDocument doc = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        doc.setFieldValue(fieldName, 1);
        GrokProcessor processor = new GrokProcessor(
            randomAlphaOfLength(10),
            null,
            Collections.singletonMap("ONE", "1"),
            Collections.singletonList("%{ONE:one}"),
            fieldName,
            false,
            true,
            false,
            MatcherWatchdog.noop()
        );
        Exception e = expectThrows(Exception.class, () -> processor.execute(doc));
        assertThat(e.getMessage(), equalTo("field [" + fieldName + "] of type [java.lang.Integer] cannot be cast to [java.lang.String]"));
    }

    public void testMissingField() {
        String fieldName = "foo.bar";
        IngestDocument doc = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        GrokProcessor processor = new GrokProcessor(
            randomAlphaOfLength(10),
            null,
            Collections.singletonMap("ONE", "1"),
            Collections.singletonList("%{ONE:one}"),
            fieldName,
            false,
            false,
            false,
            MatcherWatchdog.noop()
        );
        Exception e = expectThrows(Exception.class, () -> processor.execute(doc));
        assertThat(e.getMessage(), equalTo("field [foo] not present as part of path [foo.bar]"));
    }

    public void testMissingFieldWithIgnoreMissing() throws Exception {
        String fieldName = "foo.bar";
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        GrokProcessor processor = new GrokProcessor(
            randomAlphaOfLength(10),
            null,
            Collections.singletonMap("ONE", "1"),
            Collections.singletonList("%{ONE:one}"),
            fieldName,
            false,
            true,
            false,
            MatcherWatchdog.noop()
        );
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testMultiplePatternsWithMatchReturn() throws Exception {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        IngestDocument doc = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        doc.setFieldValue(fieldName, "2");
        Map<String, String> patternBank = new HashMap<>();
        patternBank.put("ONE", "1");
        patternBank.put("TWO", "2");
        patternBank.put("THREE", "3");
        GrokProcessor processor = new GrokProcessor(
            randomAlphaOfLength(10),
            null,
            patternBank,
            Arrays.asList("%{ONE:one}", "%{TWO:two}", "%{THREE:three}"),
            fieldName,
            false,
            false,
            false,
            MatcherWatchdog.noop()
        );
        processor.execute(doc);
        assertThat(doc.hasField("one"), equalTo(false));
        assertThat(doc.getFieldValue("two", String.class), equalTo("2"));
        assertThat(doc.hasField("three"), equalTo(false));
    }

    public void testSetMetadata() throws Exception {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        IngestDocument doc = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        doc.setFieldValue(fieldName, "abc23");
        Map<String, String> patternBank = new HashMap<>();
        patternBank.put("ONE", "1");
        patternBank.put("TWO", "2");
        patternBank.put("THREE", "3");
        GrokProcessor processor = new GrokProcessor(
            randomAlphaOfLength(10),
            null,
            patternBank,
            Arrays.asList("%{ONE:one}", "%{TWO:two}", "%{THREE:three}"),
            fieldName,
            true,
            false,
            false,
            MatcherWatchdog.noop()
        );
        processor.execute(doc);
        assertThat(doc.hasField("one"), equalTo(false));
        assertThat(doc.getFieldValue("two", String.class), equalTo("2"));
        assertThat(doc.hasField("three"), equalTo(false));
        assertThat(doc.getFieldValue("_ingest._grok_match_index", String.class), equalTo("1"));
    }

    public void testTraceWithOnePattern() throws Exception {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        IngestDocument doc = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        doc.setFieldValue(fieldName, "first1");
        Map<String, String> patternBank = new HashMap<>();
        patternBank.put("ONE", "1");
        GrokProcessor processor = new GrokProcessor(
            randomAlphaOfLength(10),
            null,
            patternBank,
            Arrays.asList("%{ONE:one}"),
            fieldName,
            true,
            false,
            false,
            MatcherWatchdog.noop()
        );
        processor.execute(doc);
        assertThat(doc.hasField("one"), equalTo(true));
        assertThat(doc.getFieldValue("_ingest._grok_match_index", String.class), equalTo("0"));
    }

    public void testCombinedPatterns() {
        String combined;
        combined = GrokProcessor.combinePatterns(Arrays.asList(""), false);
        assertThat(combined, equalTo(""));
        combined = GrokProcessor.combinePatterns(Arrays.asList(""), true);
        assertThat(combined, equalTo(""));
        combined = GrokProcessor.combinePatterns(Arrays.asList("foo"), false);
        assertThat(combined, equalTo("foo"));
        combined = GrokProcessor.combinePatterns(Arrays.asList("foo"), true);
        assertThat(combined, equalTo("foo"));
        combined = GrokProcessor.combinePatterns(Arrays.asList("foo", "bar"), false);
        assertThat(combined, equalTo("(?:foo)|(?:bar)"));
        combined = GrokProcessor.combinePatterns(Arrays.asList("foo", "bar"), true);
        assertThat(combined, equalTo("(?<_ingest._grok_match_index.0>foo)|(?<_ingest._grok_match_index.1>bar)"));
    }

    public void testCombineSamePatternNameAcrossPatterns() throws Exception {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        IngestDocument doc = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        doc.setFieldValue(fieldName, "1-3");
        Map<String, String> patternBank = new HashMap<>();
        patternBank.put("ONE", "1");
        patternBank.put("TWO", "2");
        patternBank.put("THREE", "3");
        GrokProcessor processor = new GrokProcessor(
            randomAlphaOfLength(10),
            null,
            patternBank,
            Arrays.asList("%{ONE:first}-%{TWO:second}", "%{ONE:first}-%{THREE:second}"),
            fieldName,
            randomBoolean(),
            randomBoolean(),
            false,
            MatcherWatchdog.noop()
        );
        processor.execute(doc);
        assertThat(doc.getFieldValue("first", String.class), equalTo("1"));
        assertThat(doc.getFieldValue("second", String.class), equalTo("3"));
    }

    public void testFirstWinNamedCapture() throws Exception {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        IngestDocument doc = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        doc.setFieldValue(fieldName, "12");
        Map<String, String> patternBank = new HashMap<>();
        patternBank.put("ONETWO", "1|2");
        GrokProcessor processor = new GrokProcessor(
            randomAlphaOfLength(10),
            null,
            patternBank,
            Collections.singletonList("%{ONETWO:first}%{ONETWO:first}"),
            fieldName,
            randomBoolean(),
            randomBoolean(),
            false,
            MatcherWatchdog.noop()
        );
        processor.execute(doc);
        assertThat(doc.getFieldValue("first", String.class), equalTo("1"));
    }

    public void testUnmatchedNamesNotIncludedInDocument() throws Exception {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        IngestDocument doc = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        doc.setFieldValue(fieldName, "3");
        Map<String, String> patternBank = new HashMap<>();
        patternBank.put("ONETWO", "1|2");
        patternBank.put("THREE", "3");
        GrokProcessor processor = new GrokProcessor(
            randomAlphaOfLength(10),
            null,
            patternBank,
            Collections.singletonList("%{ONETWO:first}|%{THREE:second}"),
            fieldName,
            randomBoolean(),
            randomBoolean(),
            false,
            MatcherWatchdog.noop()
        );
        processor.execute(doc);
        assertFalse(doc.hasField("first"));
        assertThat(doc.getFieldValue("second", String.class), equalTo("3"));
    }

    public void testCaptureAllMatchesWithSameFieldName() throws Exception {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        IngestDocument doc = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        doc.setFieldValue(fieldName, "1 2 3");
        Map<String, String> patternBank = new HashMap<>();
        patternBank.put("NUMBER", "\\d");

        // Create processor with captureAllMatches=true
        GrokProcessor processor = new GrokProcessor(
            randomAlphaOfLength(10),
            null,
            patternBank,
            Collections.singletonList("%{NUMBER:num} %{NUMBER:num} %{NUMBER:num}"),
            fieldName,
            false,
            false,
            true,
            MatcherWatchdog.noop()
        );

        processor.execute(doc);

        // Verify that 'num' field contains a list of all matches
        Object numField = doc.getFieldValue("num", Object.class);
        assertThat(numField, instanceOf(List.class));

        @SuppressWarnings("unchecked")
        List<String> numList = (List<String>) numField;
        assertEquals(3, numList.size());
        assertEquals("1", numList.get(0));
        assertEquals("2", numList.get(1));
        assertEquals("3", numList.get(2));

        fieldName = RandomDocumentPicks.randomFieldName(random());
        doc = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        doc.setFieldValue(fieldName, "192.168.1.1 172.16.0.1");
        patternBank = new HashMap<>();
        patternBank.put(
            "IP",
            "(?<![0-9])(?:(?:[0-1]?[0-9]{1,2}|2[0-4][0-9]|25[0-5])[.](?:[0-1]?[0-9]{1,2}|2[0-4][0-9]|25[0-5])[.](?:[0-1]?[0-9]{1,2}|2[0-4][0-9]|25[0-5])[.](?:[0-1]?[0-9]{1,2}|2[0-4][0-9]|25[0-5]))(?![0-9])"
        );

        // Create multiple patterns to match each IP address separately
        processor = new GrokProcessor(
            randomAlphaOfLength(10),
            null,
            patternBank,
            Collections.singletonList("%{IP:ipAddress} (%{IP:ipAddress})?"),
            fieldName,
            false,
            false,
            true,
            MatcherWatchdog.noop()
        );

        processor.execute(doc);

        // Verify that 'ipAddress' field contains a list of all IP addresses
        Object ipField = doc.getFieldValue("ipAddress", Object.class);
        assertThat(ipField, instanceOf(List.class));

        @SuppressWarnings("unchecked")
        List<String> ipList = (List<String>) ipField;
        assertEquals(2, ipList.size());
        assertEquals("192.168.1.1", ipList.get(0));
        assertEquals("172.16.0.1", ipList.get(1));
    }

    public void testCaptureAllMatchesDisabled() throws Exception {
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        IngestDocument doc = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        doc.setFieldValue(fieldName, "1 2 3");
        Map<String, String> patternBank = new HashMap<>();
        patternBank.put("NUMBER", "\\d");

        // Create processor with captureAllMatches=false (default behavior)
        GrokProcessor processor = new GrokProcessor(
            randomAlphaOfLength(10),
            null,
            patternBank,
            Collections.singletonList("%{NUMBER:num} %{NUMBER:num} %{NUMBER:num}"),
            fieldName,
            false,
            false,
            false,
            MatcherWatchdog.noop()
        );

        processor.execute(doc);

        // Verify that only the first match is captured
        String numValue = doc.getFieldValue("num", String.class);
        assertEquals("1", numValue);
    }
}
