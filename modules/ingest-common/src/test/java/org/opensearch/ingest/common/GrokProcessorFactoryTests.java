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

import org.opensearch.OpenSearchParseException;
import org.opensearch.grok.MatcherWatchdog;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class GrokProcessorFactoryTests extends OpenSearchTestCase {

    public void testBuild() throws Exception {
        GrokProcessor.Factory factory = new GrokProcessor.Factory(Collections.emptyMap(), MatcherWatchdog.noop());

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("patterns", Collections.singletonList("(?<foo>\\w+)"));
        String processorTag = randomAlphaOfLength(10);
        GrokProcessor processor = factory.create(null, processorTag, null, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getMatchField(), equalTo("_field"));
        assertThat(processor.getGrok(), notNullValue());
        assertThat(processor.isIgnoreMissing(), is(false));
    }

    public void testBuildWithIgnoreMissing() throws Exception {
        GrokProcessor.Factory factory = new GrokProcessor.Factory(Collections.emptyMap(), MatcherWatchdog.noop());

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("patterns", Collections.singletonList("(?<foo>\\w+)"));
        config.put("ignore_missing", true);
        String processorTag = randomAlphaOfLength(10);
        GrokProcessor processor = factory.create(null, processorTag, null, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getMatchField(), equalTo("_field"));
        assertThat(processor.getGrok(), notNullValue());
        assertThat(processor.isIgnoreMissing(), is(true));
    }

    public void testBuildMissingField() throws Exception {
        GrokProcessor.Factory factory = new GrokProcessor.Factory(Collections.emptyMap(), MatcherWatchdog.noop());
        Map<String, Object> config = new HashMap<>();
        config.put("patterns", Collections.singletonList("(?<foo>\\w+)"));
        OpenSearchParseException e = expectThrows(OpenSearchParseException.class, () -> factory.create(null, null, null, config));
        assertThat(e.getMessage(), equalTo("[field] required property is missing"));
    }

    public void testBuildMissingPatterns() throws Exception {
        GrokProcessor.Factory factory = new GrokProcessor.Factory(Collections.emptyMap(), MatcherWatchdog.noop());
        Map<String, Object> config = new HashMap<>();
        config.put("field", "foo");
        OpenSearchParseException e = expectThrows(OpenSearchParseException.class, () -> factory.create(null, null, null, config));
        assertThat(e.getMessage(), equalTo("[patterns] required property is missing"));
    }

    public void testBuildEmptyPatternsList() throws Exception {
        GrokProcessor.Factory factory = new GrokProcessor.Factory(Collections.emptyMap(), MatcherWatchdog.noop());
        Map<String, Object> config = new HashMap<>();
        config.put("field", "foo");
        config.put("patterns", Collections.emptyList());
        OpenSearchParseException e = expectThrows(OpenSearchParseException.class, () -> factory.create(null, null, null, config));
        assertThat(e.getMessage(), equalTo("[patterns] List of patterns must not be empty"));
    }

    public void testCreateWithCustomPatterns() throws Exception {
        GrokProcessor.Factory factory = new GrokProcessor.Factory(Collections.emptyMap(), MatcherWatchdog.noop());

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("patterns", Collections.singletonList("%{MY_PATTERN:name}!"));
        config.put("pattern_definitions", Collections.singletonMap("MY_PATTERN", "foo"));
        GrokProcessor processor = factory.create(null, null, null, config);
        assertThat(processor.getMatchField(), equalTo("_field"));
        assertThat(processor.getGrok(), notNullValue());
        assertThat(processor.getGrok().match("foo!"), equalTo(true));
    }

    public void testCreateWithInvalidPattern() throws Exception {
        GrokProcessor.Factory factory = new GrokProcessor.Factory(Collections.emptyMap(), MatcherWatchdog.noop());
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("patterns", Collections.singletonList("["));
        OpenSearchParseException e = expectThrows(OpenSearchParseException.class, () -> factory.create(null, null, null, config));
        assertThat(e.getMessage(), equalTo("[patterns] Invalid regex pattern found in: [[]. premature end of char-class"));
    }

    public void testCreateWithInvalidPatternDefinition() throws Exception {
        GrokProcessor.Factory factory = new GrokProcessor.Factory(Collections.emptyMap(), MatcherWatchdog.noop());
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("patterns", Collections.singletonList("%{MY_PATTERN:name}!"));
        config.put("pattern_definitions", Collections.singletonMap("MY_PATTERN", "["));
        OpenSearchParseException e = expectThrows(OpenSearchParseException.class, () -> factory.create(null, null, null, config));
        assertThat(
            e.getMessage(),
            equalTo("[patterns] Invalid regex pattern found in: [%{MY_PATTERN:name}!]. premature end of char-class")
        );
    }

    public void testBuildWithCaptureAllMatches() throws Exception {
        GrokProcessor.Factory factory = new GrokProcessor.Factory(Collections.emptyMap(), MatcherWatchdog.noop());

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("patterns", Collections.singletonList("(?<foo>\\w+)"));
        config.put("capture_all_matches", true);
        String processorTag = randomAlphaOfLength(10);
        GrokProcessor processor = factory.create(null, processorTag, null, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getMatchField(), equalTo("_field"));
        assertThat(processor.getGrok(), notNullValue());
        assertThat(processor.isCaptureAllMatches(), is(true));
    }
}
