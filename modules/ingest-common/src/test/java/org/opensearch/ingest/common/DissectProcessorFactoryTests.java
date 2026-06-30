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
import org.opensearch.dissect.DissectException;
import org.opensearch.ingest.RandomDocumentPicks;
import org.opensearch.test.OpenSearchTestCase;
import org.hamcrest.Matchers;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.is;

public class DissectProcessorFactoryTests extends OpenSearchTestCase {

    public void testCreate() {
        DissectProcessor.Factory factory = new DissectProcessor.Factory();
        String fieldName = RandomDocumentPicks.randomFieldName(random());
        String processorTag = randomAlphaOfLength(10);
        String pattern = "%{a},%{b},%{c}";
        String appendSeparator = ":";

        Map<String, Object> config = new HashMap<>();
        config.put("field", fieldName);
        config.put("pattern", pattern);
        config.put("append_separator", appendSeparator);
        config.put("ignore_missing", true);

        DissectProcessor processor = factory.create(null, processorTag, null, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.field, equalTo(fieldName));
        assertThat(processor.pattern, equalTo(pattern));
        assertThat(processor.appendSeparator, equalTo(appendSeparator));
        assertThat(processor.dissectParser, is(notNullValue()));
        assertThat(processor.ignoreMissing, is(true));
    }

    public void testCreateMissingField() {
        DissectProcessor.Factory factory = new DissectProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("pattern", "%{a},%{b},%{c}");
        Exception e = expectThrows(OpenSearchParseException.class, () -> factory.create(null, "_tag", null, config));
        assertThat(e.getMessage(), Matchers.equalTo("[field] required property is missing"));
    }

    public void testCreateMissingPattern() {
        DissectProcessor.Factory factory = new DissectProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", randomAlphaOfLength(10));
        Exception e = expectThrows(OpenSearchParseException.class, () -> factory.create(null, "_tag", null, config));
        assertThat(e.getMessage(), Matchers.equalTo("[pattern] required property is missing"));
    }

    public void testCreateMissingOptionals() {
        DissectProcessor.Factory factory = new DissectProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("pattern", "%{a},%{b},%{c}");
        config.put("field", randomAlphaOfLength(10));
        DissectProcessor processor = factory.create(null, "_tag", null, config);
        assertThat(processor.appendSeparator, equalTo(""));
        assertThat(processor.ignoreMissing, is(false));
    }

    public void testCreateBadPattern() {
        DissectProcessor.Factory factory = new DissectProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("pattern", "no keys defined");
        config.put("field", randomAlphaOfLength(10));
        expectThrows(DissectException.class, () -> factory.create(null, "_tag", null, config));
    }
}
