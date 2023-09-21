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

import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchParseException;
import org.opensearch.ingest.TestTemplateService;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;

public class SetProcessorFactoryTests extends OpenSearchTestCase {

    private SetProcessor.Factory factory;

    @Before
    public void init() {
        factory = new SetProcessor.Factory(TestTemplateService.instance());
    }

    public void testCreate() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("value", "value1");
        String processorTag = randomAlphaOfLength(10);
        SetProcessor setProcessor = factory.create(null, processorTag, null, config);
        assertThat(setProcessor.getTag(), equalTo(processorTag));
        assertThat(setProcessor.getField().newInstance(Collections.emptyMap()).execute(), equalTo("field1"));
        assertThat(setProcessor.getValue().copyAndResolve(Collections.emptyMap()), equalTo("value1"));
        assertThat(setProcessor.isOverrideEnabled(), equalTo(true));
    }

    public void testCreateWithOverride() throws Exception {
        boolean overrideEnabled = randomBoolean();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("value", "value1");
        config.put("override", overrideEnabled);
        String processorTag = randomAlphaOfLength(10);
        SetProcessor setProcessor = factory.create(null, processorTag, null, config);
        assertThat(setProcessor.getTag(), equalTo(processorTag));
        assertThat(setProcessor.getField().newInstance(Collections.emptyMap()).execute(), equalTo("field1"));
        assertThat(setProcessor.getValue().copyAndResolve(Collections.emptyMap()), equalTo("value1"));
        assertThat(setProcessor.isOverrideEnabled(), equalTo(overrideEnabled));
    }

    public void testCreateNoFieldPresent() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("value", "value1");
        try {
            factory.create(null, null, null, config);
            fail("factory create should have failed");
        } catch (OpenSearchParseException e) {
            assertThat(e.getMessage(), equalTo("[field] required property is missing"));
        }
    }

    public void testCreateNoValuePresent() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        try {
            factory.create(null, null, null, config);
            fail("factory create should have failed");
        } catch (OpenSearchParseException e) {
            assertThat(e.getMessage(), equalTo("[value] required property is missing"));
        }
    }

    public void testCreateNullValue() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("value", null);
        try {
            factory.create(null, null, null, config);
            fail("factory create should have failed");
        } catch (OpenSearchParseException e) {
            assertThat(e.getMessage(), equalTo("[value] required property is missing"));
        }
    }

    public void testInvalidMustacheTemplate() throws Exception {
        SetProcessor.Factory factory = new SetProcessor.Factory(TestTemplateService.instance(true));
        Map<String, Object> config = new HashMap<>();
        config.put("field", "{{field1}}");
        config.put("value", "value1");
        String processorTag = randomAlphaOfLength(10);
        OpenSearchException exception = expectThrows(OpenSearchException.class, () -> factory.create(null, processorTag, null, config));
        assertThat(exception.getMessage(), equalTo("java.lang.RuntimeException: could not compile script"));
        assertThat(exception.getMetadata("opensearch.processor_tag").get(0), equalTo(processorTag));
    }

    public void testCopyFrom() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("value", "value1");
        config.put("copy_from", "field2");
        String processorTag = randomAlphaOfLength(10);
        assertThrows(
            "either copy_from or value can be set",
            OpenSearchParseException.class,
            () -> factory.create(null, processorTag, null, config)
        );
    }
}
