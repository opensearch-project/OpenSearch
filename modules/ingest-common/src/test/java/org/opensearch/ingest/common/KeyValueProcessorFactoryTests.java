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
import org.opensearch.common.util.set.Sets;
import org.opensearch.ingest.TestTemplateService;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class KeyValueProcessorFactoryTests extends OpenSearchTestCase {

    private KeyValueProcessor.Factory factory;

    @Before
    public void init() {
        factory = new KeyValueProcessor.Factory(TestTemplateService.instance());
    }

    public void testCreateWithDefaults() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("field_split", "&");
        config.put("value_split", "=");
        String processorTag = randomAlphaOfLength(10);
        KeyValueProcessor processor = factory.create(null, processorTag, null, config);
        assertEquals(processor.getTag(), processorTag);
        assertEquals(processor.getField().newInstance(Collections.emptyMap()).execute(), "field1");
        assertEquals(processor.getFieldSplit(), "&");
        assertEquals(processor.getValueSplit(), "=");
        assertEquals(processor.getIncludeKeys(), null);
        assertEquals(processor.getTargetField(), null);
        assertFalse(processor.isIgnoreMissing());
    }

    public void testCreateWithAllFieldsSet() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("field_split", "&");
        config.put("value_split", "=");
        config.put("target_field", "target");
        config.put("include_keys", Arrays.asList("a", "b"));
        config.put("exclude_keys", Collections.emptyList());
        config.put("ignore_missing", true);
        String processorTag = randomAlphaOfLength(10);
        KeyValueProcessor processor = factory.create(null, processorTag, null, config);
        assertEquals(processor.getTag(), processorTag);
        assertEquals(processor.getField().newInstance(Collections.emptyMap()).execute(), "field1");
        assertEquals(processor.getFieldSplit(), "&");
        assertEquals(processor.getValueSplit(), "=");
        assertEquals(processor.getIncludeKeys(), Sets.newHashSet("a", "b"));
        assertEquals(processor.getExcludeKeys(), Collections.emptySet());
        assertEquals(processor.getTargetField().newInstance(Collections.emptyMap()).execute(), "target");
        assertTrue(processor.isIgnoreMissing());
    }

    public void testCreateWithMissingField() {
        Map<String, Object> config = new HashMap<>();
        String processorTag = randomAlphaOfLength(10);
        OpenSearchException exception = expectThrows(
            OpenSearchParseException.class,
            () -> factory.create(null, processorTag, null, config)
        );
        assertEquals(exception.getMessage(), "[field] required property is missing");
    }

    public void testCreateWithMissingFieldSplit() {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        String processorTag = randomAlphaOfLength(10);
        OpenSearchException exception = expectThrows(
            OpenSearchParseException.class,
            () -> factory.create(null, processorTag, null, config)
        );
        assertEquals(exception.getMessage(), "[field_split] required property is missing");
    }

    public void testCreateWithMissingValueSplit() {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("field_split", "&");
        String processorTag = randomAlphaOfLength(10);
        OpenSearchException exception = expectThrows(
            OpenSearchParseException.class,
            () -> factory.create(null, processorTag, null, config)
        );
        assertEquals(exception.getMessage(), "[value_split] required property is missing");
    }
}
