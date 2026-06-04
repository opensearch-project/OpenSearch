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
import org.opensearch.test.OpenSearchTestCase;
import org.hamcrest.Matchers;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ConvertProcessorFactoryTests extends OpenSearchTestCase {

    public void testCreate() throws Exception {
        ConvertProcessor.Factory factory = new ConvertProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        ConvertProcessor.Type type = randomFrom(ConvertProcessor.Type.values());
        config.put("field", "field1");
        config.put("type", type.toString());
        String processorTag = randomAlphaOfLength(10);
        ConvertProcessor convertProcessor = factory.create(null, processorTag, null, config);
        assertThat(convertProcessor.getTag(), equalTo(processorTag));
        assertThat(convertProcessor.getField(), equalTo("field1"));
        assertThat(convertProcessor.getTargetField(), equalTo("field1"));
        assertThat(convertProcessor.getConvertType(), equalTo(type));
        assertThat(convertProcessor.isIgnoreMissing(), is(false));
    }

    public void testCreateUnsupportedType() throws Exception {
        ConvertProcessor.Factory factory = new ConvertProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        String type = "type-" + randomAlphaOfLengthBetween(1, 10);
        config.put("field", "field1");
        config.put("type", type);
        try {
            factory.create(null, null, null, config);
            fail("factory create should have failed");
        } catch (OpenSearchParseException e) {
            assertThat(e.getMessage(), Matchers.equalTo("[type] type [" + type + "] not supported, cannot convert field."));
            assertThat(e.getMetadata("opensearch.processor_type").get(0), equalTo(ConvertProcessor.TYPE));
            assertThat(e.getMetadata("opensearch.property_name").get(0), equalTo("type"));
            assertThat(e.getMetadata("opensearch.processor_tag"), nullValue());
        }
    }

    public void testCreateNoFieldPresent() throws Exception {
        ConvertProcessor.Factory factory = new ConvertProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        String type = "type-" + randomAlphaOfLengthBetween(1, 10);
        config.put("type", type);
        try {
            factory.create(null, null, null, config);
            fail("factory create should have failed");
        } catch (OpenSearchParseException e) {
            assertThat(e.getMessage(), Matchers.equalTo("[field] required property is missing"));
        }
    }

    public void testCreateNoTypePresent() throws Exception {
        ConvertProcessor.Factory factory = new ConvertProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        try {
            factory.create(null, null, null, config);
            fail("factory create should have failed");
        } catch (OpenSearchParseException e) {
            assertThat(e.getMessage(), Matchers.equalTo("[type] required property is missing"));
        }
    }

    public void testCreateWithExplicitTargetField() throws Exception {
        ConvertProcessor.Factory factory = new ConvertProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        ConvertProcessor.Type type = randomFrom(ConvertProcessor.Type.values());
        config.put("field", "field1");
        config.put("target_field", "field2");
        config.put("type", type.toString());
        String processorTag = randomAlphaOfLength(10);
        ConvertProcessor convertProcessor = factory.create(null, processorTag, null, config);
        assertThat(convertProcessor.getTag(), equalTo(processorTag));
        assertThat(convertProcessor.getField(), equalTo("field1"));
        assertThat(convertProcessor.getTargetField(), equalTo("field2"));
        assertThat(convertProcessor.getConvertType(), equalTo(type));
        assertThat(convertProcessor.isIgnoreMissing(), is(false));
    }

    public void testCreateWithIgnoreMissing() throws Exception {
        ConvertProcessor.Factory factory = new ConvertProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        ConvertProcessor.Type type = randomFrom(ConvertProcessor.Type.values());
        config.put("field", "field1");
        config.put("type", type.toString());
        config.put("ignore_missing", true);
        String processorTag = randomAlphaOfLength(10);
        ConvertProcessor convertProcessor = factory.create(null, processorTag, null, config);
        assertThat(convertProcessor.getTag(), equalTo(processorTag));
        assertThat(convertProcessor.getField(), equalTo("field1"));
        assertThat(convertProcessor.getTargetField(), equalTo("field1"));
        assertThat(convertProcessor.getConvertType(), equalTo(type));
        assertThat(convertProcessor.isIgnoreMissing(), is(true));
    }
}
