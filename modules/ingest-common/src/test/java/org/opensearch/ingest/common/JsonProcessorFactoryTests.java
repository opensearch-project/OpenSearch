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
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;

public class JsonProcessorFactoryTests extends OpenSearchTestCase {

    private static final JsonProcessor.Factory FACTORY = new JsonProcessor.Factory();

    public void testCreate() throws Exception {
        String processorTag = randomAlphaOfLength(10);
        String randomField = randomAlphaOfLength(10);
        String randomTargetField = randomAlphaOfLength(5);
        Map<String, Object> config = new HashMap<>();
        config.put("field", randomField);
        config.put("target_field", randomTargetField);
        JsonProcessor jsonProcessor = FACTORY.create(null, processorTag, null, config);
        assertThat(jsonProcessor.getTag(), equalTo(processorTag));
        assertThat(jsonProcessor.getField(), equalTo(randomField));
        assertThat(jsonProcessor.getTargetField(), equalTo(randomTargetField));
    }

    public void testCreateWithAddToRoot() throws Exception {
        String processorTag = randomAlphaOfLength(10);
        String randomField = randomAlphaOfLength(10);
        Map<String, Object> config = new HashMap<>();
        config.put("field", randomField);
        config.put("add_to_root", true);
        JsonProcessor jsonProcessor = FACTORY.create(null, processorTag, null, config);
        assertThat(jsonProcessor.getTag(), equalTo(processorTag));
        assertThat(jsonProcessor.getField(), equalTo(randomField));
        assertThat(jsonProcessor.getTargetField(), equalTo(randomField));
        assertTrue(jsonProcessor.isAddToRoot());
    }

    public void testCreateWithDefaultTarget() throws Exception {
        String processorTag = randomAlphaOfLength(10);
        String randomField = randomAlphaOfLength(10);
        Map<String, Object> config = new HashMap<>();
        config.put("field", randomField);
        JsonProcessor jsonProcessor = FACTORY.create(null, processorTag, null, config);
        assertThat(jsonProcessor.getTag(), equalTo(processorTag));
        assertThat(jsonProcessor.getField(), equalTo(randomField));
        assertThat(jsonProcessor.getTargetField(), equalTo(randomField));
    }

    public void testCreateWithMissingField() throws Exception {
        Map<String, Object> config = new HashMap<>();
        String processorTag = randomAlphaOfLength(10);
        OpenSearchException exception = expectThrows(
            OpenSearchParseException.class,
            () -> FACTORY.create(null, processorTag, null, config)
        );
        assertThat(exception.getMessage(), equalTo("[field] required property is missing"));
    }

    public void testCreateWithBothTargetFieldAndAddToRoot() throws Exception {
        String randomField = randomAlphaOfLength(10);
        String randomTargetField = randomAlphaOfLength(5);
        Map<String, Object> config = new HashMap<>();
        config.put("field", randomField);
        config.put("target_field", randomTargetField);
        config.put("add_to_root", true);
        OpenSearchException exception = expectThrows(
            OpenSearchParseException.class,
            () -> FACTORY.create(null, randomAlphaOfLength(10), null, config)
        );
        assertThat(exception.getMessage(), equalTo("[target_field] Cannot set a target field while also setting `add_to_root` to true"));
    }
}
