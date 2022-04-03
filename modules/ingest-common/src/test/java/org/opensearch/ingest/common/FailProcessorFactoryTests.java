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

public class FailProcessorFactoryTests extends OpenSearchTestCase {

    private FailProcessor.Factory factory;

    @Before
    public void init() {
        factory = new FailProcessor.Factory(TestTemplateService.instance());
    }

    public void testCreate() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("message", "error");
        String processorTag = randomAlphaOfLength(10);
        FailProcessor failProcessor = factory.create(null, processorTag, null, config);
        assertThat(failProcessor.getTag(), equalTo(processorTag));
        assertThat(failProcessor.getMessage().newInstance(Collections.emptyMap()).execute(), equalTo("error"));
    }

    public void testCreateMissingMessageField() throws Exception {
        Map<String, Object> config = new HashMap<>();
        try {
            factory.create(null, null, null, config);
            fail("factory create should have failed");
        } catch (OpenSearchParseException e) {
            assertThat(e.getMessage(), equalTo("[message] required property is missing"));
        }
    }

    public void testInvalidMustacheTemplate() throws Exception {
        FailProcessor.Factory factory = new FailProcessor.Factory(TestTemplateService.instance(true));
        Map<String, Object> config = new HashMap<>();
        config.put("message", "{{error}}");
        String processorTag = randomAlphaOfLength(10);
        OpenSearchException exception = expectThrows(OpenSearchException.class, () -> factory.create(null, processorTag, null, config));
        assertThat(exception.getMessage(), equalTo("java.lang.RuntimeException: could not compile script"));
        assertThat(exception.getMetadata("opensearch.processor_tag").get(0), equalTo(processorTag));
    }
}
