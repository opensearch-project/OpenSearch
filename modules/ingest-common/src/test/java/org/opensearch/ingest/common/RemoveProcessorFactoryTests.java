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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;

public class RemoveProcessorFactoryTests extends OpenSearchTestCase {

    private RemoveProcessor.Factory factory;

    @Before
    public void init() {
        factory = new RemoveProcessor.Factory(TestTemplateService.instance());
    }

    public void testCreate() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        String processorTag = randomAlphaOfLength(10);
        RemoveProcessor removeProcessor = factory.create(null, processorTag, null, config);
        assertThat(removeProcessor.getTag(), equalTo(processorTag));
        assertThat(removeProcessor.getFields().get(0).newInstance(Collections.emptyMap()).execute(), equalTo("field1"));
    }

    public void testCreateMultipleFields() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", Arrays.asList("field1", "field2"));
        String processorTag = randomAlphaOfLength(10);
        RemoveProcessor removeProcessor = factory.create(null, processorTag, null, config);
        assertThat(removeProcessor.getTag(), equalTo(processorTag));
        assertThat(
            removeProcessor.getFields()
                .stream()
                .map(template -> template.newInstance(Collections.emptyMap()).execute())
                .collect(Collectors.toList()),
            equalTo(Arrays.asList("field1", "field2"))
        );
    }

    public void testInvalidMustacheTemplate() throws Exception {
        RemoveProcessor.Factory factory = new RemoveProcessor.Factory(TestTemplateService.instance(true));
        Map<String, Object> config = new HashMap<>();
        config.put("field", "{{field1}}");
        String processorTag = randomAlphaOfLength(10);
        OpenSearchException exception = expectThrows(OpenSearchException.class, () -> factory.create(null, processorTag, null, config));
        assertThat(exception.getMessage(), equalTo("java.lang.RuntimeException: could not compile script"));
        assertThat(exception.getMetadata("opensearch.processor_tag").get(0), equalTo(processorTag));
    }

    public void testCreateWithExcludeField() throws Exception {
        Map<String, Object> config = new HashMap<>();
        String processorTag = randomAlphaOfLength(10);
        OpenSearchException exception = expectThrows(
            OpenSearchParseException.class,
            () -> factory.create(null, processorTag, null, config)
        );
        assertThat(exception.getMessage(), equalTo("[field] either field or exclude_field must be set"));

        Map<String, Object> config2 = new HashMap<>();
        config2.put("field", "field1");
        config2.put("exclude_field", "field2");
        exception = expectThrows(OpenSearchParseException.class, () -> factory.create(null, processorTag, null, config2));
        assertThat(exception.getMessage(), equalTo("[field] either field or exclude_field must be set"));

        Map<String, Object> config6 = new HashMap<>();
        config6.put("exclude_field", "exclude_field");
        RemoveProcessor removeProcessor = factory.create(null, processorTag, null, config6);
        assertThat(
            removeProcessor.getExcludeFields()
                .stream()
                .map(template -> template.newInstance(Collections.emptyMap()).execute())
                .collect(Collectors.toList()),
            equalTo(List.of("exclude_field"))
        );
    }
}
