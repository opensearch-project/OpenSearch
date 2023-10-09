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

    public void testCreateWithFieldPatterns() throws Exception {
        Map<String, Object> config = new HashMap<>();
        List<String> patterns = List.of("foo*");
        config.put("field_pattern", patterns);
        config.put("exclude_field", "field");
        String processorTag = randomAlphaOfLength(10);
        OpenSearchException exception = expectThrows(
            OpenSearchParseException.class,
            () -> factory.create(null, processorTag, null, config)
        );
        assertThat(
            exception.getMessage(),
            equalTo("[field] ether (field,field_pattern) or (exclude_field,exclude_field_pattern) can be set")
        );

        Map<String, Object> config2 = new HashMap<>();
        patterns = Arrays.asList("foo*", "*", " ", ",", "#", ":", "_");
        config2.put("field_pattern", patterns);
        exception = expectThrows(OpenSearchParseException.class, () -> factory.create(null, processorTag, null, config2));
        assertThat(
            exception.getMessage(),
            equalTo(
                "[field_pattern] Validation Failed: 1: field_pattern [ ] must not contain a space;"
                    + "2: field_pattern [ ] must not contain the following characters [ , \", *, \\, <, |, ,, >, /, ?];"
                    + "3: field_pattern [,] must not contain a ',';"
                    + "4: field_pattern [,] must not contain the following characters [ , \", *, \\, <, |, ,, >, /, ?];"
                    + "5: field_pattern [#] must not contain a '#';"
                    + "6: field_pattern [:] must not contain a ':';"
                    + "7: field_pattern [_] must not start with '_';"
            )
        );

        Map<String, Object> config3 = new HashMap<>();
        patterns = Arrays.asList("foo*", "*", " ", ",", "#", ":", "_");
        config3.put("exclude_field_pattern", patterns);
        exception = expectThrows(OpenSearchParseException.class, () -> factory.create(null, processorTag, null, config3));
        assertThat(
            exception.getMessage(),
            equalTo(
                "[exclude_field_pattern] Validation Failed: 1: exclude_field_pattern [ ] must not contain a space;"
                    + "2: exclude_field_pattern [ ] must not contain the following characters [ , \", *, \\, <, |, ,, >, /, ?];"
                    + "3: exclude_field_pattern [,] must not contain a ',';"
                    + "4: exclude_field_pattern [,] must not contain the following characters [ , \", *, \\, <, |, ,, >, /, ?];"
                    + "5: exclude_field_pattern [#] must not contain a '#';"
                    + "6: exclude_field_pattern [:] must not contain a ':';"
                    + "7: exclude_field_pattern [_] must not start with '_';"
            )
        );

        Map<String, Object> config4 = new HashMap<>();
        exception = expectThrows(OpenSearchParseException.class, () -> factory.create(null, processorTag, null, config4));
        assertThat(
            exception.getMessage(),
            equalTo("[field] at least one of the parameters field, field_pattern, exclude_field and exclude_field_pattern need to be set")
        );

        Map<String, Object> config5 = new HashMap<>();
        config5.put("field_pattern", "field*");
        RemoveProcessor removeProcessor = factory.create(null, processorTag, null, config5);
        assertThat(removeProcessor.getFieldPatterns(), equalTo(List.of("field*")));

        Map<String, Object> config6 = new HashMap<>();
        config6.put("exclude_field", "exclude_field");
        removeProcessor = factory.create(null, processorTag, null, config6);
        assertThat(
            removeProcessor.getExcludeFields()
                .stream()
                .map(template -> template.newInstance(Collections.emptyMap()).execute())
                .collect(Collectors.toList()),
            equalTo(List.of("exclude_field"))
        );

        Map<String, Object> config7 = new HashMap<>();
        config7.put("exclude_field_pattern", "exclude_field*");
        removeProcessor = factory.create(null, processorTag, null, config7);
        assertThat(removeProcessor.getExcludeFieldPatterns(), equalTo(List.of("exclude_field*")));
    }
}
