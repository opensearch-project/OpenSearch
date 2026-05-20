/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.common;

import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchParseException;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;

public class RemoveByPatternProcessorFactoryTests extends OpenSearchTestCase {

    private RemoveByPatternProcessor.Factory factory;

    @Before
    public void init() {
        factory = new RemoveByPatternProcessor.Factory();
    }

    public void testCreateFieldPatterns() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field_pattern", "field1*");
        String processorTag = randomAlphaOfLength(10);
        RemoveByPatternProcessor removeByPatternProcessor = factory.create(null, processorTag, null, config);
        assertThat(removeByPatternProcessor.getTag(), equalTo(processorTag));
        assertThat(removeByPatternProcessor.getFieldPatterns().get(0), equalTo("field1*"));

        Map<String, Object> config2 = new HashMap<>();
        config2.put("field_pattern", List.of("field1*", "field2*"));
        removeByPatternProcessor = factory.create(null, processorTag, null, config2);
        assertThat(removeByPatternProcessor.getTag(), equalTo(processorTag));
        assertThat(removeByPatternProcessor.getFieldPatterns().get(0), equalTo("field1*"));
        assertThat(removeByPatternProcessor.getFieldPatterns().get(1), equalTo("field2*"));

        Map<String, Object> config3 = new HashMap<>();
        List<String> patterns = Arrays.asList("foo*", "*", " ", ",", "#", ":", "_");
        config3.put("field_pattern", patterns);
        Exception exception = expectThrows(OpenSearchParseException.class, () -> factory.create(null, processorTag, null, config3));
        assertThat(
            exception.getMessage(),
            equalTo(
                "[field_pattern] Validation Failed: "
                    + "1: field_pattern [ ] must not contain the following characters [ , \", *, \\, <, |, ,, >, /, ?];"
                    + "2: field_pattern [,] must not contain the following characters [ , \", *, \\, <, |, ,, >, /, ?];"
                    + "3: field_pattern [#] must not contain a '#';"
                    + "4: field_pattern [:] must not contain a ':';"
                    + "5: field_pattern [_] must not start with '_';"
            )
        );
    }

    public void testCreateExcludeFieldPatterns() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("exclude_field_pattern", "field1*");
        String processorTag = randomAlphaOfLength(10);
        RemoveByPatternProcessor removeByPatternProcessor = factory.create(null, processorTag, null, config);
        assertThat(removeByPatternProcessor.getTag(), equalTo(processorTag));
        assertThat(removeByPatternProcessor.getExcludeFieldPatterns().get(0), equalTo("field1*"));

        Map<String, Object> config2 = new HashMap<>();
        config2.put("exclude_field_pattern", List.of("field1*", "field2*"));
        removeByPatternProcessor = factory.create(null, processorTag, null, config2);
        assertThat(removeByPatternProcessor.getTag(), equalTo(processorTag));
        assertThat(removeByPatternProcessor.getExcludeFieldPatterns().get(0), equalTo("field1*"));
        assertThat(removeByPatternProcessor.getExcludeFieldPatterns().get(1), equalTo("field2*"));

        Map<String, Object> config3 = new HashMap<>();
        List<String> patterns = Arrays.asList("foo*", "*", " ", ",", "#", ":", "_");
        config3.put("exclude_field_pattern", patterns);
        Exception exception = expectThrows(OpenSearchParseException.class, () -> factory.create(null, processorTag, null, config3));
        assertThat(
            exception.getMessage(),
            equalTo(
                "[exclude_field_pattern] Validation Failed: "
                    + "1: exclude_field_pattern [ ] must not contain the following characters [ , \", *, \\, <, |, ,, >, /, ?];"
                    + "2: exclude_field_pattern [,] must not contain the following characters [ , \", *, \\, <, |, ,, >, /, ?];"
                    + "3: exclude_field_pattern [#] must not contain a '#';"
                    + "4: exclude_field_pattern [:] must not contain a ':';"
                    + "5: exclude_field_pattern [_] must not start with '_';"
            )
        );
    }

    public void testCreatePatternsFailed() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field_pattern", List.of("foo*"));
        config.put("exclude_field_pattern", List.of("bar*"));
        String processorTag = randomAlphaOfLength(10);
        OpenSearchException exception = expectThrows(
            OpenSearchParseException.class,
            () -> factory.create(null, processorTag, null, config)
        );
        assertThat(exception.getMessage(), equalTo("[field_pattern] either field_pattern or exclude_field_pattern must be set"));

        Map<String, Object> config2 = new HashMap<>();
        config2.put("field_pattern", null);
        config2.put("exclude_field_pattern", null);

        exception = expectThrows(OpenSearchParseException.class, () -> factory.create(null, processorTag, null, config2));
        assertThat(exception.getMessage(), equalTo("[field_pattern] either field_pattern or exclude_field_pattern must be set"));
    }
}
