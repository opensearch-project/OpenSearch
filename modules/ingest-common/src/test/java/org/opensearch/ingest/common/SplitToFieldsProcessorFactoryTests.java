/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.common;

import org.opensearch.OpenSearchParseException;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;

public class SplitToFieldsProcessorFactoryTests extends OpenSearchTestCase {

    public void testCreate() throws Exception {
        SplitToFieldsProcessor.Factory factory = new SplitToFieldsProcessor.Factory();
        boolean ignoreMissing = randomBoolean();
        boolean preserveTrailing = randomBoolean();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "source_field");
        config.put("separator", ">");
        config.put("target_fields", Arrays.asList("out1", "out2", "out3"));
        config.put("ignore_missing", ignoreMissing);
        config.put("preserve_trailing", preserveTrailing);
        String processorTag = randomAlphaOfLength(10);
        SplitToFieldsProcessor processor = factory.create(null, processorTag, null, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo("source_field"));
        assertThat(processor.getSeparator(), equalTo(">"));
        assertThat(processor.getTargetFields(), equalTo(Arrays.asList("out1", "out2", "out3")));
        assertThat(processor.isIgnoreMissing(), equalTo(ignoreMissing));
        assertThat(processor.isPreserveTrailing(), equalTo(preserveTrailing));
    }

    public void testCreateNoFieldPresent() throws Exception {
        SplitToFieldsProcessor.Factory factory = new SplitToFieldsProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("separator", ">");
        config.put("target_fields", Arrays.asList("out1", "out2"));
        try {
            factory.create(null, null, null, config);
            fail("factory create should have failed");
        } catch (OpenSearchParseException e) {
            assertThat(e.getMessage(), equalTo("[field] required property is missing"));
        }
    }

    public void testCreateNoSeparatorPresent() throws Exception {
        SplitToFieldsProcessor.Factory factory = new SplitToFieldsProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "source_field");
        config.put("target_fields", Arrays.asList("out1", "out2"));
        try {
            factory.create(null, null, null, config);
            fail("factory create should have failed");
        } catch (OpenSearchParseException e) {
            assertThat(e.getMessage(), equalTo("[separator] required property is missing"));
        }
    }

    public void testCreateNoTargetFieldsPresent() throws Exception {
        SplitToFieldsProcessor.Factory factory = new SplitToFieldsProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "source_field");
        config.put("separator", ">");
        try {
            factory.create(null, null, null, config);
            fail("factory create should have failed");
        } catch (OpenSearchParseException e) {
            assertThat(e.getMessage(), equalTo("[target_fields] required property is missing"));
        }
    }
}
