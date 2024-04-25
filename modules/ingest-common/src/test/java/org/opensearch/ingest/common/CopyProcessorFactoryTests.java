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
import org.opensearch.ingest.TestTemplateService;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;

public class CopyProcessorFactoryTests extends OpenSearchTestCase {

    private CopyProcessor.Factory factory;

    @Before
    public void init() {
        factory = new CopyProcessor.Factory(TestTemplateService.instance());
    }

    public void testCreate() throws Exception {
        boolean ignoreMissing = randomBoolean();
        boolean removeSource = randomBoolean();
        boolean overrideTarget = randomBoolean();
        Map<String, Object> config = new HashMap<>();
        config.put("source_field", "source");
        config.put("target_field", "target");
        config.put("ignore_missing", ignoreMissing);
        config.put("remove_source", removeSource);
        config.put("override_target", overrideTarget);
        String processorTag = randomAlphaOfLength(10);
        CopyProcessor copyProcessor = factory.create(null, processorTag, null, config);
        assertThat(copyProcessor.getTag(), equalTo(processorTag));
        assertThat(copyProcessor.getSourceField().newInstance(Collections.emptyMap()).execute(), equalTo("source"));
        assertThat(copyProcessor.getTargetField().newInstance(Collections.emptyMap()).execute(), equalTo("target"));
        assertThat(copyProcessor.isIgnoreMissing(), equalTo(ignoreMissing));
        assertThat(copyProcessor.isRemoveSource(), equalTo(removeSource));
        assertThat(copyProcessor.isOverrideTarget(), equalTo(overrideTarget));
    }

    public void testCreateWithSourceField() throws Exception {
        Map<String, Object> config = new HashMap<>();
        try {
            factory.create(null, null, null, config);
            fail("factory create should have failed");
        } catch (OpenSearchParseException e) {
            assertThat(e.getMessage(), equalTo("[source_field] required property is missing"));
        }

        config.put("source_field", null);
        try {
            factory.create(null, null, null, config);
            fail("factory create should have failed");
        } catch (OpenSearchParseException e) {
            assertThat(e.getMessage(), equalTo("[source_field] required property is missing"));
        }
    }

    public void testCreateWithTargetField() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("source_field", "source");
        try {
            factory.create(null, null, null, config);
            fail("factory create should have failed");
        } catch (OpenSearchParseException e) {
            assertThat(e.getMessage(), equalTo("[target_field] required property is missing"));
        }

        config.put("source_field", "source");
        config.put("target_field", null);
        try {
            factory.create(null, null, null, config);
            fail("factory create should have failed");
        } catch (OpenSearchParseException e) {
            assertThat(e.getMessage(), equalTo("[target_field] required property is missing"));
        }
    }

    public void testInvalidMustacheTemplate() throws Exception {
        CopyProcessor.Factory factory = new CopyProcessor.Factory(TestTemplateService.instance(true));
        Map<String, Object> config = new HashMap<>();
        config.put("source_field", "{{source}}");
        config.put("target_field", "target");
        String processorTag = randomAlphaOfLength(10);
        OpenSearchException exception = expectThrows(OpenSearchException.class, () -> factory.create(null, processorTag, null, config));
        assertThat(exception.getMessage(), equalTo("java.lang.RuntimeException: could not compile script"));
        assertThat(exception.getMetadata("opensearch.processor_tag").get(0), equalTo(processorTag));
    }

}
