/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest;

import org.opensearch.OpenSearchParseException;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.opensearch.ingest.PipelineProcessorTests.createIngestService;
import static org.hamcrest.CoreMatchers.equalTo;

public class PipelineProcessorFactoryTests extends OpenSearchTestCase {
    private PipelineProcessor.Factory factory;

    @Before
    public void init() {
        factory = new PipelineProcessor.Factory(createIngestService());
    }

    public void testCreate() throws Exception {
        Map<String, Object> config = new HashMap<>();
        boolean ignoreMissingPipeline = randomBoolean();
        config.put("name", "pipeline_name");
        config.put("ignore_missing_pipeline", ignoreMissingPipeline);
        String processorTag = randomAlphaOfLength(10);
        PipelineProcessor pipelineProcessor = factory.create(null, processorTag, null, config);
        assertThat(pipelineProcessor.getTag(), equalTo(processorTag));
        assertThat(pipelineProcessor.getPipelineTemplate().newInstance(Collections.emptyMap()).execute(), equalTo("pipeline_name"));
        assertThat(pipelineProcessor.isIgnoreMissingPipeline(), equalTo(ignoreMissingPipeline));
    }

    public void testCreateNoPipelinePresent() throws Exception {
        Map<String, Object> config = new HashMap<>();
        try {
            factory.create(null, null, null, config);
            fail("factory create should have failed");
        } catch (OpenSearchParseException e) {
            assertThat(e.getMessage(), equalTo("[name] required property is missing"));
        }
    }
}
