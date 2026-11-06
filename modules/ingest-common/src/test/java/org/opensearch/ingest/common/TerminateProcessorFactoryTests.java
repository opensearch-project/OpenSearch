/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.common;

import org.opensearch.OpenSearchParseException;
import org.opensearch.ingest.CompoundProcessor;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.ingest.Processor;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class TerminateProcessorFactoryTests extends OpenSearchTestCase {

    private TerminateProcessor.Factory factory;

    @Before
    public void init() {
        factory = new TerminateProcessor.Factory();
    }

    public void testCreate() throws Exception {
        Map<String, Object> config = new HashMap<>();

        TerminateProcessor processor = factory.create(null, null, null, config);

        assertThat(processor.getType(), equalTo("terminate"));
        assertThat(processor.getTag(), nullValue());
        assertThat(processor.getDescription(), nullValue());
    }

    public void testCreateWithTagAndDescription() throws Exception {
        String tag = randomAlphaOfLength(10);
        String description = randomAlphaOfLength(20);

        TerminateProcessor processor = factory.create(null, tag, description, new HashMap<>());

        assertThat(processor.getTag(), equalTo(tag));
        assertThat(processor.getDescription(), equalTo(description));
    }

    public void testTagAndDescriptionAreReadFromPipelineConfiguration() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("tag", "skip-parsing");
        config.put("description", "stop here");

        Processor processor = ConfigurationUtils.readProcessor(Map.of("terminate", factory), null, "terminate", config);

        assertThat(processor.getType(), equalTo("terminate"));
        assertThat(processor.getTag(), equalTo("skip-parsing"));
        assertThat(processor.getDescription(), equalTo("stop here"));
    }

    public void testUnknownParameterIsRejected() {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "message");

        OpenSearchParseException e = expectThrows(
            OpenSearchParseException.class,
            () -> ConfigurationUtils.readProcessor(Map.of("terminate", factory), null, "terminate", config)
        );

        assertThat(e.getMessage(), containsString("doesn't support one or more provided configuration parameters [field]"));
    }

    public void testIgnoreFailureIsSupported() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("ignore_failure", true);

        Processor processor = ConfigurationUtils.readProcessor(Map.of("terminate", factory), null, "terminate", config);

        // like for every other processor, ignore_failure wraps the processor in a compound processor
        assertThat(processor, instanceOf(CompoundProcessor.class));
        List<Processor> wrapped = ((CompoundProcessor) processor).getProcessors();
        assertThat(wrapped.size(), equalTo(1));
        assertThat(wrapped.get(0), instanceOf(TerminateProcessor.class));
        assertThat(((CompoundProcessor) processor).isIgnoreFailure(), equalTo(true));
    }

    public void testOnFailureIsSupported() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("on_failure", List.of(Map.of("terminate", new HashMap<String, Object>())));

        Processor processor = ConfigurationUtils.readProcessor(Map.of("terminate", factory), null, "terminate", config);

        // like for every other processor, on_failure wraps the processor in a compound processor
        assertThat(processor, instanceOf(CompoundProcessor.class));
        CompoundProcessor compound = (CompoundProcessor) processor;
        assertThat(compound.getProcessors().size(), equalTo(1));
        assertThat(compound.getProcessors().get(0), instanceOf(TerminateProcessor.class));
        assertThat(compound.getOnFailureProcessors().size(), equalTo(1));
        assertThat(compound.getOnFailureProcessors().get(0), instanceOf(TerminateProcessor.class));
    }
}
