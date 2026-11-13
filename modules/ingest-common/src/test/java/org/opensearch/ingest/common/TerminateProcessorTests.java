/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.common;

import org.opensearch.ingest.CompoundProcessor;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.IngestDocumentWrapper;
import org.opensearch.ingest.Pipeline;
import org.opensearch.ingest.Processor;
import org.opensearch.ingest.RandomDocumentPicks;
import org.opensearch.ingest.TestTemplateService;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class TerminateProcessorTests extends OpenSearchTestCase {

    public void testExecuteRequestsTerminationAndReturnsTheDocument() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        Processor processor = new TerminateProcessor(randomAlphaOfLength(5), null);

        IngestDocument result = processor.execute(ingestDocument);

        assertThat(result, sameInstance(ingestDocument));
        assertThat(ingestDocument.isPipelineTerminated(), is(true));
    }

    public void testExecuteDoesNotChangeTheDocument() throws Exception {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        IngestDocument original = new IngestDocument(ingestDocument);

        new TerminateProcessor(null, null).execute(ingestDocument);

        assertIngestDocument(ingestDocument, original);
    }

    public void testExecuteWithHandlerIsASuccess() {
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random());
        IngestDocument[] result = new IngestDocument[1];
        Exception[] failure = new Exception[1];

        new TerminateProcessor(null, null).execute(ingestDocument, (r, e) -> {
            result[0] = r;
            failure[0] = e;
        });

        assertThat(failure[0], nullValue());
        assertThat(result[0], sameInstance(ingestDocument));
        assertThat(ingestDocument.isPipelineTerminated(), is(true));
    }

    public void testBatchExecuteRequestsTerminationForEveryDocument() {
        List<IngestDocumentWrapper> wrappers = new ArrayList<>();
        int numberOfDocuments = randomIntBetween(1, 8);
        for (int slot = 0; slot < numberOfDocuments; slot++) {
            wrappers.add(new IngestDocumentWrapper(slot, 0, RandomDocumentPicks.randomIngestDocument(random()), null));
        }
        List<IngestDocumentWrapper> results = new ArrayList<>();

        new TerminateProcessor(null, null).batchExecute(wrappers, results::addAll);

        assertThat(results, hasSize(numberOfDocuments));
        for (IngestDocumentWrapper result : results) {
            assertThat(result.getException(), nullValue());
            assertThat(result.getIngestDocument(), notNullValue());
            assertThat(result.getIngestDocument().isPipelineTerminated(), is(true));
        }
    }

    public void testTypeTagAndDescription() {
        String tag = randomAlphaOfLength(8);
        String description = randomAlphaOfLength(12);
        Processor processor = new TerminateProcessor(tag, description);

        assertThat(processor.getType(), equalTo("terminate"));
        assertThat(processor.getType(), equalTo(TerminateProcessor.TYPE));
        assertThat(processor.getTag(), equalTo(tag));
        assertThat(processor.getDescription(), equalTo(description));
    }

    public void testSkipsTheRemainingProcessorsOfThePipeline() throws Exception {
        SetProcessor.Factory setFactory = new SetProcessor.Factory(TestTemplateService.instance());
        Processor before = setFactory.create(null, "before", null, config("field", "before", "value", "ran"));
        Processor terminate = new TerminateProcessor("stop", null);
        Processor after = setFactory.create(null, "after", null, config("field", "after", "value", "ran"));
        Pipeline pipeline = new Pipeline("_id", null, null, new CompoundProcessor(before, terminate, after));
        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());

        IngestDocument[] result = new IngestDocument[1];
        Exception[] failure = new Exception[1];
        pipeline.execute(ingestDocument, (r, e) -> {
            result[0] = r;
            failure[0] = e;
        });

        assertThat(failure[0], nullValue());
        assertThat(result[0], sameInstance(ingestDocument));
        assertThat(ingestDocument.getFieldValue("before", String.class), equalTo("ran"));
        assertThat(ingestDocument.hasField("after"), is(false));
        // the document ends up as if the pipeline had finished: not terminated, not failed, no failure metadata
        assertThat(ingestDocument.isPipelineTerminated(), is(false));
        assertThat(ingestDocument.getIngestMetadata().keySet(), equalTo(Collections.emptySet()));
        assertThat(pipeline.getMetrics().createStats().getFailedCount(), equalTo(0L));
    }

    private static Map<String, Object> config(String... keysAndValues) {
        Map<String, Object> config = new HashMap<>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            config.put(keysAndValues[i], keysAndValues[i + 1]);
        }
        return config;
    }
}
