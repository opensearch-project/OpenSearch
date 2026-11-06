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

package org.opensearch.ingest;

import org.opensearch.OpenSearchException;
import org.opensearch.common.metrics.OperationStats;
import org.opensearch.script.ScriptService;
import org.opensearch.script.TemplateScript;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PipelineProcessorTests extends OpenSearchTestCase {

    public void testExecutesPipeline() throws Exception {
        String pipelineId = "pipeline";
        IngestService ingestService = createIngestService();
        CompletableFuture<IngestDocument> invoked = new CompletableFuture<>();
        IngestDocument testIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        Pipeline pipeline = new Pipeline(pipelineId, null, null, new CompoundProcessor(new Processor() {
            @Override
            public IngestDocument execute(final IngestDocument ingestDocument) throws Exception {
                invoked.complete(ingestDocument);
                return ingestDocument;
            }

            @Override
            public String getType() {
                return null;
            }

            @Override
            public String getTag() {
                return null;
            }

            @Override
            public String getDescription() {
                return null;
            }
        }));
        when(ingestService.getPipeline(pipelineId)).thenReturn(pipeline);
        PipelineProcessor.Factory factory = new PipelineProcessor.Factory(ingestService);
        Map<String, Object> config = new HashMap<>();
        config.put("name", pipelineId);
        factory.create(Collections.emptyMap(), null, null, config).execute(testIngestDocument, (result, e) -> {});
        assertEquals(testIngestDocument, invoked.get());
    }

    public void testThrowsOnMissingPipeline() throws Exception {
        IngestService ingestService = createIngestService();
        IngestDocument testIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        PipelineProcessor.Factory factory = new PipelineProcessor.Factory(ingestService);
        Map<String, Object> config = new HashMap<>();
        config.put("name", "missingPipelineId");
        IllegalStateException[] e = new IllegalStateException[1];
        factory.create(Collections.emptyMap(), null, null, config)
            .execute(testIngestDocument, (result, e1) -> e[0] = (IllegalStateException) e1);
        assertEquals("Pipeline processor configured for non-existent pipeline [missingPipelineId]", e[0].getMessage());
    }

    public void testThrowsOnRecursivePipelineInvocations() throws Exception {
        String innerPipelineId = "inner";
        String outerPipelineId = "outer";
        IngestService ingestService = createIngestService();
        IngestDocument testIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        Map<String, Object> outerConfig = new HashMap<>();
        outerConfig.put("name", innerPipelineId);
        PipelineProcessor.Factory factory = new PipelineProcessor.Factory(ingestService);
        Pipeline outer = new Pipeline(
            outerPipelineId,
            null,
            null,
            new CompoundProcessor(factory.create(Collections.emptyMap(), null, null, outerConfig))
        );
        Map<String, Object> innerConfig = new HashMap<>();
        innerConfig.put("name", outerPipelineId);
        Pipeline inner = new Pipeline(
            innerPipelineId,
            null,
            null,
            new CompoundProcessor(factory.create(Collections.emptyMap(), null, null, innerConfig))
        );
        when(ingestService.getPipeline(outerPipelineId)).thenReturn(outer);
        when(ingestService.getPipeline(innerPipelineId)).thenReturn(inner);
        outerConfig.put("name", innerPipelineId);
        OpenSearchException[] e = new OpenSearchException[1];
        factory.create(Collections.emptyMap(), null, null, outerConfig)
            .execute(testIngestDocument, (result, e1) -> e[0] = (OpenSearchException) e1);
        assertEquals("Cycle detected for pipeline: inner", e[0].getRootCause().getMessage());
    }

    public void testAllowsRepeatedPipelineInvocations() throws Exception {
        String innerPipelineId = "inner";
        IngestService ingestService = createIngestService();
        IngestDocument testIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        Map<String, Object> outerConfig = new HashMap<>();
        outerConfig.put("name", innerPipelineId);
        PipelineProcessor.Factory factory = new PipelineProcessor.Factory(ingestService);
        Pipeline inner = new Pipeline(innerPipelineId, null, null, new CompoundProcessor());
        when(ingestService.getPipeline(innerPipelineId)).thenReturn(inner);
        Processor outerProc = factory.create(Collections.emptyMap(), null, null, outerConfig);
        outerProc.execute(testIngestDocument, (result, e) -> {});
        outerProc.execute(testIngestDocument, (result, e) -> {});
    }

    public void testPipelineProcessorWithPipelineChain() throws Exception {
        String pipeline1Id = "pipeline1";
        String pipeline2Id = "pipeline2";
        String pipeline3Id = "pipeline3";
        IngestService ingestService = createIngestService();
        PipelineProcessor.Factory factory = new PipelineProcessor.Factory(ingestService);

        Map<String, Object> pipeline1ProcessorConfig = new HashMap<>();
        pipeline1ProcessorConfig.put("name", pipeline2Id);
        PipelineProcessor pipeline1Processor = factory.create(Collections.emptyMap(), null, null, pipeline1ProcessorConfig);

        Map<String, Object> pipeline2ProcessorConfig = new HashMap<>();
        pipeline2ProcessorConfig.put("name", pipeline3Id);
        PipelineProcessor pipeline2Processor = factory.create(Collections.emptyMap(), null, null, pipeline2ProcessorConfig);

        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L);
        Pipeline pipeline1 = new Pipeline(pipeline1Id, null, null, new CompoundProcessor(pipeline1Processor), relativeTimeProvider);

        String key1 = randomAlphaOfLength(10);
        relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L, TimeUnit.MILLISECONDS.toNanos(3));
        Pipeline pipeline2 = new Pipeline(
            pipeline2Id,
            null,
            null,
            new CompoundProcessor(true, Arrays.asList(new TestProcessor(ingestDocument -> {
                ingestDocument.setFieldValue(key1, randomInt());
            }), pipeline2Processor), Collections.emptyList()),
            relativeTimeProvider
        );
        relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L, TimeUnit.MILLISECONDS.toNanos(2));
        Pipeline pipeline3 = new Pipeline(pipeline3Id, null, null, new CompoundProcessor(new TestProcessor(ingestDocument -> {
            throw new RuntimeException("error");
        })), relativeTimeProvider);
        when(ingestService.getPipeline(pipeline1Id)).thenReturn(pipeline1);
        when(ingestService.getPipeline(pipeline2Id)).thenReturn(pipeline2);
        when(ingestService.getPipeline(pipeline3Id)).thenReturn(pipeline3);

        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        // start the chain
        ingestDocument.executePipeline(pipeline1, (result, e) -> {});
        assertNotNull(ingestDocument.getSourceAndMetadata().get(key1));

        // check the stats
        OperationStats pipeline1Stats = pipeline1.getMetrics().createStats();
        OperationStats pipeline2Stats = pipeline2.getMetrics().createStats();
        OperationStats pipeline3Stats = pipeline3.getMetrics().createStats();

        // current
        assertThat(pipeline1Stats.getCurrent(), equalTo(0L));
        assertThat(pipeline2Stats.getCurrent(), equalTo(0L));
        assertThat(pipeline3Stats.getCurrent(), equalTo(0L));

        // count
        assertThat(pipeline1Stats.getCount(), equalTo(1L));
        assertThat(pipeline2Stats.getCount(), equalTo(1L));
        assertThat(pipeline3Stats.getCount(), equalTo(1L));

        // time
        assertThat(pipeline1Stats.getTotalTime(), equalTo(0L));
        assertThat(pipeline2Stats.getTotalTime(), equalTo(3L));
        assertThat(pipeline3Stats.getTotalTime(), equalTo(2L));

        // failure
        assertThat(pipeline1Stats.getFailedCount(), equalTo(0L));
        assertThat(pipeline2Stats.getFailedCount(), equalTo(0L));
        assertThat(pipeline3Stats.getFailedCount(), equalTo(1L));
    }

    public void testIngestPipelineMetadata() {
        IngestService ingestService = createIngestService();

        final int numPipelines = 16;
        Pipeline firstPipeline = null;
        for (int i = 0; i < numPipelines; i++) {
            String pipelineId = Integer.toString(i);
            List<Processor> processors = new ArrayList<>();
            processors.add(new AbstractProcessor(null, null) {
                @Override
                public IngestDocument execute(final IngestDocument ingestDocument) throws Exception {
                    ingestDocument.appendFieldValue("pipelines", ingestDocument.getIngestMetadata().get("pipeline"));
                    return ingestDocument;
                }

                @Override
                public String getType() {
                    return null;
                }

            });
            if (i < (numPipelines - 1)) {
                TemplateScript.Factory pipelineName = new TestTemplateService.MockTemplateScript.Factory(Integer.toString(i + 1));
                processors.add(new PipelineProcessor(null, null, pipelineName, ingestService));
            }

            Pipeline pipeline = new Pipeline(pipelineId, null, null, new CompoundProcessor(false, processors, Collections.emptyList()));
            when(ingestService.getPipeline(pipelineId)).thenReturn(pipeline);
            if (firstPipeline == null) {
                firstPipeline = pipeline;
            }
        }

        IngestDocument testIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
        IngestDocument[] docHolder = new IngestDocument[1];
        Exception[] errorHolder = new Exception[1];
        testIngestDocument.executePipeline(firstPipeline, (doc, e) -> {
            docHolder[0] = doc;
            errorHolder[0] = e;
        });
        assertThat(docHolder[0], notNullValue());
        assertThat(errorHolder[0], nullValue());

        IngestDocument ingestDocument = docHolder[0];
        List<?> pipelines = ingestDocument.getFieldValue("pipelines", List.class);
        assertThat(pipelines.size(), equalTo(numPipelines));
        for (int i = 0; i < numPipelines; i++) {
            assertThat(pipelines.get(i), equalTo(Integer.toString(i)));
        }
    }

    public void testTerminateInCalledPipelineDoesNotEndCallingPipeline() {
        IngestService ingestService = createIngestService();
        TestProcessor innerFirst = new TestProcessor(doc -> {});
        TestProcessor innerTerminator = terminator();
        TestProcessor innerLast = new TestProcessor(doc -> {});
        Pipeline inner = new Pipeline("inner", null, null, new CompoundProcessor(innerFirst, innerTerminator, innerLast));
        when(ingestService.getPipeline("inner")).thenReturn(inner);
        List<Boolean> terminatedSeenByCaller = new ArrayList<>();
        TestProcessor outerBefore = new TestProcessor(doc -> {});
        TestProcessor outerProbe = new TestProcessor(doc -> { terminatedSeenByCaller.add(doc.isPipelineTerminated()); });
        TestProcessor outerAfter = new TestProcessor(doc -> {});
        Pipeline outer = new Pipeline(
            "outer",
            null,
            null,
            new CompoundProcessor(outerBefore, callPipeline("inner", ingestService), outerProbe, outerAfter)
        );
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());

        IngestDocument[] result = new IngestDocument[1];
        Exception[] failure = new Exception[1];
        ingestDocument.executePipeline(outer, (r, e) -> {
            result[0] = r;
            failure[0] = e;
        });

        assertThat(failure[0], nullValue());
        assertThat(result[0], equalTo(ingestDocument));
        assertThat(innerFirst.getInvokedCounter(), equalTo(1));
        assertThat(innerTerminator.getInvokedCounter(), equalTo(1));
        assertThat(innerLast.getInvokedCounter(), equalTo(0));
        // the calling pipeline carries on, and never sees the request of the pipeline that it called
        assertThat(outerBefore.getInvokedCounter(), equalTo(1));
        assertThat(outerProbe.getInvokedCounter(), equalTo(1));
        assertThat(outerAfter.getInvokedCounter(), equalTo(1));
        assertThat(terminatedSeenByCaller, equalTo(List.of(false)));
        assertThat(ingestDocument.isPipelineTerminated(), equalTo(false));
        // the called pipeline finished successfully
        assertThat(inner.getMetrics().createStats().getCount(), equalTo(1L));
        assertThat(inner.getMetrics().createStats().getFailedCount(), equalTo(0L));
        assertThat(outer.getMetrics().createStats().getFailedCount(), equalTo(0L));
    }

    public void testTerminateInCallingPipelineAfterCalledPipelineCompleted() {
        IngestService ingestService = createIngestService();
        TestProcessor innerFirst = new TestProcessor(doc -> {});
        TestProcessor innerLast = new TestProcessor(doc -> {});
        Pipeline inner = new Pipeline("inner", null, null, new CompoundProcessor(innerFirst, innerLast));
        when(ingestService.getPipeline("inner")).thenReturn(inner);
        TestProcessor outerTerminator = terminator();
        TestProcessor outerAfter = new TestProcessor(doc -> {});
        Pipeline outer = new Pipeline(
            "outer",
            null,
            null,
            new CompoundProcessor(callPipeline("inner", ingestService), outerTerminator, outerAfter)
        );
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());

        IngestDocument[] result = new IngestDocument[1];
        Exception[] failure = new Exception[1];
        ingestDocument.executePipeline(outer, (r, e) -> {
            result[0] = r;
            failure[0] = e;
        });

        assertThat(failure[0], nullValue());
        // the pipeline completed, rather than the handler never having been called
        assertThat(result[0], sameInstance(ingestDocument));
        assertThat(innerFirst.getInvokedCounter(), equalTo(1));
        assertThat(innerLast.getInvokedCounter(), equalTo(1));
        assertThat(outerTerminator.getInvokedCounter(), equalTo(1));
        assertThat(outerAfter.getInvokedCounter(), equalTo(0));
        assertThat(ingestDocument.isPipelineTerminated(), equalTo(false));
    }

    public void testTerminateInInnermostOfThreeNestedPipelinesOnlyEndsThatPipeline() {
        IngestService ingestService = createIngestService();
        TestProcessor cTerminator = terminator();
        TestProcessor cLast = new TestProcessor(doc -> {});
        Pipeline c = new Pipeline("c", null, null, new CompoundProcessor(cTerminator, cLast));
        TestProcessor bAfter = new TestProcessor(doc -> {});
        Pipeline b = new Pipeline("b", null, null, new CompoundProcessor(callPipeline("c", ingestService), bAfter));
        TestProcessor aAfter = new TestProcessor(doc -> {});
        Pipeline a = new Pipeline("a", null, null, new CompoundProcessor(callPipeline("b", ingestService), aAfter));
        when(ingestService.getPipeline("b")).thenReturn(b);
        when(ingestService.getPipeline("c")).thenReturn(c);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());

        IngestDocument[] result = new IngestDocument[1];
        Exception[] failure = new Exception[1];
        ingestDocument.executePipeline(a, (r, e) -> {
            result[0] = r;
            failure[0] = e;
        });

        assertThat(failure[0], nullValue());
        // the pipeline completed, rather than the handler never having been called
        assertThat(result[0], sameInstance(ingestDocument));
        assertThat(cTerminator.getInvokedCounter(), equalTo(1));
        assertThat(cLast.getInvokedCounter(), equalTo(0));
        assertThat(bAfter.getInvokedCounter(), equalTo(1));
        assertThat(aAfter.getInvokedCounter(), equalTo(1));
        assertThat(ingestDocument.isPipelineTerminated(), equalTo(false));
    }

    public void testTerminateInMiddleOfThreeNestedPipelinesSkipsOnlyItsOwnRemainder() {
        IngestService ingestService = createIngestService();
        TestProcessor cFirst = new TestProcessor(doc -> {});
        Pipeline c = new Pipeline("c", null, null, new CompoundProcessor(cFirst));
        TestProcessor bTerminator = terminator();
        TestProcessor bAfter = new TestProcessor(doc -> {});
        Pipeline b = new Pipeline("b", null, null, new CompoundProcessor(callPipeline("c", ingestService), bTerminator, bAfter));
        TestProcessor aAfter = new TestProcessor(doc -> {});
        Pipeline a = new Pipeline("a", null, null, new CompoundProcessor(callPipeline("b", ingestService), aAfter));
        when(ingestService.getPipeline("b")).thenReturn(b);
        when(ingestService.getPipeline("c")).thenReturn(c);
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());

        IngestDocument[] result = new IngestDocument[1];
        Exception[] failure = new Exception[1];
        ingestDocument.executePipeline(a, (r, e) -> {
            result[0] = r;
            failure[0] = e;
        });

        assertThat(failure[0], nullValue());
        // the pipeline completed, rather than the handler never having been called
        assertThat(result[0], sameInstance(ingestDocument));
        assertThat(cFirst.getInvokedCounter(), equalTo(1));
        assertThat(bTerminator.getInvokedCounter(), equalTo(1));
        assertThat(bAfter.getInvokedCounter(), equalTo(0));
        assertThat(aAfter.getInvokedCounter(), equalTo(1));
        assertThat(ingestDocument.isPipelineTerminated(), equalTo(false));
    }

    public void testTerminateRequestDoesNotLeakBetweenRepeatedCallsOfTheSamePipeline() {
        IngestService ingestService = createIngestService();
        TestProcessor innerTerminator = terminator();
        TestProcessor innerLast = new TestProcessor(doc -> {});
        Pipeline inner = new Pipeline("inner", null, null, new CompoundProcessor(innerTerminator, innerLast));
        when(ingestService.getPipeline("inner")).thenReturn(inner);
        TestProcessor outerAfter = new TestProcessor(doc -> {});
        Pipeline outer = new Pipeline(
            "outer",
            null,
            null,
            new CompoundProcessor(callPipeline("inner", ingestService), callPipeline("inner", ingestService), outerAfter)
        );
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());

        IngestDocument[] result = new IngestDocument[1];
        Exception[] failure = new Exception[1];
        ingestDocument.executePipeline(outer, (r, e) -> {
            result[0] = r;
            failure[0] = e;
        });

        assertThat(failure[0], nullValue());
        // the pipeline completed, rather than the handler never having been called
        assertThat(result[0], sameInstance(ingestDocument));
        assertThat(innerTerminator.getInvokedCounter(), equalTo(2));
        assertThat(innerLast.getInvokedCounter(), equalTo(0));
        assertThat(outerAfter.getInvokedCounter(), equalTo(1));
    }

    public void testTerminateInCalledPipelineWithIgnoreFailureOnPipelineProcessor() {
        IngestService ingestService = createIngestService();
        TestProcessor innerTerminator = terminator();
        TestProcessor innerLast = new TestProcessor(doc -> {});
        Pipeline inner = new Pipeline("inner", null, null, new CompoundProcessor(innerTerminator, innerLast));
        when(ingestService.getPipeline("inner")).thenReturn(inner);
        TestProcessor outerAfter = new TestProcessor(doc -> {});
        // ignore_failure wraps the pipeline processor in a compound processor of its own
        CompoundProcessor wrappedCall = new CompoundProcessor(true, List.of(callPipeline("inner", ingestService)), Collections.emptyList());
        Pipeline outer = new Pipeline("outer", null, null, new CompoundProcessor(wrappedCall, outerAfter));
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());

        IngestDocument[] result = new IngestDocument[1];
        Exception[] failure = new Exception[1];
        ingestDocument.executePipeline(outer, (r, e) -> {
            result[0] = r;
            failure[0] = e;
        });

        assertThat(failure[0], nullValue());
        // the pipeline completed, rather than the handler never having been called
        assertThat(result[0], sameInstance(ingestDocument));
        assertThat(innerLast.getInvokedCounter(), equalTo(0));
        assertThat(outerAfter.getInvokedCounter(), equalTo(1));
    }

    public void testTerminateRequestPendingWhenAPipelineIsCalledIsKeptForTheCaller() {
        IngestService ingestService = createIngestService();
        TestProcessor innerFirst = new TestProcessor(doc -> {});
        TestProcessor innerSecond = new TestProcessor(doc -> {});
        Pipeline inner = new Pipeline("inner", null, null, new CompoundProcessor(innerFirst, innerSecond));
        when(ingestService.getPipeline("inner")).thenReturn(inner);
        // a processor that runs other processors, like foreach, can have a request to terminate pending from an earlier step when it
        // calls a pipeline in a later one: the request belongs to the pipeline that runs it, not to the pipeline that it calls
        Processor requestsTerminationThenCallsPipeline = new AbstractProcessor(null, null) {
            @Override
            public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
                ingestDocument.terminatePipeline();
                callPipeline("inner", ingestService).execute(ingestDocument, handler);
            }

            @Override
            public IngestDocument execute(IngestDocument ingestDocument) {
                throw new UnsupportedOperationException();
            }

            @Override
            public String getType() {
                return "test-loop";
            }
        };
        TestProcessor outerAfter = new TestProcessor(doc -> {});
        Pipeline outer = new Pipeline("outer", null, null, new CompoundProcessor(requestsTerminationThenCallsPipeline, outerAfter));
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());

        IngestDocument[] result = new IngestDocument[1];
        Exception[] failure = new Exception[1];
        ingestDocument.executePipeline(outer, (r, e) -> {
            result[0] = r;
            failure[0] = e;
        });

        assertThat(failure[0], nullValue());
        // the pipeline completed, rather than the handler never having been called
        assertThat(result[0], sameInstance(ingestDocument));
        // the called pipeline is not cut short by a request that was made before it started
        assertThat(innerFirst.getInvokedCounter(), equalTo(1));
        assertThat(innerSecond.getInvokedCounter(), equalTo(1));
        // the request is still there when the calling pipeline gets back to it
        assertThat(outerAfter.getInvokedCounter(), equalTo(0));
        assertThat(ingestDocument.isPipelineTerminated(), equalTo(false));
    }

    private static TestProcessor terminator() {
        return new TestProcessor("terminator", "terminate", null, doc -> { doc.terminatePipeline(); });
    }

    private static PipelineProcessor callPipeline(String pipelineId, IngestService ingestService) {
        return new PipelineProcessor(null, null, new TestTemplateService.MockTemplateScript.Factory(pipelineId), ingestService);
    }

    static IngestService createIngestService() {
        IngestService ingestService = mock(IngestService.class);
        ScriptService scriptService = mock(ScriptService.class);
        when(ingestService.getScriptService()).thenReturn(scriptService);
        return ingestService;
    }
}
