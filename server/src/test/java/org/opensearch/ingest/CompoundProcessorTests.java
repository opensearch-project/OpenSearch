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
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

import static java.util.Collections.singletonList;
import static org.opensearch.ingest.IngestDocumentPreparer.SHOULD_FAIL_KEY;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CompoundProcessorTests extends OpenSearchTestCase {
    private IngestDocument ingestDocument;

    @Before
    public void init() {
        ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
    }

    public void testEmpty() throws Exception {
        CompoundProcessor processor = new CompoundProcessor();
        assertThat(processor.getProcessors().isEmpty(), is(true));
        assertThat(processor.getOnFailureProcessors().isEmpty(), is(true));
        processor.execute(ingestDocument, (result, e) -> {});
    }

    public void testSingleProcessor() throws Exception {
        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L, TimeUnit.MILLISECONDS.toNanos(1));
        TestProcessor processor = new TestProcessor(ingestDocument -> {
            assertStats(0, ingestDocument.getFieldValue("compoundProcessor", CompoundProcessor.class), 1, 0, 0, 0);
        });
        CompoundProcessor compoundProcessor = new CompoundProcessor(relativeTimeProvider, processor);
        ingestDocument.setFieldValue("compoundProcessor", compoundProcessor); // ugly hack to assert current count = 1
        assertThat(compoundProcessor.getProcessors().size(), equalTo(1));
        assertThat(compoundProcessor.getProcessors().get(0), sameInstance(processor));
        assertThat(compoundProcessor.getProcessors().get(0), sameInstance(processor));
        assertThat(compoundProcessor.getOnFailureProcessors().isEmpty(), is(true));
        compoundProcessor.execute(ingestDocument, (result, e) -> {});
        verify(relativeTimeProvider, times(2)).getAsLong();
        assertThat(processor.getInvokedCounter(), equalTo(1));
        assertStats(compoundProcessor, 1, 0, 1);

    }

    public void testSingleProcessorWithException() throws Exception {
        TestProcessor processor = new TestProcessor(new RuntimeException("error"));
        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L);
        CompoundProcessor compoundProcessor = new CompoundProcessor(relativeTimeProvider, processor);
        assertThat(compoundProcessor.getProcessors().size(), equalTo(1));
        assertThat(compoundProcessor.getProcessors().get(0), sameInstance(processor));
        assertThat(compoundProcessor.getOnFailureProcessors().isEmpty(), is(true));
        Exception[] holder = new Exception[1];
        compoundProcessor.execute(ingestDocument, (result, e) -> holder[0] = e);
        assertThat(((OpenSearchException) holder[0]).getRootCause().getMessage(), equalTo("error"));
        assertThat(processor.getInvokedCounter(), equalTo(1));
        assertStats(compoundProcessor, 1, 1, 0);

    }

    public void testIgnoreFailure() throws Exception {
        TestProcessor processor1 = new TestProcessor(new RuntimeException("error"));
        TestProcessor processor2 = new TestProcessor(ingestDocument -> { ingestDocument.setFieldValue("field", "value"); });
        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L);
        CompoundProcessor compoundProcessor = new CompoundProcessor(
            true,
            Arrays.asList(processor1, processor2),
            Collections.emptyList(),
            relativeTimeProvider
        );
        compoundProcessor.execute(ingestDocument, (result, e) -> {});
        assertThat(processor1.getInvokedCounter(), equalTo(1));
        assertStats(0, compoundProcessor, 0, 1, 1, 0);
        assertThat(processor2.getInvokedCounter(), equalTo(1));
        assertStats(1, compoundProcessor, 0, 1, 0, 0);
        assertThat(ingestDocument.getFieldValue("field", String.class), equalTo("value"));
    }

    public void testSingleProcessorWithOnFailureProcessor() throws Exception {
        TestProcessor processor1 = new TestProcessor("id", "first", null, new RuntimeException("error"));
        TestProcessor processor2 = new TestProcessor(ingestDocument -> {
            Map<String, Object> ingestMetadata = ingestDocument.getIngestMetadata();
            assertThat(ingestMetadata.size(), equalTo(3));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_MESSAGE_FIELD), equalTo("error"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TYPE_FIELD), equalTo("first"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TAG_FIELD), equalTo("id"));
        });

        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L, TimeUnit.MILLISECONDS.toNanos(1));
        CompoundProcessor compoundProcessor = new CompoundProcessor(
            false,
            singletonList(processor1),
            singletonList(processor2),
            relativeTimeProvider
        );
        compoundProcessor.execute(ingestDocument, (result, e) -> {});
        verify(relativeTimeProvider, times(2)).getAsLong();

        assertThat(processor1.getInvokedCounter(), equalTo(1));
        assertStats(compoundProcessor, 1, 1, 1);
        assertThat(processor2.getInvokedCounter(), equalTo(1));
    }

    public void testSingleProcessorWithOnFailureDropProcessor() throws Exception {
        TestProcessor processor1 = new TestProcessor("id", "first", null, new RuntimeException("error"));
        Processor processor2 = new Processor() {
            @Override
            public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
                // Simulates the drop processor
                return null;
            }

            @Override
            public String getType() {
                return "drop";
            }

            @Override
            public String getTag() {
                return null;
            }

            @Override
            public String getDescription() {
                return null;
            }
        };

        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L);
        CompoundProcessor compoundProcessor = new CompoundProcessor(
            false,
            singletonList(processor1),
            singletonList(processor2),
            relativeTimeProvider
        );
        IngestDocument[] result = new IngestDocument[1];
        compoundProcessor.execute(ingestDocument, (r, e) -> result[0] = r);
        assertThat(result[0], nullValue());
        assertThat(processor1.getInvokedCounter(), equalTo(1));
        assertStats(compoundProcessor, 1, 1, 0);
    }

    public void testSingleProcessorWithNestedFailures() throws Exception {
        TestProcessor processor = new TestProcessor("id", "first", null, new RuntimeException("error"));
        TestProcessor processorToFail = new TestProcessor("id2", "second", null, (Consumer<IngestDocument>) ingestDocument -> {
            Map<String, Object> ingestMetadata = ingestDocument.getIngestMetadata();
            assertThat(ingestMetadata.size(), equalTo(3));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_MESSAGE_FIELD), equalTo("error"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TYPE_FIELD), equalTo("first"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TAG_FIELD), equalTo("id"));
            throw new RuntimeException("error");
        });
        TestProcessor lastProcessor = new TestProcessor(ingestDocument -> {
            Map<String, Object> ingestMetadata = ingestDocument.getIngestMetadata();
            assertThat(ingestMetadata.size(), equalTo(3));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_MESSAGE_FIELD), equalTo("error"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TYPE_FIELD), equalTo("second"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TAG_FIELD), equalTo("id2"));
        });
        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L);
        CompoundProcessor compoundOnFailProcessor = new CompoundProcessor(
            false,
            singletonList(processorToFail),
            singletonList(lastProcessor),
            relativeTimeProvider
        );
        CompoundProcessor compoundProcessor = new CompoundProcessor(
            false,
            singletonList(processor),
            singletonList(compoundOnFailProcessor),
            relativeTimeProvider
        );
        compoundProcessor.execute(ingestDocument, (result, e) -> {});

        assertThat(processorToFail.getInvokedCounter(), equalTo(1));
        assertThat(lastProcessor.getInvokedCounter(), equalTo(1));
        assertStats(compoundProcessor, 1, 1, 0);
    }

    public void testCompoundProcessorExceptionFailWithoutOnFailure() throws Exception {
        TestProcessor firstProcessor = new TestProcessor("id1", "first", null, new RuntimeException("error"));
        TestProcessor secondProcessor = new TestProcessor("id3", "second", null, ingestDocument -> {
            Map<String, Object> ingestMetadata = ingestDocument.getIngestMetadata();
            assertThat(ingestMetadata.entrySet(), hasSize(3));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_MESSAGE_FIELD), equalTo("error"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TYPE_FIELD), equalTo("first"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TAG_FIELD), equalTo("id1"));
        });
        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L);

        CompoundProcessor failCompoundProcessor = new CompoundProcessor(relativeTimeProvider, firstProcessor);

        CompoundProcessor compoundProcessor = new CompoundProcessor(
            false,
            singletonList(failCompoundProcessor),
            singletonList(secondProcessor),
            relativeTimeProvider
        );
        compoundProcessor.execute(ingestDocument, (result, e) -> {});

        assertThat(firstProcessor.getInvokedCounter(), equalTo(1));
        assertThat(secondProcessor.getInvokedCounter(), equalTo(1));
        assertStats(compoundProcessor, 1, 1, 0);
    }

    public void testCompoundProcessorExceptionFail() throws Exception {
        TestProcessor firstProcessor = new TestProcessor("id1", "first", null, new RuntimeException("error"));
        TestProcessor failProcessor = new TestProcessor("tag_fail", "fail", null, new RuntimeException("custom error message"));
        TestProcessor secondProcessor = new TestProcessor("id3", "second", null, ingestDocument -> {
            Map<String, Object> ingestMetadata = ingestDocument.getIngestMetadata();
            assertThat(ingestMetadata.entrySet(), hasSize(3));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_MESSAGE_FIELD), equalTo("custom error message"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TYPE_FIELD), equalTo("fail"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TAG_FIELD), equalTo("tag_fail"));
        });

        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L);
        CompoundProcessor failCompoundProcessor = new CompoundProcessor(
            false,
            singletonList(firstProcessor),
            singletonList(failProcessor),
            relativeTimeProvider
        );

        CompoundProcessor compoundProcessor = new CompoundProcessor(
            false,
            singletonList(failCompoundProcessor),
            singletonList(secondProcessor),
            relativeTimeProvider
        );
        compoundProcessor.execute(ingestDocument, (result, e) -> {});

        assertThat(firstProcessor.getInvokedCounter(), equalTo(1));
        assertThat(secondProcessor.getInvokedCounter(), equalTo(1));
        assertStats(compoundProcessor, 1, 1, 0);
    }

    public void testCompoundProcessorExceptionFailInOnFailure() throws Exception {
        TestProcessor firstProcessor = new TestProcessor("id1", "first", null, new RuntimeException("error"));
        TestProcessor failProcessor = new TestProcessor("tag_fail", "fail", null, new RuntimeException("custom error message"));
        TestProcessor secondProcessor = new TestProcessor("id3", "second", null, ingestDocument -> {
            Map<String, Object> ingestMetadata = ingestDocument.getIngestMetadata();
            assertThat(ingestMetadata.entrySet(), hasSize(3));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_MESSAGE_FIELD), equalTo("custom error message"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TYPE_FIELD), equalTo("fail"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TAG_FIELD), equalTo("tag_fail"));
        });

        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L);
        CompoundProcessor failCompoundProcessor = new CompoundProcessor(
            false,
            singletonList(firstProcessor),
            singletonList(new CompoundProcessor(relativeTimeProvider, failProcessor))
        );

        CompoundProcessor compoundProcessor = new CompoundProcessor(
            false,
            singletonList(failCompoundProcessor),
            singletonList(secondProcessor),
            relativeTimeProvider
        );
        compoundProcessor.execute(ingestDocument, (result, e) -> {});

        assertThat(firstProcessor.getInvokedCounter(), equalTo(1));
        assertThat(secondProcessor.getInvokedCounter(), equalTo(1));
        assertStats(compoundProcessor, 1, 1, 0);
    }

    public void testBreakOnFailure() throws Exception {
        TestProcessor firstProcessor = new TestProcessor("id1", "first", null, new RuntimeException("error1"));
        TestProcessor secondProcessor = new TestProcessor("id2", "second", null, new RuntimeException("error2"));
        TestProcessor onFailureProcessor = new TestProcessor("id2", "on_failure", null, ingestDocument -> {});
        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        when(relativeTimeProvider.getAsLong()).thenReturn(0L);
        CompoundProcessor pipeline = new CompoundProcessor(
            false,
            Arrays.asList(firstProcessor, secondProcessor),
            singletonList(onFailureProcessor),
            relativeTimeProvider
        );
        pipeline.execute(ingestDocument, (result, e) -> {});
        assertThat(firstProcessor.getInvokedCounter(), equalTo(1));
        assertThat(secondProcessor.getInvokedCounter(), equalTo(0));
        assertThat(onFailureProcessor.getInvokedCounter(), equalTo(1));
        assertStats(pipeline, 1, 1, 0);
    }

    public void testFailureProcessorIsInvokedOnFailure() {
        TestProcessor onFailureProcessor = new TestProcessor(null, "on_failure", null, ingestDocument -> {
            Map<String, Object> ingestMetadata = ingestDocument.getIngestMetadata();
            assertThat(ingestMetadata.entrySet(), hasSize(5));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_MESSAGE_FIELD), equalTo("failure!"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TYPE_FIELD), equalTo("test-processor"));
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PROCESSOR_TAG_FIELD), nullValue());
            assertThat(ingestMetadata.get(CompoundProcessor.ON_FAILURE_PIPELINE_FIELD), equalTo("2"));
            assertThat(ingestMetadata.get("pipeline"), equalTo("1"));
        });

        Pipeline pipeline2 = new Pipeline("2", null, null, new CompoundProcessor(new TestProcessor(new RuntimeException("failure!"))));
        Pipeline pipeline1 = new Pipeline("1", null, null, new CompoundProcessor(false, singletonList(new AbstractProcessor(null, null) {
            @Override
            public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
                ingestDocument.executePipeline(pipeline2, handler);
            }

            @Override
            public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
                throw new AssertionError();
            }

            @Override
            public String getType() {
                return "pipeline";
            }
        }), singletonList(onFailureProcessor)));

        ingestDocument.executePipeline(pipeline1, (document, e) -> {
            assertThat(document, notNullValue());
            assertThat(e, nullValue());
        });
        assertThat(onFailureProcessor.getInvokedCounter(), equalTo(1));
    }

    public void testNewCompoundProcessorException() {
        TestProcessor processor = new TestProcessor("my_tag", "my_type", null, new RuntimeException());
        IngestProcessorException ingestProcessorException1 = CompoundProcessor.newCompoundProcessorException(
            new RuntimeException(),
            processor,
            ingestDocument
        );
        assertThat(ingestProcessorException1.getHeader("processor_tag"), equalTo(singletonList("my_tag")));
        assertThat(ingestProcessorException1.getHeader("processor_type"), equalTo(singletonList("my_type")));
        assertThat(ingestProcessorException1.getHeader("pipeline_origin"), nullValue());

        IngestProcessorException ingestProcessorException2 = CompoundProcessor.newCompoundProcessorException(
            ingestProcessorException1,
            processor,
            ingestDocument
        );
        assertThat(ingestProcessorException2, sameInstance(ingestProcessorException1));
    }

    public void testNewCompoundProcessorExceptionPipelineOrigin() {
        Pipeline pipeline2 = new Pipeline(
            "2",
            null,
            null,
            new CompoundProcessor(new TestProcessor("my_tag", "my_type", null, new RuntimeException()))
        );
        Pipeline pipeline1 = new Pipeline("1", null, null, new CompoundProcessor(new AbstractProcessor(null, null) {
            @Override
            public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
                throw new UnsupportedOperationException();
            }

            @Override
            public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
                ingestDocument.executePipeline(pipeline2, handler);
            }

            @Override
            public String getType() {
                return "my_type2";
            }
        }));

        Exception[] holder = new Exception[1];
        ingestDocument.executePipeline(pipeline1, (document, e) -> holder[0] = e);
        IngestProcessorException ingestProcessorException = (IngestProcessorException) holder[0];
        assertThat(ingestProcessorException.getHeader("processor_tag"), equalTo(singletonList("my_tag")));
        assertThat(ingestProcessorException.getHeader("processor_type"), equalTo(singletonList("my_type")));
        assertThat(ingestProcessorException.getHeader("pipeline_origin"), equalTo(Arrays.asList("2", "1")));
    }

    public void testBatchExecute_happyCase() {
        List<IngestDocumentWrapper> wrapperList = Arrays.asList(
            IngestDocumentPreparer.createIngestDocumentWrapper(1),
            IngestDocumentPreparer.createIngestDocumentWrapper(2),
            IngestDocumentPreparer.createIngestDocumentWrapper(3)
        );
        TestProcessor firstProcessor = new TestProcessor(doc -> {});
        TestProcessor secondProcessor = new TestProcessor(doc -> {});
        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        CompoundProcessor compoundProcessor = new CompoundProcessor(
            false,
            Arrays.asList(firstProcessor, secondProcessor),
            null,
            relativeTimeProvider
        );

        compoundProcessor.batchExecute(wrapperList, results -> {
            assertEquals(firstProcessor.getInvokedCounter(), wrapperList.size());
            assertEquals(secondProcessor.getInvokedCounter(), wrapperList.size());
            assertEquals(results.size(), wrapperList.size());
            OperationStats stats = compoundProcessor.getProcessorsWithMetrics().get(0).v2().createStats();
            assertEquals(0, stats.getCurrent());
            assertEquals(3, stats.getCount());
            for (int i = 0; i < wrapperList.size(); ++i) {
                assertEquals(wrapperList.get(i).getSlot(), results.get(i).getSlot());
                assertEquals(wrapperList.get(i).getIngestDocument(), results.get(i).getIngestDocument());
                assertEquals(wrapperList.get(i).getException(), results.get(i).getException());
            }
        });
    }

    public void testBatchExecute_documentToDrop() {
        List<IngestDocumentWrapper> wrapperList = Arrays.asList(
            IngestDocumentPreparer.createIngestDocumentWrapper(1),
            IngestDocumentPreparer.createIngestDocumentWrapper(2, true),
            IngestDocumentPreparer.createIngestDocumentWrapper(3)
        );
        TestProcessor firstProcessor = new TestProcessor("", "", "", doc -> {
            if (doc.hasField(SHOULD_FAIL_KEY) && doc.getFieldValue(SHOULD_FAIL_KEY, Boolean.class)) {
                return null;
            }
            return doc;
        });
        TestProcessor secondProcessor = new TestProcessor(doc -> {});
        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        CompoundProcessor compoundProcessor = new CompoundProcessor(
            false,
            Arrays.asList(firstProcessor, secondProcessor),
            null,
            relativeTimeProvider
        );

        AtomicInteger callCounter = new AtomicInteger();
        List<IngestDocumentWrapper> totalResults = Collections.synchronizedList(new ArrayList<>());
        compoundProcessor.batchExecute(wrapperList, results -> {
            totalResults.addAll(results);
            if (callCounter.addAndGet(results.size()) == 3) {
                assertEquals(firstProcessor.getInvokedCounter(), wrapperList.size());
                assertEquals(secondProcessor.getInvokedCounter(), wrapperList.size() - 1);
                assertEquals(totalResults.size(), wrapperList.size());
                OperationStats stats = compoundProcessor.getProcessorsWithMetrics().get(0).v2().createStats();
                assertEquals(0, stats.getCurrent());
                assertEquals(3, stats.getCount());
                totalResults.sort(Comparator.comparingInt(IngestDocumentWrapper::getSlot));
                for (int i = 0; i < wrapperList.size(); ++i) {
                    assertEquals(wrapperList.get(i).getSlot(), totalResults.get(i).getSlot());
                    if (2 == wrapperList.get(i).getSlot()) {
                        assertNull(totalResults.get(i).getIngestDocument());
                    } else {
                        assertEquals(wrapperList.get(i).getIngestDocument(), totalResults.get(i).getIngestDocument());
                    }
                    assertEquals(wrapperList.get(i).getException(), totalResults.get(i).getException());
                }
            }
        });
    }

    public void testBatchExecute_ignoreFailure() {
        List<IngestDocumentWrapper> wrapperList = Arrays.asList(
            IngestDocumentPreparer.createIngestDocumentWrapper(1),
            IngestDocumentPreparer.createIngestDocumentWrapper(2, true),
            IngestDocumentPreparer.createIngestDocumentWrapper(3, true)
        );
        TestProcessor firstProcessor = new TestProcessor(doc -> {
            if (doc.hasField(SHOULD_FAIL_KEY) && doc.getFieldValue(SHOULD_FAIL_KEY, Boolean.class)) {
                throw new RuntimeException("fail");
            }
        });
        TestProcessor secondProcessor = new TestProcessor(doc -> {});
        TestProcessor onFailureProcessor = new TestProcessor("id2", "on_failure", null, doc -> {});
        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        CompoundProcessor compoundProcessor = new CompoundProcessor(
            true,
            Arrays.asList(firstProcessor, secondProcessor),
            singletonList(onFailureProcessor),
            relativeTimeProvider
        );

        compoundProcessor.batchExecute(wrapperList, results -> {
            assertEquals(firstProcessor.getInvokedCounter(), wrapperList.size());
            assertEquals(secondProcessor.getInvokedCounter(), wrapperList.size());
            assertEquals(0, onFailureProcessor.getInvokedCounter());
            assertEquals(results.size(), wrapperList.size());
            OperationStats stats = compoundProcessor.getProcessorsWithMetrics().get(0).v2().createStats();
            assertEquals(0, stats.getCurrent());
            assertEquals(3, stats.getCount());
            for (int i = 0; i < wrapperList.size(); ++i) {
                assertEquals(wrapperList.get(i).getSlot(), results.get(i).getSlot());
                assertEquals(wrapperList.get(i).getIngestDocument(), results.get(i).getIngestDocument());
                assertEquals(wrapperList.get(i).getException(), results.get(i).getException());
            }
        });
    }

    public void testBatchExecute_exception_no_onFailureProcessor() {
        Set<Integer> failureSlot = new HashSet<>(Arrays.asList(2, 3));
        List<IngestDocumentWrapper> wrapperList = Arrays.asList(
            IngestDocumentPreparer.createIngestDocumentWrapper(1),
            IngestDocumentPreparer.createIngestDocumentWrapper(2, true),
            IngestDocumentPreparer.createIngestDocumentWrapper(3, true)
        );
        TestProcessor firstProcessor = new TestProcessor(doc -> {
            if (doc.hasField(SHOULD_FAIL_KEY) && doc.getFieldValue(SHOULD_FAIL_KEY, Boolean.class)) {
                throw new RuntimeException("fail");
            }
        });
        TestProcessor secondProcessor = new TestProcessor(doc -> {});
        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        CompoundProcessor compoundProcessor = new CompoundProcessor(
            false,
            Arrays.asList(firstProcessor, secondProcessor),
            Collections.emptyList(),
            relativeTimeProvider
        );

        AtomicInteger callCounter = new AtomicInteger();
        List<IngestDocumentWrapper> totalResults = Collections.synchronizedList(new ArrayList<>());
        compoundProcessor.batchExecute(wrapperList, results -> {
            totalResults.addAll(results);
            if (callCounter.incrementAndGet() == 3) {
                assertEquals(wrapperList.size(), firstProcessor.getInvokedCounter());
                assertEquals(1, secondProcessor.getInvokedCounter());
                assertEquals(totalResults.size(), wrapperList.size());
                OperationStats stats = compoundProcessor.getProcessorsWithMetrics().get(0).v2().createStats();
                assertEquals(0, stats.getCurrent());
                assertEquals(3, stats.getCount());
                assertEquals(2, stats.getFailedCount());
                totalResults.sort(Comparator.comparingInt(IngestDocumentWrapper::getSlot));
                for (int i = 0; i < wrapperList.size(); ++i) {
                    assertEquals(wrapperList.get(i).getSlot(), totalResults.get(i).getSlot());
                    if (failureSlot.contains(wrapperList.get(i).getSlot())) {
                        assertNotNull(totalResults.get(i).getException());
                    } else {
                        assertEquals(wrapperList.get(i).getIngestDocument(), totalResults.get(i).getIngestDocument());
                        assertEquals(wrapperList.get(i).getException(), totalResults.get(i).getException());
                    }
                }
            }
        });
    }

    public void testBatchExecute_exception_with_onFailureProcessor() {
        List<IngestDocumentWrapper> wrapperList = Arrays.asList(
            IngestDocumentPreparer.createIngestDocumentWrapper(1),
            IngestDocumentPreparer.createIngestDocumentWrapper(2, true),
            IngestDocumentPreparer.createIngestDocumentWrapper(3, true)
        );
        TestProcessor firstProcessor = new TestProcessor(doc -> {
            if (doc.hasField(SHOULD_FAIL_KEY) && doc.getFieldValue(SHOULD_FAIL_KEY, Boolean.class)) {
                throw new RuntimeException("fail");
            }
        });
        TestProcessor secondProcessor = new TestProcessor(doc -> {});
        TestProcessor onFailureProcessor = new TestProcessor("id2", "on_failure", null, doc -> {});
        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        CompoundProcessor compoundProcessor = new CompoundProcessor(
            false,
            Arrays.asList(firstProcessor, secondProcessor),
            singletonList(onFailureProcessor),
            relativeTimeProvider
        );

        AtomicInteger callCounter = new AtomicInteger();
        List<IngestDocumentWrapper> totalResults = Collections.synchronizedList(new ArrayList<>());
        compoundProcessor.batchExecute(wrapperList, results -> {
            totalResults.addAll(results);
            if (callCounter.incrementAndGet() == 3) {
                assertEquals(wrapperList.size(), firstProcessor.getInvokedCounter());
                assertEquals(1, secondProcessor.getInvokedCounter());
                assertEquals(2, onFailureProcessor.getInvokedCounter());
                assertEquals(totalResults.size(), wrapperList.size());
                OperationStats stats = compoundProcessor.getProcessorsWithMetrics().get(0).v2().createStats();
                assertEquals(0, stats.getCurrent());
                assertEquals(3, stats.getCount());
                assertEquals(2, stats.getFailedCount());
                totalResults.sort(Comparator.comparingInt(IngestDocumentWrapper::getSlot));
                for (int i = 0; i < wrapperList.size(); ++i) {
                    assertEquals(wrapperList.get(i).getSlot(), totalResults.get(i).getSlot());
                    assertEquals(wrapperList.get(i).getIngestDocument(), totalResults.get(i).getIngestDocument());
                    assertNull(totalResults.get(i).getException());
                }
            }
        });
    }

    public void testTerminateSkipsRemainingProcessors() {
        TestProcessor first = new TestProcessor(doc -> {});
        TestProcessor terminator = terminator();
        TestProcessor last = new TestProcessor(doc -> {});
        CompoundProcessor compoundProcessor = new CompoundProcessor(ZERO_CLOCK, first, terminator, last);

        IngestDocument[] result = new IngestDocument[1];
        Exception[] failure = new Exception[1];
        compoundProcessor.execute(ingestDocument, (r, e) -> {
            result[0] = r;
            failure[0] = e;
        });

        assertThat(failure[0], nullValue());
        assertThat(result[0], sameInstance(ingestDocument));
        assertThat(first.getInvokedCounter(), equalTo(1));
        assertThat(terminator.getInvokedCounter(), equalTo(1));
        assertThat(last.getInvokedCounter(), equalTo(0));
        // the terminating processor is a success, and the skipped processor was never started
        assertStats(0, compoundProcessor, 0, 1, 0, 0);
        assertStats(1, compoundProcessor, 0, 1, 0, 0);
        assertStats(2, compoundProcessor, 0, 0, 0, 0);
        assertThat(ingestDocument.getIngestMetadata().isEmpty(), is(true));
    }

    public void testTerminateAsLastProcessor() {
        TestProcessor first = new TestProcessor(doc -> {});
        TestProcessor terminator = terminator();
        CompoundProcessor compoundProcessor = new CompoundProcessor(ZERO_CLOCK, first, terminator);

        IngestDocument[] result = new IngestDocument[1];
        compoundProcessor.execute(ingestDocument, (r, e) -> result[0] = r);

        assertThat(result[0], sameInstance(ingestDocument));
        assertThat(first.getInvokedCounter(), equalTo(1));
        assertThat(terminator.getInvokedCounter(), equalTo(1));
    }

    public void testTerminateInsideIgnoreFailureCompoundEndsEnclosingCompound() {
        TestProcessor first = new TestProcessor(doc -> {});
        TestProcessor terminator = terminator();
        TestProcessor last = new TestProcessor(doc -> {});
        // processors that are configured with ignore_failure or on_failure are wrapped in a compound processor of their own
        CompoundProcessor wrapped = new CompoundProcessor(true, singletonList(terminator), Collections.emptyList(), ZERO_CLOCK);
        CompoundProcessor outer = new CompoundProcessor(false, Arrays.asList(first, wrapped, last), Collections.emptyList(), ZERO_CLOCK);

        IngestDocument[] result = new IngestDocument[1];
        Exception[] failure = new Exception[1];
        outer.execute(ingestDocument, (r, e) -> {
            result[0] = r;
            failure[0] = e;
        });

        assertThat(failure[0], nullValue());
        assertThat(result[0], sameInstance(ingestDocument));
        assertThat(first.getInvokedCounter(), equalTo(1));
        assertThat(terminator.getInvokedCounter(), equalTo(1));
        assertThat(last.getInvokedCounter(), equalTo(0));
        assertStats(1, outer, 0, 1, 0, 0);
        assertStats(2, outer, 0, 0, 0, 0);
    }

    public void testTerminateInsideProcessorOnFailureEndsEnclosingCompound() {
        TestProcessor first = new TestProcessor(doc -> {});
        TestProcessor failing = new TestProcessor("failing-tag", "failing-type", null, new RuntimeException("error"));
        TestProcessor onFailureFirst = new TestProcessor(doc -> {
            assertThat(doc.getIngestMetadata().get(CompoundProcessor.ON_FAILURE_MESSAGE_FIELD), equalTo("error"));
        });
        TestProcessor terminator = terminator();
        TestProcessor onFailureLast = new TestProcessor(doc -> {});
        TestProcessor last = new TestProcessor(doc -> {});
        CompoundProcessor wrapped = new CompoundProcessor(
            false,
            singletonList(failing),
            Arrays.asList(onFailureFirst, terminator, onFailureLast),
            ZERO_CLOCK
        );
        CompoundProcessor outer = new CompoundProcessor(false, Arrays.asList(first, wrapped, last), Collections.emptyList(), ZERO_CLOCK);

        IngestDocument[] result = new IngestDocument[1];
        Exception[] failure = new Exception[1];
        outer.execute(ingestDocument, (r, e) -> {
            result[0] = r;
            failure[0] = e;
        });

        assertThat(failure[0], nullValue());
        assertThat(result[0], sameInstance(ingestDocument));
        assertThat(failing.getInvokedCounter(), equalTo(1));
        assertThat(onFailureFirst.getInvokedCounter(), equalTo(1));
        assertThat(terminator.getInvokedCounter(), equalTo(1));
        // the remaining on_failure processors and the remaining pipeline processors are skipped
        assertThat(onFailureLast.getInvokedCounter(), equalTo(0));
        assertThat(last.getInvokedCounter(), equalTo(0));
        // the failure of the wrapped processor is still counted as a failure
        assertStats(wrapped, 1, 1, 0);
        // the failure metadata is removed, as it is whenever on_failure processors complete
        assertThat(ingestDocument.getIngestMetadata().isEmpty(), is(true));
    }

    public void testTerminateInPipelineLevelOnFailure() {
        TestProcessor failing = new TestProcessor(new RuntimeException("error"));
        TestProcessor skipped = new TestProcessor(doc -> {});
        TestProcessor onFailureFirst = new TestProcessor(doc -> {});
        TestProcessor terminator = terminator();
        TestProcessor onFailureLast = new TestProcessor(doc -> {});
        CompoundProcessor compoundProcessor = new CompoundProcessor(
            false,
            Arrays.asList(failing, skipped),
            Arrays.asList(onFailureFirst, terminator, onFailureLast),
            ZERO_CLOCK
        );

        IngestDocument[] result = new IngestDocument[1];
        Exception[] failure = new Exception[1];
        compoundProcessor.execute(ingestDocument, (r, e) -> {
            result[0] = r;
            failure[0] = e;
        });

        assertThat(failure[0], nullValue());
        assertThat(result[0], sameInstance(ingestDocument));
        assertThat(skipped.getInvokedCounter(), equalTo(0));
        assertThat(onFailureFirst.getInvokedCounter(), equalTo(1));
        assertThat(terminator.getInvokedCounter(), equalTo(1));
        assertThat(onFailureLast.getInvokedCounter(), equalTo(0));
        assertThat(ingestDocument.getIngestMetadata().isEmpty(), is(true));
    }

    public void testTerminateBeforeDropKeepsTheDocument() {
        TestProcessor terminator = terminator();
        TestProcessor dropper = new TestProcessor("", "drop", null, doc -> null);
        CompoundProcessor compoundProcessor = new CompoundProcessor(ZERO_CLOCK, terminator, dropper);

        IngestDocument[] result = new IngestDocument[1];
        compoundProcessor.execute(ingestDocument, (r, e) -> result[0] = r);

        assertThat(result[0], sameInstance(ingestDocument));
        assertThat(dropper.getInvokedCounter(), equalTo(0));
    }

    public void testDropBeforeTerminateDropsTheDocument() {
        TestProcessor dropper = new TestProcessor("", "drop", null, doc -> null);
        TestProcessor terminator = terminator();
        CompoundProcessor compoundProcessor = new CompoundProcessor(ZERO_CLOCK, dropper, terminator);

        IngestDocument[] result = new IngestDocument[] { ingestDocument };
        compoundProcessor.execute(ingestDocument, (r, e) -> result[0] = r);

        assertThat(result[0], nullValue());
        assertThat(terminator.getInvokedCounter(), equalTo(0));
    }

    public void testBatchExecute_documentToTerminate() {
        List<IngestDocumentWrapper> wrapperList = Arrays.asList(
            IngestDocumentPreparer.createIngestDocumentWrapper(1),
            IngestDocumentPreparer.createIngestDocumentWrapper(2, true),
            IngestDocumentPreparer.createIngestDocumentWrapper(3)
        );
        TestProcessor first = terminatorForFlaggedDocuments();
        TestProcessor second = new TestProcessor(doc -> {});
        CompoundProcessor compoundProcessor = new CompoundProcessor(
            false,
            Arrays.asList(first, second),
            Collections.emptyList(),
            ZERO_CLOCK
        );

        List<IngestDocumentWrapper> totalResults = Collections.synchronizedList(new ArrayList<>());
        compoundProcessor.batchExecute(wrapperList, results -> totalResults.addAll(results));

        assertThat(totalResults, hasSize(3));
        assertThat(first.getInvokedCounter(), equalTo(3));
        // only the documents that were not terminated reach the second processor
        assertThat(second.getInvokedCounter(), equalTo(2));
        totalResults.sort(Comparator.comparingInt(IngestDocumentWrapper::getSlot));
        for (int i = 0; i < wrapperList.size(); ++i) {
            assertThat(totalResults.get(i).getSlot(), equalTo(wrapperList.get(i).getSlot()));
            assertThat(totalResults.get(i).getIngestDocument(), sameInstance(wrapperList.get(i).getIngestDocument()));
            assertThat(totalResults.get(i).getException(), nullValue());
        }
        assertStats(0, compoundProcessor, 0, 3, 0, 0);
        assertStats(1, compoundProcessor, 0, 2, 0, 0);
    }

    public void testBatchExecute_allDocumentsTerminated() {
        List<IngestDocumentWrapper> wrapperList = Arrays.asList(
            IngestDocumentPreparer.createIngestDocumentWrapper(1, true),
            IngestDocumentPreparer.createIngestDocumentWrapper(2, true)
        );
        TestProcessor first = terminatorForFlaggedDocuments();
        TestProcessor second = new TestProcessor(doc -> {});
        CompoundProcessor compoundProcessor = new CompoundProcessor(
            false,
            Arrays.asList(first, second),
            Collections.emptyList(),
            ZERO_CLOCK
        );

        List<IngestDocumentWrapper> totalResults = Collections.synchronizedList(new ArrayList<>());
        compoundProcessor.batchExecute(wrapperList, results -> totalResults.addAll(results));

        assertThat(totalResults, hasSize(2));
        assertThat(second.getInvokedCounter(), equalTo(0));
        assertStats(1, compoundProcessor, 0, 0, 0, 0);
    }

    public void testBatchExecute_terminateInsideIgnoreFailureCompound() {
        List<IngestDocumentWrapper> wrapperList = Arrays.asList(
            IngestDocumentPreparer.createIngestDocumentWrapper(1),
            IngestDocumentPreparer.createIngestDocumentWrapper(2, true),
            IngestDocumentPreparer.createIngestDocumentWrapper(3)
        );
        TestProcessor flaggedTerminator = terminatorForFlaggedDocuments();
        TestProcessor last = new TestProcessor(doc -> {});
        CompoundProcessor wrapped = new CompoundProcessor(true, singletonList(flaggedTerminator), Collections.emptyList(), ZERO_CLOCK);
        CompoundProcessor outer = new CompoundProcessor(false, Arrays.asList(wrapped, last), Collections.emptyList(), ZERO_CLOCK);

        List<IngestDocumentWrapper> totalResults = Collections.synchronizedList(new ArrayList<>());
        outer.batchExecute(wrapperList, results -> totalResults.addAll(results));

        assertThat(totalResults, hasSize(3));
        assertThat(last.getInvokedCounter(), equalTo(2));
        for (IngestDocumentWrapper result : totalResults) {
            assertThat(result.getIngestDocument(), notNullValue());
            assertThat(result.getException(), nullValue());
        }
    }

    public void testBatchExecute_terminateInsideProcessorOnFailureEndsEnclosingCompound() {
        List<IngestDocumentWrapper> wrapperList = Arrays.asList(
            IngestDocumentPreparer.createIngestDocumentWrapper(1),
            IngestDocumentPreparer.createIngestDocumentWrapper(2, true),
            IngestDocumentPreparer.createIngestDocumentWrapper(3, true)
        );
        TestProcessor failing = new TestProcessor(doc -> {
            if (doc.hasField(SHOULD_FAIL_KEY) && doc.getFieldValue(SHOULD_FAIL_KEY, Boolean.class)) {
                throw new RuntimeException("fail");
            }
        });
        TestProcessor onFailureFirst = new TestProcessor("id", "on_failure", null, doc -> {});
        TestProcessor terminator = terminator();
        TestProcessor onFailureLast = new TestProcessor(doc -> {});
        TestProcessor last = new TestProcessor(doc -> {});
        CompoundProcessor wrapped = new CompoundProcessor(
            false,
            singletonList(failing),
            Arrays.asList(onFailureFirst, terminator, onFailureLast),
            ZERO_CLOCK
        );
        CompoundProcessor outer = new CompoundProcessor(false, Arrays.asList(wrapped, last), Collections.emptyList(), ZERO_CLOCK);

        List<IngestDocumentWrapper> totalResults = Collections.synchronizedList(new ArrayList<>());
        outer.batchExecute(wrapperList, results -> totalResults.addAll(results));

        assertThat(totalResults, hasSize(3));
        assertThat(failing.getInvokedCounter(), equalTo(3));
        assertThat(onFailureFirst.getInvokedCounter(), equalTo(2));
        assertThat(terminator.getInvokedCounter(), equalTo(2));
        assertThat(onFailureLast.getInvokedCounter(), equalTo(0));
        // only the document that did not fail continues past the wrapped processor
        assertThat(last.getInvokedCounter(), equalTo(1));
        for (IngestDocumentWrapper result : totalResults) {
            assertThat(result.getIngestDocument(), notNullValue());
            assertThat(result.getException(), nullValue());
            assertThat(result.getIngestDocument().getIngestMetadata().isEmpty(), is(true));
        }
    }

    public void testBatchExecute_terminateInPipelineLevelOnFailure() {
        List<IngestDocumentWrapper> wrapperList = Arrays.asList(
            IngestDocumentPreparer.createIngestDocumentWrapper(1),
            IngestDocumentPreparer.createIngestDocumentWrapper(2, true),
            IngestDocumentPreparer.createIngestDocumentWrapper(3, true)
        );
        TestProcessor failing = new TestProcessor(doc -> {
            if (doc.hasField(SHOULD_FAIL_KEY) && doc.getFieldValue(SHOULD_FAIL_KEY, Boolean.class)) {
                throw new RuntimeException("fail");
            }
        });
        TestProcessor second = new TestProcessor(doc -> {});
        TestProcessor onFailureFirst = new TestProcessor(doc -> {});
        TestProcessor terminator = terminator();
        TestProcessor onFailureLast = new TestProcessor(doc -> {});
        CompoundProcessor compoundProcessor = new CompoundProcessor(
            false,
            Arrays.asList(failing, second),
            Arrays.asList(onFailureFirst, terminator, onFailureLast),
            ZERO_CLOCK
        );

        List<IngestDocumentWrapper> totalResults = Collections.synchronizedList(new ArrayList<>());
        compoundProcessor.batchExecute(wrapperList, results -> totalResults.addAll(results));

        assertThat(totalResults, hasSize(3));
        assertThat(failing.getInvokedCounter(), equalTo(3));
        assertThat(second.getInvokedCounter(), equalTo(1));
        assertThat(onFailureFirst.getInvokedCounter(), equalTo(2));
        assertThat(terminator.getInvokedCounter(), equalTo(2));
        assertThat(onFailureLast.getInvokedCounter(), equalTo(0));
        for (IngestDocumentWrapper result : totalResults) {
            assertThat(result.getIngestDocument(), notNullValue());
            assertThat(result.getException(), nullValue());
            assertThat(result.getIngestDocument().getIngestMetadata().isEmpty(), is(true));
        }
    }

    public void testIgnoreFailureHonoursTerminationRequestOfProcessorThatFailed() {
        // a processor that asks for termination and then fails: with ignore_failure the request is not lost
        TestProcessor terminateThenFail = new TestProcessor(doc -> {
            doc.terminatePipeline();
            throw new RuntimeException("boom");
        });
        TestProcessor last = new TestProcessor(doc -> {});
        CompoundProcessor compoundProcessor = new CompoundProcessor(
            true,
            Arrays.asList(terminateThenFail, last),
            Collections.emptyList(),
            ZERO_CLOCK
        );

        IngestDocument[] result = new IngestDocument[1];
        Exception[] failure = new Exception[1];
        compoundProcessor.execute(ingestDocument, (r, e) -> {
            result[0] = r;
            failure[0] = e;
        });

        assertThat(failure[0], nullValue());
        assertThat(result[0], sameInstance(ingestDocument));
        assertThat(last.getInvokedCounter(), equalTo(0));
        assertStats(0, compoundProcessor, 0, 1, 1, 0);
    }

    public void testBatchExecute_ignoreFailureHonoursTerminationRequestOfProcessorThatFailed() {
        List<IngestDocumentWrapper> wrapperList = Arrays.asList(
            IngestDocumentPreparer.createIngestDocumentWrapper(1),
            IngestDocumentPreparer.createIngestDocumentWrapper(2, true),
            IngestDocumentPreparer.createIngestDocumentWrapper(3)
        );
        TestProcessor terminateThenFail = new TestProcessor(doc -> {
            if (doc.hasField(SHOULD_FAIL_KEY) && doc.getFieldValue(SHOULD_FAIL_KEY, Boolean.class)) {
                doc.terminatePipeline();
                throw new RuntimeException("boom");
            }
        });
        TestProcessor last = new TestProcessor(doc -> {});
        CompoundProcessor compoundProcessor = new CompoundProcessor(
            true,
            Arrays.asList(terminateThenFail, last),
            Collections.emptyList(),
            ZERO_CLOCK
        );

        List<IngestDocumentWrapper> totalResults = Collections.synchronizedList(new ArrayList<>());
        compoundProcessor.batchExecute(wrapperList, results -> totalResults.addAll(results));

        assertThat(totalResults, hasSize(3));
        assertThat(last.getInvokedCounter(), equalTo(2));
        for (IngestDocumentWrapper result : totalResults) {
            assertThat(result.getIngestDocument(), notNullValue());
            assertThat(result.getException(), nullValue());
        }
        assertStats(0, compoundProcessor, 0, 3, 1, 0);
    }

    private static final LongSupplier ZERO_CLOCK = () -> 0L;

    /** A processor that behaves like the {@code terminate} processor. */
    private static TestProcessor terminator() {
        return new TestProcessor("terminator", "terminate", null, doc -> { doc.terminatePipeline(); });
    }

    /** Terminates only documents that carry {@link IngestDocumentPreparer#SHOULD_FAIL_KEY}, like an {@code if} condition would. */
    private static TestProcessor terminatorForFlaggedDocuments() {
        return new TestProcessor("terminator", "terminate", null, doc -> {
            if (doc.hasField(SHOULD_FAIL_KEY) && doc.getFieldValue(SHOULD_FAIL_KEY, Boolean.class)) {
                doc.terminatePipeline();
            }
        });
    }

    private void assertStats(CompoundProcessor compoundProcessor, long count, long failed, long time) {
        assertStats(0, compoundProcessor, 0L, count, failed, time);
    }

    private void assertStats(int processor, CompoundProcessor compoundProcessor, long current, long count, long failed, long time) {
        OperationStats stats = compoundProcessor.getProcessorsWithMetrics().get(processor).v2().createStats();
        assertThat(stats.getCount(), equalTo(count));
        assertThat(stats.getCurrent(), equalTo(current));
        assertThat(stats.getFailedCount(), equalTo(failed));
        assertThat(stats.getTotalTime(), equalTo(time));
    }
}
