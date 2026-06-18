/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.fielddata.ordinals;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.index.fielddata.IndexOrdinalsFieldData;
import org.opensearch.index.fielddata.plain.AbstractLeafOrdinalsFieldData;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GlobalOrdinalsBuilderTests extends OpenSearchTestCase {

    public void testBuildWithCancellationBetweenSegments() throws IOException {
        try (Directory dir = newDirectory()) {
            RandomIndexWriter w = new RandomIndexWriter(random(), dir);
            w.w.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);

            // Create 3 segments with distinct terms
            for (int seg = 0; seg < 3; seg++) {
                for (int i = 0; i < 10; i++) {
                    Document doc = new Document();
                    doc.add(new StringField("field", "seg" + seg + "_term" + i, Field.Store.NO));
                    w.addDocument(doc);
                }
                w.flush();
            }

            try (IndexReader reader = w.getReader()) {
                w.close();
                assertTrue("Need multiple segments for global ordinals", reader.leaves().size() > 1);

                IndexOrdinalsFieldData fieldData = mockFieldData("field", reader);

                // Build without cancellation — should succeed
                assertNotNull(
                    GlobalOrdinalsBuilder.build(
                        reader,
                        fieldData,
                        new NoneCircuitBreakerService(),
                        logger,
                        AbstractLeafOrdinalsFieldData.DEFAULT_SCRIPT_FUNCTION,
                        () -> {}
                    )
                );

                // Build with immediate cancellation — should throw between segments
                expectThrows(
                    TaskCancelledException.class,
                    () -> GlobalOrdinalsBuilder.build(
                        reader,
                        fieldData,
                        new NoneCircuitBreakerService(),
                        logger,
                        AbstractLeafOrdinalsFieldData.DEFAULT_SCRIPT_FUNCTION,
                        () -> {
                            throw new TaskCancelledException("cancelled");
                        }
                    )
                );
            }
        }
    }

    public void testBuildWithDelayedCancellation() throws IOException {
        try (Directory dir = newDirectory()) {
            RandomIndexWriter w = new RandomIndexWriter(random(), dir);
            w.w.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);

            for (int seg = 0; seg < 3; seg++) {
                Document doc = new Document();
                doc.add(new StringField("field", "term" + seg, Field.Store.NO));
                w.addDocument(doc);
                w.flush();
            }

            try (IndexReader reader = w.getReader()) {
                w.close();
                assertTrue(reader.leaves().size() > 1);

                IndexOrdinalsFieldData fieldData = mockFieldData("field", reader);

                // Cancel after first segment — should still throw
                AtomicBoolean cancelled = new AtomicBoolean(false);
                expectThrows(
                    TaskCancelledException.class,
                    () -> GlobalOrdinalsBuilder.build(
                        reader,
                        fieldData,
                        new NoneCircuitBreakerService(),
                        logger,
                        AbstractLeafOrdinalsFieldData.DEFAULT_SCRIPT_FUNCTION,
                        () -> {
                            if (cancelled.get()) {
                                throw new TaskCancelledException("cancelled after first segment");
                            }
                            cancelled.set(true); // arm cancellation after first check passes
                        }
                    )
                );
            }
        }
    }

    public void testOriginalBuildMethodStillWorks() throws IOException {
        try (Directory dir = newDirectory()) {
            RandomIndexWriter w = new RandomIndexWriter(random(), dir);
            w.w.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);

            for (int seg = 0; seg < 2; seg++) {
                Document doc = new Document();
                doc.add(new StringField("field", "term" + seg, Field.Store.NO));
                w.addDocument(doc);
                w.flush();
            }

            try (IndexReader reader = w.getReader()) {
                w.close();
                assertTrue(reader.leaves().size() > 1);

                IndexOrdinalsFieldData fieldData = mockFieldData("field", reader);

                // Original method (no Runnable param) should still work
                assertNotNull(
                    GlobalOrdinalsBuilder.build(
                        reader,
                        fieldData,
                        new NoneCircuitBreakerService(),
                        logger,
                        AbstractLeafOrdinalsFieldData.DEFAULT_SCRIPT_FUNCTION
                    )
                );
            }
        }
    }

    private static IndexOrdinalsFieldData mockFieldData(String fieldName, IndexReader reader) {
        IndexOrdinalsFieldData fieldData = mock(IndexOrdinalsFieldData.class);
        when(fieldData.getFieldName()).thenReturn(fieldName);
        when(fieldData.load(any(LeafReaderContext.class))).thenAnswer(invocation -> {
            LeafReaderContext ctx = invocation.getArgument(0);
            return new AbstractLeafOrdinalsFieldData(AbstractLeafOrdinalsFieldData.DEFAULT_SCRIPT_FUNCTION) {
                @Override
                public SortedSetDocValues getOrdinalsValues() {
                    try {
                        SortedSetDocValues dv = ctx.reader().getSortedSetDocValues(fieldName);
                        return dv != null ? dv : DocValues.emptySortedSet();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public long ramBytesUsed() {
                    return 0;
                }

                @Override
                public java.util.Collection<org.apache.lucene.util.Accountable> getChildResources() {
                    return Collections.emptyList();
                }

                @Override
                public void close() {}
            };
        });
        return fieldData;
    }
}
