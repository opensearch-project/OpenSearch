/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.index;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.opensearch.index.engine.dataformat.FlushInput;

import java.io.IOException;

/**
 * A {@link FilterCodec} wrapper that injects the {@code writer_generation} attribute into
 * every segment written by a {@link LuceneWriter}.
 * <p>
 * During {@link LuceneWriter#flush(FlushInput)}, each per-generation writer creates exactly one segment.
 * This codec intercepts the {@link SegmentInfoFormat#write} call to stamp the segment with
 * the writer generation number. After the segment is incorporated into the shared
 * {@link org.apache.lucene.index.IndexWriter} via {@code addIndexes}, the attribute allows
 * {@link LuceneIndexingExecutionEngine} to correlate the Lucene segment back to the
 * originating writer generation and its corresponding Parquet file.
 *
 * @opensearch.experimental
 */
public class LuceneWriterCodec extends FilterCodec {

    private static final String ROW_ID = LuceneDocumentInput.ROW_ID_FIELD;

    private final long writerGeneration;
    private volatile boolean rewriteRowIds = false;

    /**
     * Creates a new codec that wraps the given delegate and tags segments with the specified
     * writer generation.
     *
     * @param delegate          the underlying codec to delegate all format operations to
     * @param writerGeneration  the generation number to store as a segment info attribute
     */
    public LuceneWriterCodec(Codec delegate, long writerGeneration) {
        super(delegate.getName(), delegate);
        this.writerGeneration = writerGeneration;
    }

    /**
     * Enables sequential __row_id__ rewriting during the next merge.
     * When enabled, the DocValuesFormat intercepts writes to __row_id__
     * and replaces the values with sequential 0..N.
     */
    public void enableRowIdRewrite() {
        this.rewriteRowIds = true;
    }

    /**
     * Returns a {@link SegmentInfoFormat} that delegates reads to the underlying codec and
     * intercepts writes to inject the {@code writer_generation} attribute into the
     * {@link SegmentInfo} before persisting.
     *
     * @return the decorated segment info format
     */
    @Override
    public SegmentInfoFormat segmentInfoFormat() {
        return new SegmentInfoFormat() {
            @Override
            public SegmentInfo read(Directory directory, String segmentName, byte[] segmentID, IOContext context) throws IOException {
                return delegate.segmentInfoFormat().read(directory, segmentName, segmentID, context);
            }

            @Override
            public void write(Directory directory, SegmentInfo info, IOContext ioContext) throws IOException {
                info.putAttribute("writer_generation", String.valueOf(writerGeneration));
                delegate.segmentInfoFormat().write(directory, info, ioContext);
            }
        };
    }

    /**
     * Returns a {@link DocValuesFormat} that intercepts writes to {@code __row_id__}
     * and replaces the values with sequential 0..N when {@link #enableRowIdRewrite()}
     * has been called. This allows the reorder merge and the row ID rewrite to happen
     * in a single pass.
     */
    @Override
    public DocValuesFormat docValuesFormat() {
        if (rewriteRowIds == false) {
            return delegate.docValuesFormat();
        }
        DocValuesFormat delegateFormat = delegate.docValuesFormat();
        return new DocValuesFormat(delegateFormat.getName()) {
            @Override
            public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
                DocValuesConsumer delegateConsumer = delegateFormat.fieldsConsumer(state);
                return new DocValuesConsumer() {
                    private int nextDocId = 0;

                    @Override
                    public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
                        if (ROW_ID.equals(field.name)) {
                            // Replace with sequential 0..N values
                            delegateConsumer.addSortedNumericField(field, new DocValuesProducer() {
                                @Override
                                public SortedNumericDocValues getSortedNumeric(FieldInfo fi) {
                                    return new SortedNumericDocValues() {
                                        private int docID = -1;
                                        private final int maxDoc = state.segmentInfo.maxDoc();
                                        @Override public long nextValue() { return docID; }
                                        @Override public int docValueCount() { return 1; }
                                        @Override public boolean advanceExact(int target) { docID = target; return true; }
                                        @Override public int docID() { return docID; }
                                        @Override public int nextDoc() { return ++docID < maxDoc ? docID : NO_MORE_DOCS; }
                                        @Override public int advance(int target) { docID = target; return docID < maxDoc ? docID : NO_MORE_DOCS; }
                                        @Override public long cost() { return maxDoc; }
                                    };
                                }

                                @Override public org.apache.lucene.index.NumericDocValues getNumeric(FieldInfo fi) { return null; }
                                @Override public org.apache.lucene.index.BinaryDocValues getBinary(FieldInfo fi) { return null; }
                                @Override public org.apache.lucene.index.SortedDocValues getSorted(FieldInfo fi) { return null; }
                                @Override public org.apache.lucene.index.SortedSetDocValues getSortedSet(FieldInfo fi) { return null; }
                                @Override public org.apache.lucene.index.DocValuesSkipper getSkipper(FieldInfo fi) { return null; }
                                @Override public void checkIntegrity() {}
                                @Override public void close() {}
                            });
                        } else {
                            delegateConsumer.addSortedNumericField(field, valuesProducer);
                        }
                    }

                    @Override
                    public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
                        delegateConsumer.addNumericField(field, valuesProducer);
                    }

                    @Override
                    public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
                        delegateConsumer.addBinaryField(field, valuesProducer);
                    }

                    @Override
                    public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
                        delegateConsumer.addSortedField(field, valuesProducer);
                    }

                    @Override
                    public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
                        delegateConsumer.addSortedSetField(field, valuesProducer);
                    }

                    @Override
                    public void close() throws IOException {
                        delegateConsumer.close();
                    }
                };
            }

            @Override
            public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
                return delegateFormat.fieldsProducer(state);
            }
        };
    }
}
