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

package org.opensearch.index.engine;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.OneMergeWrappingMergePolicy;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;

/**
 * This merge policy drops id field postings for all delete documents this can be
 * useful to guarantee consistent update performance even if a large number of deleted / updated documents
 * are retained. Merging postings away is efficient since lucene visits postings term by term and
 * with the original live-docs being available we are adding a negotiable overhead such that we can
 * prune soft-deletes by default. Yet, using this merge policy will cause loosing all search capabilities on top of
 * soft deleted documents independent of the retention policy. Note, in order for this merge policy to be effective it needs to be added
 * before the {@link org.apache.lucene.index.SoftDeletesRetentionMergePolicy} because otherwise only documents that are deleted / removed
 * anyways will be pruned.
 *
 * @opensearch.internal
 */
final class PrunePostingsAndKnnMergePolicy extends OneMergeWrappingMergePolicy {

    public static final String KNN_FIELD = "knn_field";

    PrunePostingsAndKnnMergePolicy(MergePolicy in, String idField) {
        super(in, toWrap -> new OneMerge(toWrap.segments) {
            @Override
            public CodecReader wrapForMerge(CodecReader reader) throws IOException {
                CodecReader wrapped = toWrap.wrapForMerge(reader);
                return wrapReader(wrapped, idField);
            }
        });
    }

    private static CodecReader wrapReader(CodecReader reader, String idField) {
        Bits liveDocs = reader.getLiveDocs();
        if (liveDocs == null) {
            return reader; // no deleted docs - we are good!
        }
        final boolean fullyDeletedSegment = reader.numDocs() == 0;
        return new FilterCodecReader(reader) {

            @Override
            public FieldsProducer getPostingsReader() {
                FieldsProducer postingsReader = super.getPostingsReader();
                if (postingsReader == null) {
                    return null;
                }
                return new FieldsProducer() {
                    @Override
                    public void close() throws IOException {
                        postingsReader.close();
                    }

                    @Override
                    public void checkIntegrity() throws IOException {
                        postingsReader.checkIntegrity();
                    }

                    @Override
                    public Iterator<String> iterator() {
                        return postingsReader.iterator();
                    }

                    @Override
                    public Terms terms(String field) throws IOException {
                        Terms in = postingsReader.terms(field);
                        if (idField.equals(field) && in != null) {
                            return new FilterLeafReader.FilterTerms(in) {
                                @Override
                                public TermsEnum iterator() throws IOException {
                                    TermsEnum iterator = super.iterator();
                                    return new FilteredTermsEnum(iterator, false) {
                                        private PostingsEnum internal;

                                        @Override
                                        protected AcceptStatus accept(BytesRef term) throws IOException {
                                            if (fullyDeletedSegment) {
                                                return AcceptStatus.END; // short-cut this if we don't match anything
                                            }
                                            internal = postings(internal, PostingsEnum.NONE);
                                            if (internal.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                                                return AcceptStatus.YES;
                                            }
                                            return AcceptStatus.NO;
                                        }

                                        @Override
                                        public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
                                            if (reuse instanceof OnlyLiveDocsPostingsEnum) {
                                                OnlyLiveDocsPostingsEnum reuseInstance = (OnlyLiveDocsPostingsEnum) reuse;
                                                reuseInstance.reset(super.postings(reuseInstance.in, flags));
                                                return reuseInstance;
                                            }
                                            return new OnlyLiveDocsPostingsEnum(super.postings(null, flags), liveDocs);
                                        }

                                        @Override
                                        public ImpactsEnum impacts(int flags) throws IOException {
                                            throw new UnsupportedOperationException();
                                        }
                                    };
                                }
                            };
                        } else {
                            return in;
                        }
                    }

                    @Override
                    public int size() {
                        return postingsReader.size();
                    }
                };
            }

            @Override
            public DocValuesProducer getDocValuesReader() {
                DocValuesProducer docValuesProducer = super.getDocValuesReader();
                if (docValuesProducer == null) {
                    return null;
                }

                return new DocValuesProducer() {
                    @Override
                    public NumericDocValues getNumeric(FieldInfo fieldInfo) throws IOException {
                        return docValuesProducer.getNumeric(fieldInfo);
                    }

                    @Override
                    public BinaryDocValues getBinary(FieldInfo fieldInfo) throws IOException {
                        BinaryDocValues binaryDocValues = docValuesProducer.getBinary(fieldInfo);
                        if (binaryDocValues != null && fieldInfo.attributes().containsKey(KNN_FIELD)) {
                            return new MergeFilterBinaryDocValues(binaryDocValues, liveDocs);
                        }
                        return binaryDocValues;
                    }

                    @Override
                    public SortedDocValues getSorted(FieldInfo fieldInfo) throws IOException {
                        return docValuesProducer.getSorted(fieldInfo);
                    }

                    @Override
                    public SortedNumericDocValues getSortedNumeric(FieldInfo fieldInfo) throws IOException {
                        return docValuesProducer.getSortedNumeric(fieldInfo);
                    }

                    @Override
                    public SortedSetDocValues getSortedSet(FieldInfo fieldInfo) throws IOException {
                        return docValuesProducer.getSortedSet(fieldInfo);
                    }

                    @Override
                    public DocValuesSkipper getSkipper(FieldInfo fieldInfo) throws IOException {
                        return docValuesProducer.getSkipper(fieldInfo);
                    }

                    @Override
                    public void checkIntegrity() throws IOException {
                        docValuesProducer.checkIntegrity();
                    }

                    @Override
                    public void close() throws IOException {
                        docValuesProducer.close();
                    }
                };
            }

            @Override
            public KnnVectorsReader getVectorReader() {
                return super.getVectorReader();
            }

            @Override
            public CacheHelper getCoreCacheHelper() {
                return null;
            }

            @Override
            public CacheHelper getReaderCacheHelper() {
                return null;
            }
        };
    }

    /**
     * Class holder for only live docs and the postings enumerator
     *
     * @opensearch.internal
     */
    private static final class OnlyLiveDocsPostingsEnum extends PostingsEnum {

        private final Bits liveDocs;
        private PostingsEnum in;

        OnlyLiveDocsPostingsEnum(PostingsEnum in, Bits liveDocs) {
            this.liveDocs = liveDocs;
            reset(in);
        }

        void reset(PostingsEnum in) {
            this.in = in;
        }

        @Override
        public int docID() {
            return in.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            int docId;
            do {
                docId = in.nextDoc();
            } while (docId != DocIdSetIterator.NO_MORE_DOCS && liveDocs.get(docId) == false);
            return docId;
        }

        @Override
        public int advance(int target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long cost() {
            return in.cost();
        }

        @Override
        public int freq() throws IOException {
            return in.freq();
        }

        @Override
        public int nextPosition() throws IOException {
            return in.nextPosition();
        }

        @Override
        public int startOffset() throws IOException {
            return in.startOffset();
        }

        @Override
        public int endOffset() throws IOException {
            return in.endOffset();
        }

        @Override
        public BytesRef getPayload() throws IOException {
            return in.getPayload();
        }
    }

    private static final class MergeFilterBinaryDocValues extends BinaryDocValues {

        protected final BinaryDocValues in;
        protected final Bits liveDocs;

        protected MergeFilterBinaryDocValues(BinaryDocValues in, Bits liveDocs) {
            Objects.requireNonNull(in);
            this.in = in;
            this.liveDocs = liveDocs;
        }

        public int docID() {
            return this.in.docID();
        }

        public int nextDoc() throws IOException {
            int docId;
            do {
                docId = in.nextDoc();
            } while (docId != DocIdSetIterator.NO_MORE_DOCS && liveDocs.get(docId) == false);
            return docId;
        }

        public int advance(int target) throws IOException {
            return this.in.advance(target);
        }

        public boolean advanceExact(int target) throws IOException {
            return this.in.advanceExact(target);
        }

        public long cost() {
            return this.in.cost();
        }

        public BytesRef binaryValue() throws IOException {
            return this.in.binaryValue();
        }
    }
}
