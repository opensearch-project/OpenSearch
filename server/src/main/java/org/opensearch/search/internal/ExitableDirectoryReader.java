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

package org.opensearch.search.internal;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.suggest.document.CompletionTerms;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.opensearch.common.lucene.index.SequentialStoredFieldsLeafReader;
import org.opensearch.core.common.Strings;

import java.io.IOException;

/**
 * Wraps an {@link IndexReader} with a {@link QueryCancellation}
 * which checks for cancelled or timed-out query.
 *
 * @opensearch.internal
 */
class ExitableDirectoryReader extends FilterDirectoryReader {

    /**
     * Used to check if query cancellation is actually enabled
     * and if so use it to check if the query is cancelled or timed-out.
     */
    interface QueryCancellation {

        /**
         * Used to prevent unnecessary checks for cancellation
         * @return true if query cancellation is enabled
         */
        boolean isEnabled();

        /**
         * Call to check if the query is cancelled or timed-out.
         * If so a {@link RuntimeException} is thrown
         */
        void checkCancelled();
    }

    ExitableDirectoryReader(DirectoryReader in, QueryCancellation queryCancellation) throws IOException {
        super(in, new SubReaderWrapper() {
            @Override
            public LeafReader wrap(LeafReader reader) {
                return new ExitableLeafReader(reader, queryCancellation);
            }
        });
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) {
        throw new UnsupportedOperationException("doWrapDirectoryReader() should never be invoked");
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
    }

    /**
     * Wraps a {@link FilterLeafReader} with a {@link QueryCancellation}.
     */
    static class ExitableLeafReader extends SequentialStoredFieldsLeafReader {

        private final QueryCancellation queryCancellation;

        private ExitableLeafReader(LeafReader leafReader, QueryCancellation queryCancellation) {
            super(leafReader);
            this.queryCancellation = queryCancellation;
        }

        @Override
        public PointValues getPointValues(String field) throws IOException {
            if (Strings.isEmpty(field)) {
                return null;
            }
            final PointValues pointValues = in.getPointValues(field);
            if (pointValues == null) {
                return null;
            }
            return queryCancellation.isEnabled() ? new ExitablePointValues(pointValues, queryCancellation) : pointValues;
        }

        @Override
        public Terms terms(String field) throws IOException {
            Terms terms = in.terms(field);
            if (terms == null) {
                return null;
            }
            // If we have a suggest CompletionQuery then the CompletionWeight#bulkScorer() will check that
            // the terms are instanceof CompletionTerms (not generic FilterTerms) and will throw an exception
            // if that's not the case.
            return (queryCancellation.isEnabled() && terms instanceof CompletionTerms == false)
                ? new ExitableTerms(terms, queryCancellation)
                : terms;
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return in.getCoreCacheHelper();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }

        @Override
        protected StoredFieldsReader doGetSequentialStoredFieldsReader(StoredFieldsReader reader) {
            return reader;
        }
    }

    /**
     * Wrapper class for {@link FilterLeafReader.FilterTerms} that check for query cancellation or timeout.
     */
    static class ExitableTerms extends FilterLeafReader.FilterTerms {

        private final QueryCancellation queryCancellation;

        private ExitableTerms(Terms terms, QueryCancellation queryCancellation) {
            super(terms);
            this.queryCancellation = queryCancellation;
        }

        @Override
        public TermsEnum intersect(CompiledAutomaton compiled, BytesRef startTerm) throws IOException {
            return new ExitableTermsEnum(in.intersect(compiled, startTerm), queryCancellation);
        }

        @Override
        public TermsEnum iterator() throws IOException {
            return new ExitableTermsEnum(in.iterator(), queryCancellation);
        }
    }

    /**
     * Wrapper class for {@link FilterLeafReader.FilterTermsEnum} that is used by {@link ExitableTerms} for
     * implementing an exitable enumeration of terms.
     */
    private static class ExitableTermsEnum extends FilterLeafReader.FilterTermsEnum {

        private static final int MAX_CALLS_BEFORE_QUERY_TIMEOUT_CHECK = (1 << 4) - 1; // 15

        private int calls;
        private final QueryCancellation queryCancellation;

        private ExitableTermsEnum(TermsEnum termsEnum, QueryCancellation queryCancellation) {
            super(termsEnum);
            this.queryCancellation = queryCancellation;
            this.queryCancellation.checkCancelled();
        }

        private void checkAndThrowWithSampling() {
            if ((calls++ & MAX_CALLS_BEFORE_QUERY_TIMEOUT_CHECK) == 0) {
                queryCancellation.checkCancelled();
            }
        }

        @Override
        public BytesRef next() throws IOException {
            checkAndThrowWithSampling();
            return in.next();
        }
    }

    // delegates to PointValues but adds query cancellation checks
    private static class ExitablePointTree implements PointValues.PointTree {
        private final PointValues values;
        private final PointValues.PointTree pointTree;
        private final ExitableIntersectVisitor exitableIntersectVisitor;
        private final QueryCancellation queryCancellation;
        private int calls;

        private ExitablePointTree(PointValues values, PointValues.PointTree pointTree, QueryCancellation queryCancellation) {
            this.values = values;
            this.pointTree = pointTree;
            this.exitableIntersectVisitor = new ExitableIntersectVisitor(queryCancellation);
            this.queryCancellation = queryCancellation;
        }

        @Override
        public PointValues.PointTree clone() {
            queryCancellation.checkCancelled();
            return new ExitablePointTree(values, pointTree.clone(), queryCancellation);
        }

        @Override
        public boolean moveToChild() throws IOException {
            checkAndThrowWithSampling();
            return pointTree.moveToChild();
        }

        @Override
        public boolean moveToSibling() throws IOException {
            checkAndThrowWithSampling();
            return pointTree.moveToSibling();
        }

        @Override
        public boolean moveToParent() throws IOException {
            checkAndThrowWithSampling();
            return pointTree.moveToParent();
        }

        @Override
        public byte[] getMinPackedValue() {
            checkAndThrowWithSampling();
            return pointTree.getMinPackedValue();
        }

        @Override
        public byte[] getMaxPackedValue() {
            checkAndThrowWithSampling();
            return pointTree.getMaxPackedValue();
        }

        @Override
        public long size() {
            queryCancellation.checkCancelled();
            return pointTree.size();
        }

        @Override
        public void visitDocIDs(PointValues.IntersectVisitor visitor) throws IOException {
            queryCancellation.checkCancelled();
            pointTree.visitDocIDs(visitor);
        }

        @Override
        public void visitDocValues(PointValues.IntersectVisitor visitor) throws IOException {
            queryCancellation.checkCancelled();
            exitableIntersectVisitor.setVisitor(visitor);
            pointTree.visitDocValues(exitableIntersectVisitor);
        }

        // reuse ExitableIntersectVisitor#checkAndThrowWithSampling
        private void checkAndThrowWithSampling() {
            if ((calls++ & ExitableIntersectVisitor.MAX_CALLS_BEFORE_QUERY_TIMEOUT_CHECK) == 0) {
                queryCancellation.checkCancelled();
            }
        }
    }

    /**
     * Wrapper class for {@link PointValues} that checks for query cancellation or timeout.
     */
    static class ExitablePointValues extends PointValues {

        private final PointValues in;
        private final QueryCancellation queryCancellation;

        private ExitablePointValues(PointValues in, QueryCancellation queryCancellation) {
            this.in = in;
            this.queryCancellation = queryCancellation;
            this.queryCancellation.checkCancelled();
        }

        @Override
        public PointTree getPointTree() throws IOException {
            return new ExitablePointTree(in, in.getPointTree(), queryCancellation);
        }

        @Override
        public byte[] getMinPackedValue() throws IOException {
            queryCancellation.checkCancelled();
            return in.getMinPackedValue();
        }

        @Override
        public byte[] getMaxPackedValue() throws IOException {
            queryCancellation.checkCancelled();
            return in.getMaxPackedValue();
        }

        @Override
        public int getNumDimensions() throws IOException {
            queryCancellation.checkCancelled();
            return in.getNumDimensions();
        }

        @Override
        public int getNumIndexDimensions() throws IOException {
            queryCancellation.checkCancelled();
            return in.getNumIndexDimensions();
        }

        @Override
        public int getBytesPerDimension() throws IOException {
            queryCancellation.checkCancelled();
            return in.getBytesPerDimension();
        }

        @Override
        public long size() {
            queryCancellation.checkCancelled();
            return in.size();
        }

        @Override
        public int getDocCount() {
            queryCancellation.checkCancelled();
            return in.getDocCount();
        }
    }

    private static class ExitableIntersectVisitor implements PointValues.IntersectVisitor {

        private static final int MAX_CALLS_BEFORE_QUERY_TIMEOUT_CHECK = (1 << 13) - 1; // 8191

        private PointValues.IntersectVisitor in;
        private final QueryCancellation queryCancellation;
        private int calls;

        private ExitableIntersectVisitor(QueryCancellation queryCancellation) {
            this.queryCancellation = queryCancellation;
        }

        private void checkAndThrowWithSampling() {
            if ((calls++ & MAX_CALLS_BEFORE_QUERY_TIMEOUT_CHECK) == 0) {
                queryCancellation.checkCancelled();
            }
        }

        private void setVisitor(PointValues.IntersectVisitor in) {
            this.in = in;
        }

        @Override
        public void visit(int docID) throws IOException {
            checkAndThrowWithSampling();
            in.visit(docID);
        }

        @Override
        public void visit(int docID, byte[] packedValue) throws IOException {
            checkAndThrowWithSampling();
            in.visit(docID, packedValue);
        }

        @Override
        public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            queryCancellation.checkCancelled();
            return in.compare(minPackedValue, maxPackedValue);
        }

        @Override
        public void grow(int count) {
            queryCancellation.checkCancelled();
            in.grow(count);
        }
    }
}
