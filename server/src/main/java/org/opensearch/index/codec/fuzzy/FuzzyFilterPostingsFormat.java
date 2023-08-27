/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.fuzzy;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.BaseTermsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.automaton.CompiledAutomaton;

import java.io.IOException;
import java.util.*;

public final class FuzzyFilterPostingsFormat extends PostingsFormat {

    public static final String FUZZY_SET_CODEC_NAME = "FuzzySetCodec";
    public static final int VERSION_START = 3;
    public static final int VERSION_CURRENT = VERSION_START;

    /** Extension of Fuzzy Filters file */
    public static final String FUZZY_FILTER_DATA_EXTENSION = "fzd";

    private final PostingsFormat delegatePostingsFormat;
    private final FuzzySetFactory fuzzySetFactory;

    public FuzzyFilterPostingsFormat(
        PostingsFormat delegatePostingsFormat, FuzzySetFactory fuzzySetFactory) {
        super(FUZZY_SET_CODEC_NAME);
        this.delegatePostingsFormat = delegatePostingsFormat;
        this.fuzzySetFactory = fuzzySetFactory;
    }

    // Needed for SPI
    public FuzzyFilterPostingsFormat() {
        this(null, new FuzzySetFactory());
    }

    @Override
    public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        if (delegatePostingsFormat == null) {
            throw new UnsupportedOperationException(
                "Error - "
                    + getClass().getName()
                    + " has been constructed without a choice of PostingsFormat");
        }
        FieldsConsumer fieldsConsumer = delegatePostingsFormat.fieldsConsumer(state);
        return new FuzzySetFieldsConsumer(fieldsConsumer, state);
    }

    @Override
    public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new FuzzySetFieldsProducer(state);
    }

    static class FuzzySetFieldsProducer extends FieldsProducer {
        private FieldsProducer delegateFieldsProducer;
        HashMap<String, FuzzySet> fuzzySetsByFieldName = new HashMap<>();

        public FuzzySetFieldsProducer(SegmentReadState state) throws IOException {

            String fuzzySetFileName =
                IndexFileNames.segmentFileName(
                    state.segmentInfo.name, state.segmentSuffix, FUZZY_FILTER_DATA_EXTENSION);
            ChecksumIndexInput filterIn = null;
            boolean success = false;
            try {
                filterIn = state.directory.openChecksumInput(fuzzySetFileName, IOContext.READONCE);
                CodecUtil.checkIndexHeader(
                    filterIn,
                    FUZZY_SET_CODEC_NAME,
                    VERSION_START,
                    VERSION_CURRENT,
                    state.segmentInfo.getId(),
                    state.segmentSuffix);
                // // Load the hash function used in the Fuzzy filter
                // hashFunction = HashFunction.forName(filterIn.readString());
                // Load the delegate postings format
                PostingsFormat delegatePostingsFormat = PostingsFormat.forName(filterIn.readString());

                this.delegateFieldsProducer = delegatePostingsFormat.fieldsProducer(state);
                int numFilters = filterIn.readInt();
                for (int i = 0; i < numFilters; i++) {
                    int fieldNum = filterIn.readInt();
                    FuzzySet set = FuzzySetFactory.deserializeFuzzySet(filterIn);
                    FieldInfo fieldInfo = state.fieldInfos.fieldInfo(fieldNum);
                    fuzzySetsByFieldName.put(fieldInfo.name, set);
                }
                CodecUtil.checkFooter(filterIn);
                IOUtils.close(filterIn);
                success = true;
            } finally {
                if (!success) {
                    IOUtils.closeWhileHandlingException(filterIn, delegateFieldsProducer);
                }
            }
        }

        @Override
        public Iterator<String> iterator() {
            return delegateFieldsProducer.iterator();
        }

        @Override
        public void close() throws IOException {
            delegateFieldsProducer.close();
        }

        @Override
        public Terms terms(String field) throws IOException {
            FuzzySet filter = fuzzySetsByFieldName.get(field);
            if (filter == null) {
                return delegateFieldsProducer.terms(field);
            } else {
                Terms result = delegateFieldsProducer.terms(field);
                if (result == null) {
                    return null;
                }
                return new FuzzySetFieldsProducer.FuzzyFilterFrontedTerms(result, filter);
            }
        }

        @Override
        public int size() {
            return delegateFieldsProducer.size();
        }

        static class FuzzyFilterFrontedTerms extends Terms {
            private Terms delegateTerms;
            private FuzzySet filter;

            public FuzzyFilterFrontedTerms(Terms terms, FuzzySet filter) {
                this.delegateTerms = terms;
                this.filter = filter;
            }

            @Override
            public TermsEnum intersect(CompiledAutomaton compiled, final BytesRef startTerm)
                throws IOException {
                return delegateTerms.intersect(compiled, startTerm);
            }

            @Override
            public TermsEnum iterator() throws IOException {
                return new FilterAppliedTermsEnum(delegateTerms, filter);
            }

            @Override
            public long size() throws IOException {
                return delegateTerms.size();
            }

            @Override
            public long getSumTotalTermFreq() throws IOException {
                return delegateTerms.getSumTotalTermFreq();
            }

            @Override
            public long getSumDocFreq() throws IOException {
                return delegateTerms.getSumDocFreq();
            }

            @Override
            public int getDocCount() throws IOException {
                return delegateTerms.getDocCount();
            }

            @Override
            public boolean hasFreqs() {
                return delegateTerms.hasFreqs();
            }

            @Override
            public boolean hasOffsets() {
                return delegateTerms.hasOffsets();
            }

            @Override
            public boolean hasPositions() {
                return delegateTerms.hasPositions();
            }

            @Override
            public boolean hasPayloads() {
                return delegateTerms.hasPayloads();
            }

            @Override
            public BytesRef getMin() throws IOException {
                return delegateTerms.getMin();
            }

            @Override
            public BytesRef getMax() throws IOException {
                return delegateTerms.getMax();
            }
        }

        static final class FilterAppliedTermsEnum extends BaseTermsEnum {
            private Terms delegateTerms;
            private TermsEnum delegateTermsEnum;
            private final FuzzySet filter;

            public FilterAppliedTermsEnum(Terms delegateTerms, FuzzySet filter) throws IOException {
                this.delegateTerms = delegateTerms;
                this.filter = filter;
            }

            void reset(Terms delegateTerms) throws IOException {
                this.delegateTerms = delegateTerms;
                this.delegateTermsEnum = null;
            }

            private TermsEnum delegate() throws IOException {
                if (delegateTermsEnum == null) {
                    /* pull the iterator only if we really need it -
                     * this can be a relativly heavy operation depending on the
                     * delegate postings format and they underlying directory
                     * (clone IndexInput) */
                    delegateTermsEnum = delegateTerms.iterator();
                }
                return delegateTermsEnum;
            }

            @Override
            public BytesRef next() throws IOException {
                return delegate().next();
            }

            @Override
            public boolean seekExact(BytesRef text) throws IOException {
                // The magical fail-fast speed up that is the entire point of all of
                // this code - save a disk seek if there is a match on an in-memory
                // structure
                // that may occasionally give a false positive but guaranteed no false
                // negatives
                if (filter.contains(text) == FuzzySet.Result.NO) {
                    return false;
                }
                return delegate().seekExact(text);
            }

            @Override
            public SeekStatus seekCeil(BytesRef text) throws IOException {
                return delegate().seekCeil(text);
            }

            @Override
            public void seekExact(long ord) throws IOException {
                delegate().seekExact(ord);
            }

            @Override
            public BytesRef term() throws IOException {
                return delegate().term();
            }

            @Override
            public long ord() throws IOException {
                return delegate().ord();
            }

            @Override
            public int docFreq() throws IOException {
                return delegate().docFreq();
            }

            @Override
            public long totalTermFreq() throws IOException {
                return delegate().totalTermFreq();
            }

            @Override
            public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
                return delegate().postings(reuse, flags);
            }

            @Override
            public ImpactsEnum impacts(int flags) throws IOException {
                return delegate().impacts(flags);
            }

            @Override
            public String toString() {
                return getClass().getSimpleName() + "(filter=" + filter.toString() + ")";
            }
        }

        @Override
        public void checkIntegrity() throws IOException {
            delegateFieldsProducer.checkIntegrity();
        }

        @Override
        public String toString() {
            return getClass().getSimpleName()
                + "(fields="
                + fuzzySetsByFieldName.size()
                + ",delegate="
                + delegateFieldsProducer
                + ")";
        }
    }

    class FuzzySetFieldsConsumer extends FieldsConsumer {
        private FieldsConsumer delegateFieldsConsumer;
        private Map<FieldInfo, FuzzySet> fuzzySets = new HashMap<>();
        private SegmentWriteState state;

        public FuzzySetFieldsConsumer(FieldsConsumer fieldsConsumer, SegmentWriteState state) {
            this.delegateFieldsConsumer = fieldsConsumer;
            this.state = state;
        }

        @Override
        public void write(Fields fields, NormsProducer norms) throws IOException {

            // Delegate must write first: it may have opened files
            // on creating the class
            // (e.g. Lucene41PostingsConsumer), and write() will
            // close them; alternatively, if we delayed pulling
            // the fields consumer until here, we could do it
            // afterwards:
            delegateFieldsConsumer.write(fields, norms);

            for (String field : fields) {
                Terms terms = fields.terms(field);
                if (terms == null) {
                    continue;
                }
                FieldInfo fieldInfo = state.fieldInfos.fieldInfo(field);
                TermsEnum termsEnum = terms.iterator();

                FuzzySet fuzzySet = null;

                PostingsEnum postingsEnum = null;
                while (true) {
                    BytesRef term = termsEnum.next();
                    if (term == null) {
                        break;
                    }
                    if (fuzzySet == null) {
                        fuzzySet = fuzzySetFactory.createFuzzySet(state.segmentInfo.maxDoc(), fieldInfo.name);
                        if (fuzzySet == null) {
                            break;
                        }
                        assert fuzzySets.containsKey(fieldInfo) == false;
                        fuzzySets.put(fieldInfo, fuzzySet);
                    }
                    // Make sure there's at least one doc for this term:
                    postingsEnum = termsEnum.postings(postingsEnum, 0);
                    if (postingsEnum.nextDoc() != PostingsEnum.NO_MORE_DOCS) {
                        fuzzySet.add(term);
                    }
                }
            }
        }

        private boolean closed;

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            closed = true;
            delegateFieldsConsumer.close();

            // Now we are done accumulating values for these fields
            List<Map.Entry<FieldInfo, FuzzySet>> nonSaturatedSets = new ArrayList<>();

            for (Map.Entry<FieldInfo, FuzzySet> entry : fuzzySets.entrySet()) {
                FuzzySet fuzzySet = entry.getValue();
                if (!fuzzySet.isSaturated()) {
                    nonSaturatedSets.add(entry);
                }
            }
            String fuzzyFilterFileName =
                IndexFileNames.segmentFileName(
                    state.segmentInfo.name, state.segmentSuffix, FUZZY_FILTER_DATA_EXTENSION);
            try (IndexOutput fuzzyFilterFileOutput = state.directory.createOutput(fuzzyFilterFileName, state.context)) {
                CodecUtil.writeIndexHeader(
                    fuzzyFilterFileOutput,
                    FUZZY_SET_CODEC_NAME,
                    VERSION_CURRENT,
                    state.segmentInfo.getId(),
                    state.segmentSuffix);
                // remember the name of the postings format we will delegate to
                fuzzyFilterFileOutput.writeString(delegatePostingsFormat.getName());

                // First field in the output file is the number of fields+sets saved
                fuzzyFilterFileOutput.writeInt(nonSaturatedSets.size());
                for (Map.Entry<FieldInfo, FuzzySet> entry : nonSaturatedSets) {
                    FieldInfo fieldInfo = entry.getKey();
                    FuzzySet fuzzySet = entry.getValue();
                    fuzzyFilterFileOutput.writeInt(fieldInfo.number);
                    saveAppropriatelySizedFuzzySet(fuzzyFilterFileOutput, fuzzySet, fieldInfo);
                }
                CodecUtil.writeFooter(fuzzyFilterFileOutput);
            }
            // We are done with large bitsets so no need to keep them hanging around
            fuzzySets.clear();
        }

        private void saveAppropriatelySizedFuzzySet(
            IndexOutput fileOutput, FuzzySet fuzzySet, FieldInfo fieldInfo) throws IOException {
            fileOutput.writeString(fuzzySet.setType().getSetName());
            fuzzySet.maybeDownsize()
                .orElse(fuzzySet)
                .writeTo(fileOutput);
        }
    }

    @Override
    public String toString() {
        return "FuzzyFilterPostingsFormat(" + delegatePostingsFormat + ")";
    }
}
