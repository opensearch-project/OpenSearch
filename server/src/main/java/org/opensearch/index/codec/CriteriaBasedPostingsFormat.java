/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.opensearch.index.codec.CriteriaBasedCodec.ATTRIBUTE_BINDING_TARGET_FIELD;

/**
 * Postings format to attach segment info attribute corresponding to grouping criteria associated with segments
 *
 */
public class CriteriaBasedPostingsFormat extends PostingsFormat {

    public static final String CRITERIA_BASED_CODEC_NAME = "CriteriaBasedCodec99";
    private final PostingsFormat delegatePostingsFormat;
    private final String bucket;
    /** Extension of CAS index to store delegate information. */
    public static final String CAS_FILE_EXTENSION = "cas";
    public static final int VERSION_START = 0;
    public static final int VERSION_CURRENT = VERSION_START;
    public static final String BUCKET_NAME = "bucket";
    private static final String PLACEHOLDER_BUCKET_FOR_PARENT_WRITER = "-2";
    private static final String DELEGATE_CODEC_KEY = "delegate_codec_key";

    /**
     * Creates a new postings format.
     *
     * <p>The provided name will be written into the index segment in some configurations (such as
     * when using ): in such configurations, for the segment to be read
     * this class should be registered with Java's SPI mechanism (registered in META-INF/ of your jar
     * file, etc).
     *
     */
    protected CriteriaBasedPostingsFormat(PostingsFormat delegatePostingsFormat, String bucket) {
        super(CRITERIA_BASED_CODEC_NAME);
        this.delegatePostingsFormat = delegatePostingsFormat;
        this.bucket = bucket;
    }

    // Needed for SPI
    public CriteriaBasedPostingsFormat() {
        this(null, null);
    }

    @Override
    public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        if (delegatePostingsFormat == null) {
            throw new UnsupportedOperationException(
                "Error - " + getClass().getName() + " has been constructed without a choice of PostingsFormat"
            );
        }

        FieldsConsumer fieldsConsumer = delegatePostingsFormat.fieldsConsumer(state);
        return new CriteriaBasedFieldsConsumer(fieldsConsumer, state);
    }

    @Override
    public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
        assert state.segmentInfo.getAttribute(DELEGATE_CODEC_KEY) != null;
        return PostingsFormat.forName(state.segmentInfo.getAttribute(DELEGATE_CODEC_KEY)).fieldsProducer(state);
    }

    class CriteriaBasedFieldsConsumer extends FieldsConsumer {
        private final FieldsConsumer delegateFieldsConsumer;
        private SegmentWriteState state;
        private boolean closed;

        public CriteriaBasedFieldsConsumer(FieldsConsumer delegateFieldsConsumer, SegmentWriteState state) {
            this.delegateFieldsConsumer = delegateFieldsConsumer;
            this.state = state;
        }

        @Override
        public void write(Fields fields, NormsProducer norms) throws IOException {
            delegateFieldsConsumer.write(fields, norms);
            FieldInfo idFieldInfo = state.fieldInfos.fieldInfo(ATTRIBUTE_BINDING_TARGET_FIELD);
            if (bucket != null) {
                state.segmentInfo.putAttribute(BUCKET_NAME, bucket);
                if (idFieldInfo != null) {
                    idFieldInfo.putAttribute(BUCKET_NAME, bucket);
                }
            } else if (state.segmentInfo.getAttribute(BUCKET_NAME) == null) {
                // For segment belonging to parent writer, attributes will be set. In case write went to parent
                // writer (like for no ops writes or for temporary tombstone entry which is added for deletes/updates
                // to sync version across child and parent writers), segments corresponding to those writer does not
                // have.
                state.segmentInfo.putAttribute(BUCKET_NAME, PLACEHOLDER_BUCKET_FOR_PARENT_WRITER);
                if (idFieldInfo != null) {
                    idFieldInfo.putAttribute(BUCKET_NAME, PLACEHOLDER_BUCKET_FOR_PARENT_WRITER);
                }
            } else if (idFieldInfo != null) {
                idFieldInfo.putAttribute(BUCKET_NAME, state.segmentInfo.getAttribute(BUCKET_NAME));
            }

            state.segmentInfo.putAttribute(DELEGATE_CODEC_KEY, delegatePostingsFormat.getName());
        }

        @Override
        public void merge(MergeState mergeState, NormsProducer norms) throws IOException {
            delegateFieldsConsumer.merge(mergeState, norms);
            Set<String> mergeFieldNames = StreamSupport.stream(mergeState.mergeFieldInfos.spliterator(), false)
                .map(FieldInfo::getName)
                .collect(Collectors.toSet());
            if (mergeFieldNames.contains(ATTRIBUTE_BINDING_TARGET_FIELD) && mergeState.fieldInfos.length > 0) {
                String attribute = mergeState.fieldInfos[0].fieldInfo(ATTRIBUTE_BINDING_TARGET_FIELD).getAttribute(BUCKET_NAME);
                assert attribute != null : "Attribute should not be null during merging segment.";
                mergeState.segmentInfo.putAttribute(BUCKET_NAME, attribute);

                mergeState.mergeFieldInfos.fieldInfo(ATTRIBUTE_BINDING_TARGET_FIELD).putAttribute(BUCKET_NAME, attribute);
            }

            mergeState.segmentInfo.putAttribute(DELEGATE_CODEC_KEY, delegatePostingsFormat.getName());
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }

            closed = true;
            delegateFieldsConsumer.close();
        }
    }
}
