/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodec;

import org.apache.lucene.codecs.*;
import org.apache.lucene.codecs.lucene90.*;
import org.apache.lucene.codecs.lucene92.Lucene92HnswVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;

public class Lucene92CustomCodec extends Codec {
    public static final int DEFAULT_COMPRESSION_LEVEL = 6;

    /** Each mode represents a compression algorithm. */
    public enum Mode {
        ZSTD,
        ZSTDNODICT,
        LZ4
    }

    private final TermVectorsFormat vectorsFormat = new Lucene90TermVectorsFormat();
    private final FieldInfosFormat fieldInfosFormat = new Lucene90FieldInfosFormat();
    private final SegmentInfoFormat segmentInfosFormat = new Lucene90SegmentInfoFormat();
    private final LiveDocsFormat liveDocsFormat = new Lucene90LiveDocsFormat();
    private final CompoundFormat compoundFormat = new Lucene90CompoundFormat();
    private final NormsFormat normsFormat = new Lucene90NormsFormat();

    private final PostingsFormat defaultPostingsFormat;
    private final PostingsFormat postingsFormat = new PerFieldPostingsFormat() {
        @Override
        public PostingsFormat getPostingsFormatForField(String field) {
            return Lucene92CustomCodec.this.getPostingsFormatForField(field);
        }
    };

    private final DocValuesFormat defaultDVFormat;
    private final DocValuesFormat docValuesFormat = new PerFieldDocValuesFormat() {
        @Override
        public DocValuesFormat getDocValuesFormatForField(String field) {
            return Lucene92CustomCodec.this.getDocValuesFormatForField(field);
        }
    };

    private final KnnVectorsFormat defaultKnnVectorsFormat;
    private final KnnVectorsFormat knnVectorsFormat = new PerFieldKnnVectorsFormat() {
        @Override
        public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
            return Lucene92CustomCodec.this.getKnnVectorsFormatForField(field);
        }
    };

    private final StoredFieldsFormat storedFieldsFormat;

    /** Default codec */
    public Lucene92CustomCodec() {
        this(Mode.LZ4);
    }

    /** new codec for a given compression algorithm and default compression level */
    public Lucene92CustomCodec(Mode mode) {
        super(mode.name());
        this.storedFieldsFormat = new Lucene92CustomStoredFieldsFormat(mode);
        this.defaultPostingsFormat = new Lucene90PostingsFormat();
        this.defaultDVFormat = new Lucene90DocValuesFormat();
        this.defaultKnnVectorsFormat = new Lucene92HnswVectorsFormat();
    }

    @Override
    public final StoredFieldsFormat storedFieldsFormat() {
        return storedFieldsFormat;
    }

    @Override
    public final TermVectorsFormat termVectorsFormat() {
        return vectorsFormat;
    }

    @Override
    public final PostingsFormat postingsFormat() {
        return postingsFormat;
    }

    @Override
    public final FieldInfosFormat fieldInfosFormat() {
        return fieldInfosFormat;
    }

    @Override
    public final SegmentInfoFormat segmentInfoFormat() {
        return segmentInfosFormat;
    }

    @Override
    public final LiveDocsFormat liveDocsFormat() {
        return liveDocsFormat;
    }

    @Override
    public final CompoundFormat compoundFormat() {
        return compoundFormat;
    }

    @Override
    public final PointsFormat pointsFormat() {
        return new Lucene90PointsFormat();
    }

    @Override
    public final KnnVectorsFormat knnVectorsFormat() {
        return knnVectorsFormat;
    }

    public PostingsFormat getPostingsFormatForField(String field) {
        return defaultPostingsFormat;
    }

    public DocValuesFormat getDocValuesFormatForField(String field) {
        return defaultDVFormat;
    }

    public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        return defaultKnnVectorsFormat;
    }

    @Override
    public final DocValuesFormat docValuesFormat() {
        return docValuesFormat;
    }

    @Override
    public final NormsFormat normsFormat() {
        return normsFormat;
    }
}
