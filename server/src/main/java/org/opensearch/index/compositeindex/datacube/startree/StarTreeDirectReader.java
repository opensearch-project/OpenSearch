/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.codec.composite.CompositeIndexReader;
import org.opensearch.index.codec.composite.LuceneDocValuesProducerFactory;
import org.opensearch.index.codec.composite.composite912.Composite912Codec;
import org.opensearch.index.codec.composite.composite912.Composite912DocValuesFormat;
import org.opensearch.index.compositeindex.CompositeIndexMetadata;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.fileformats.meta.DimensionConfig;
import org.opensearch.index.compositeindex.datacube.startree.fileformats.meta.StarTreeMetadata;
import org.opensearch.index.compositeindex.datacube.startree.index.CompositeIndexValues;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.mapper.CompositeMappedFieldType;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.index.compositeindex.CompositeIndexConstants.COMPOSITE_FIELD_MARKER;
import static org.opensearch.index.compositeindex.datacube.startree.fileformats.StarTreeWriter.VERSION_CURRENT;
import static org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeUtils.fullyQualifiedFieldNameForStarTreeDimensionsDocValues;
import static org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeUtils.fullyQualifiedFieldNameForStarTreeMetricsDocValues;
import static org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeUtils.getFieldInfoList;

/**
 * Standalone reader for star tree files (.cid/.cim/.cidvd/.cidvm) that works independently
 * of the segment's declared codec. Used for segments where the codec cannot be switched to
 * Composite912Codec (e.g., segments with soft deletes / docValuesGen != -1).
 *
 * <p>This reader implements {@link CompositeIndexReader} so it can be used interchangeably
 * with {@link org.opensearch.index.codec.composite.composite912.Composite912DocValuesReader}
 * in the query path.
 *
 * <p>Unlike Composite912DocValuesReader, this class does NOT extend DocValuesProducer and
 * does NOT require a delegate producer for regular doc values. It only reads star tree data.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class StarTreeDirectReader implements CompositeIndexReader, Closeable {
    private static final Logger logger = LogManager.getLogger(StarTreeDirectReader.class);

    private IndexInput dataIn;
    private final Map<String, IndexInput> compositeIndexInputMap = new LinkedHashMap<>();
    private final Map<String, CompositeIndexMetadata> compositeIndexMetadataMap = new LinkedHashMap<>();
    private final List<CompositeIndexFieldInfo> compositeFieldInfos = new ArrayList<>();
    private DocValuesProducer compositeDocValuesProducer;
    private SegmentReadState readState;

    /**
     * Opens star tree files for the given segment directly from the directory.
     *
     * @param directory   the index directory containing the star tree files
     * @param segmentInfo the segment's SegmentInfo (for name, ID, maxDoc)
     * @throws IOException if star tree files cannot be opened or are corrupt
     */
    public StarTreeDirectReader(Directory directory, SegmentInfo segmentInfo) throws IOException {
        List<String> fields = new ArrayList<>();
        String starTreeSuffix = "";

        String metaFileName = IndexFileNames.segmentFileName(segmentInfo.name, starTreeSuffix, Composite912DocValuesFormat.META_EXTENSION);

        String dataFileName = IndexFileNames.segmentFileName(segmentInfo.name, starTreeSuffix, Composite912DocValuesFormat.DATA_EXTENSION);

        boolean success = false;
        try (ChecksumIndexInput metaIn = directory.openChecksumInput(metaFileName)) {

            dataIn = directory.openInput(dataFileName, IOContext.DEFAULT);
            CodecUtil.checkIndexHeader(
                dataIn,
                Composite912DocValuesFormat.DATA_CODEC_NAME,
                Composite912DocValuesFormat.VERSION_START,
                Composite912DocValuesFormat.VERSION_CURRENT,
                segmentInfo.getId(),
                starTreeSuffix
            );

            Throwable priorE = null;
            try {
                CodecUtil.checkIndexHeader(
                    metaIn,
                    Composite912DocValuesFormat.META_CODEC_NAME,
                    Composite912DocValuesFormat.VERSION_START,
                    Composite912DocValuesFormat.VERSION_CURRENT,
                    segmentInfo.getId(),
                    starTreeSuffix
                );
                Map<String, DocValuesType> dimensionFieldTypeMap = new HashMap<>();
                while (true) {
                    long magicMarker = metaIn.readLong();
                    if (magicMarker == -1) {
                        break;
                    } else if (magicMarker < 0) {
                        throw new CorruptIndexException("Unknown token encountered: " + magicMarker, metaIn);
                    } else if (COMPOSITE_FIELD_MARKER != magicMarker) {
                        throw new IOException("Invalid composite field magic marker");
                    }

                    int version = metaIn.readVInt();
                    if (VERSION_CURRENT != version) {
                        throw new IOException("Invalid composite field version");
                    }

                    String compositeFieldName = metaIn.readString();
                    CompositeMappedFieldType.CompositeFieldType compositeFieldType = CompositeMappedFieldType.CompositeFieldType.fromName(
                        metaIn.readString()
                    );

                    switch (compositeFieldType) {
                        case STAR_TREE:
                            StarTreeMetadata starTreeMetadata = new StarTreeMetadata(
                                metaIn,
                                compositeFieldName,
                                compositeFieldType,
                                version
                            );
                            compositeFieldInfos.add(new CompositeIndexFieldInfo(compositeFieldName, compositeFieldType));

                            IndexInput starTreeIndexInput = dataIn.slice(
                                "star-tree data slice for respective star-tree fields",
                                starTreeMetadata.getDataStartFilePointer(),
                                starTreeMetadata.getDataLength()
                            );
                            compositeIndexInputMap.put(compositeFieldName, starTreeIndexInput);
                            compositeIndexMetadataMap.put(compositeFieldName, starTreeMetadata);

                            Map<String, DimensionConfig> dimensionFieldToDocValuesMap = starTreeMetadata.getDimensionFields();
                            for (Map.Entry<String, DimensionConfig> dimensionEntry : dimensionFieldToDocValuesMap.entrySet()) {
                                String dimName = fullyQualifiedFieldNameForStarTreeDimensionsDocValues(
                                    compositeFieldName,
                                    dimensionEntry.getKey()
                                );
                                fields.add(dimName);
                                dimensionFieldTypeMap.put(dimName, dimensionEntry.getValue().getDocValuesType());
                            }
                            for (Metric metric : starTreeMetadata.getMetrics()) {
                                for (MetricStat metricStat : metric.getBaseMetrics()) {
                                    fields.add(
                                        fullyQualifiedFieldNameForStarTreeMetricsDocValues(
                                            compositeFieldName,
                                            metric.getField(),
                                            metricStat.getTypeName()
                                        )
                                    );
                                }
                            }
                            break;
                        default:
                            throw new CorruptIndexException("Invalid composite field type found in the file", dataIn);
                    }
                }

                FieldInfos fieldInfos = new FieldInfos(getFieldInfoList(fields, dimensionFieldTypeMap));
                this.readState = new SegmentReadState(directory, segmentInfo, fieldInfos, IOContext.DEFAULT, starTreeSuffix);

                compositeDocValuesProducer = LuceneDocValuesProducerFactory.getDocValuesProducerForCompositeCodec(
                    Composite912Codec.COMPOSITE_INDEX_CODEC_NAME,
                    this.readState,
                    Composite912DocValuesFormat.DATA_DOC_VALUES_CODEC,
                    Composite912DocValuesFormat.DATA_DOC_VALUES_EXTENSION,
                    Composite912DocValuesFormat.META_DOC_VALUES_CODEC,
                    Composite912DocValuesFormat.META_DOC_VALUES_EXTENSION
                );

            } catch (Throwable t) {
                priorE = t;
            } finally {
                CodecUtil.checkFooter(metaIn, priorE);
            }
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    @Override
    public List<CompositeIndexFieldInfo> getCompositeIndexFields() {
        return compositeFieldInfos;
    }

    @Override
    public CompositeIndexValues getCompositeIndexValues(CompositeIndexFieldInfo compositeIndexFieldInfo) throws IOException {
        switch (compositeIndexFieldInfo.getType()) {
            case STAR_TREE:
                return new StarTreeValues(
                    compositeIndexMetadataMap.get(compositeIndexFieldInfo.getField()),
                    compositeIndexInputMap.get(compositeIndexFieldInfo.getField()),
                    compositeDocValuesProducer,
                    this.readState
                );
            default:
                throw new CorruptIndexException("Unsupported composite index field type: ", compositeIndexFieldInfo.getType().getName());
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (compositeDocValuesProducer != null) {
                compositeDocValuesProducer.close();
            }
        } finally {
            if (dataIn != null) {
                dataIn.close();
            }
            compositeIndexInputMap.clear();
            compositeIndexMetadataMap.clear();
        }
    }
}
