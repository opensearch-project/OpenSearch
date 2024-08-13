/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.composite.composite99;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.codec.composite.CompositeIndexReader;
import org.opensearch.index.codec.composite.DocValuesProvider;
import org.opensearch.index.codec.composite.LuceneDocValuesProducerFactory;
import org.opensearch.index.compositeindex.CompositeIndexMetadata;
import org.opensearch.index.compositeindex.datacube.startree.fileformats.meta.MetricEntry;
import org.opensearch.index.compositeindex.datacube.startree.fileformats.meta.StarTreeMetadata;
import org.opensearch.index.compositeindex.datacube.startree.index.CompositeIndexValues;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.mapper.CompositeMappedFieldType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.index.compositeindex.CompositeIndexConstants.COMPOSITE_FIELD_MARKER;
import static org.opensearch.index.compositeindex.datacube.startree.fileformats.StarTreeWriter.VERSION_CURRENT;
import static org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeUtils.fullyQualifiedFieldNameForStarTreeDimensionsDocValues;
import static org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeUtils.fullyQualifiedFieldNameForStarTreeMetricsDocValues;
import static org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeUtils.getFieldInfoList;

/**
 * Reader for star tree index and star tree doc values from the segments
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class Composite99DocValuesReader extends DocValuesProducer implements CompositeIndexReader {
    private static final Logger logger = LogManager.getLogger(Composite99DocValuesReader.class);

    private final DocValuesProducer delegate;
    private IndexInput dataIn;
    private ChecksumIndexInput metaIn;
    private final Map<String, IndexInput> compositeIndexInputMap = new LinkedHashMap<>();
    private final Map<String, CompositeIndexMetadata> compositeIndexMetadataMap = new LinkedHashMap<>();
    private final List<String> fields;
    private DocValuesProvider compositeDocValuesProducer;
    private final List<CompositeIndexFieldInfo> compositeFieldInfos = new ArrayList<>();
    private final SegmentReadState readState;

    public Composite99DocValuesReader(DocValuesProducer producer, SegmentReadState readState) throws IOException {
        this.delegate = producer;
        this.readState = readState;
        this.fields = new ArrayList<>();

        String metaFileName = IndexFileNames.segmentFileName(
            readState.segmentInfo.name,
            readState.segmentSuffix,
            Composite99DocValuesFormat.META_EXTENSION
        );

        String dataFileName = IndexFileNames.segmentFileName(
            readState.segmentInfo.name,
            readState.segmentSuffix,
            Composite99DocValuesFormat.DATA_EXTENSION
        );

        boolean success = false;
        try {

            // initialize meta input
            dataIn = readState.directory.openInput(dataFileName, readState.context);
            CodecUtil.checkIndexHeader(
                dataIn,
                Composite99DocValuesFormat.DATA_CODEC_NAME,
                Composite99DocValuesFormat.VERSION_START,
                Composite99DocValuesFormat.VERSION_CURRENT,
                readState.segmentInfo.getId(),
                readState.segmentSuffix
            );

            // initialize data input
            metaIn = readState.directory.openChecksumInput(metaFileName, readState.context);
            Throwable priorE = null;
            try {
                CodecUtil.checkIndexHeader(
                    metaIn,
                    Composite99DocValuesFormat.META_CODEC_NAME,
                    Composite99DocValuesFormat.VERSION_START,
                    Composite99DocValuesFormat.VERSION_CURRENT,
                    readState.segmentInfo.getId(),
                    readState.segmentSuffix
                );

                while (true) {

                    // validate magic marker
                    long magicMarker = metaIn.readLong();
                    if (magicMarker == -1) {
                        logger.info("EOF reached for composite index metadata");
                        break;
                    } else if (magicMarker < 0) {
                        throw new CorruptIndexException("Unknown token encountered: " + magicMarker, metaIn);
                    } else if (COMPOSITE_FIELD_MARKER != magicMarker) {
                        logger.error("Invalid composite field magic marker");
                        throw new IOException("Invalid composite field magic marker");
                    }

                    int version = metaIn.readVInt();
                    if (VERSION_CURRENT != version) {
                        logger.error("Invalid composite field version");
                        throw new IOException("Invalid composite field version");
                    }

                    // construct composite index metadata
                    String compositeFieldName = metaIn.readString();
                    CompositeMappedFieldType.CompositeFieldType compositeFieldType = CompositeMappedFieldType.CompositeFieldType.fromName(
                        metaIn.readString()
                    );

                    switch (compositeFieldType) {
                        case STAR_TREE:
                            StarTreeMetadata starTreeMetadata = new StarTreeMetadata(metaIn, compositeFieldName, compositeFieldType);
                            compositeFieldInfos.add(new CompositeIndexFieldInfo(compositeFieldName, compositeFieldType));

                            IndexInput starTreeIndexInput = dataIn.slice(
                                "star-tree data slice for respective star-tree fields",
                                starTreeMetadata.getDataStartFilePointer(),
                                starTreeMetadata.getDataLength()
                            );
                            compositeIndexInputMap.put(compositeFieldName, starTreeIndexInput);
                            compositeIndexMetadataMap.put(compositeFieldName, starTreeMetadata);

                            List<String> dimensionFields = starTreeMetadata.getDimensionFields();

                            // generating star tree unique fields (fully qualified name for dimension and metrics)
                            for (String dimensions : dimensionFields) {
                                fields.add(fullyQualifiedFieldNameForStarTreeDimensionsDocValues(compositeFieldName, dimensions));
                            }

                            for (MetricEntry metricEntry : starTreeMetadata.getMetricEntries()) {
                                fields.add(
                                    fullyQualifiedFieldNameForStarTreeMetricsDocValues(
                                        compositeFieldName,
                                        metricEntry.getMetricFieldName(),
                                        metricEntry.getMetricStat().getTypeName()
                                    )
                                );
                            }

                            break;
                        default:
                            throw new CorruptIndexException("Invalid composite field type found in the file", dataIn);
                    }
                }

                // populates the dummy list of field infos to fetch doc id set iterators for respective fields.
                // the dummy field info is used to fetch the doc id set iterators for respective fields based on field name
                FieldInfos fieldInfos = new FieldInfos(getFieldInfoList(fields));
                SegmentReadState segmentReadState = new SegmentReadState(
                    readState.directory,
                    readState.segmentInfo,
                    fieldInfos,
                    readState.context
                );

                // initialize star-tree doc values producer

                compositeDocValuesProducer = LuceneDocValuesProducerFactory.getDocValuesProducerForCompositeCodec(
                    Composite99Codec.COMPOSITE_INDEX_CODEC_NAME,
                    segmentReadState,
                    Composite99DocValuesFormat.DATA_DOC_VALUES_CODEC,
                    Composite99DocValuesFormat.DATA_DOC_VALUES_EXTENSION,
                    Composite99DocValuesFormat.META_DOC_VALUES_CODEC,
                    Composite99DocValuesFormat.META_DOC_VALUES_EXTENSION
                );

            } catch (Throwable t) {
                priorE = t;
            } finally {
                CodecUtil.checkFooter(metaIn, priorE);
            }
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(this);
            }
        }
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
        return delegate.getNumeric(field);
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        return delegate.getBinary(field);
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) throws IOException {
        return delegate.getSorted(field);
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
        return delegate.getSortedNumeric(field);
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
        return delegate.getSortedSet(field);
    }

    @Override
    public void checkIntegrity() throws IOException {
        delegate.checkIntegrity();
        CodecUtil.checksumEntireFile(dataIn);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
        boolean success = false;
        try {
            IOUtils.close(metaIn, dataIn);
            IOUtils.close(compositeDocValuesProducer.getDocValuesProducer());
            success = true;
        } finally {
            if (!success) {
                IOUtils.closeWhileHandlingException(metaIn, dataIn);
            }
            compositeIndexInputMap.clear();
            compositeIndexMetadataMap.clear();
            fields.clear();
            metaIn = null;
            dataIn = null;
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
                    compositeDocValuesProducer
                );

            default:
                throw new CorruptIndexException("Unsupported composite index field type: ", compositeIndexFieldInfo.getType().getName());
        }

    }

    /**
     * Returns the sorted numeric doc values for the given sorted numeric field.
     * If the sorted numeric field is null, it returns an empty doc id set iterator.
     * <p>
     * Sorted numeric field can be null for cases where the segment doesn't hold a particular value.
     *
     * @param sortedNumeric the sorted numeric doc values for a field
     * @return empty sorted numeric values if the field is not present, else sortedNumeric
     */
    public static SortedNumericDocValues getSortedNumericDocValues(SortedNumericDocValues sortedNumeric) {
        return sortedNumeric == null ? DocValues.emptySortedNumeric() : sortedNumeric;
    }

}
