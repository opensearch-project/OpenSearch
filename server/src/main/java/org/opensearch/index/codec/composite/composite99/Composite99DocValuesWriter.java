/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.composite.composite99;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.EmptyDocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.codec.composite.CompositeIndexReader;
import org.opensearch.index.codec.composite.LuceneDocValuesConsumerFactory;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.builder.StarTreesBuilder;
import org.opensearch.index.compositeindex.datacube.startree.index.CompositeIndexValues;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.mapper.CompositeMappedFieldType;
import org.opensearch.index.mapper.DocCountFieldMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.StarTreeMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class write the star tree index and star tree doc values
 * based on the doc values structures of the original index
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class Composite99DocValuesWriter extends DocValuesConsumer {
    private final DocValuesConsumer delegate;
    private final SegmentWriteState state;
    private final MapperService mapperService;
    AtomicReference<MergeState> mergeState = new AtomicReference<>();
    private final Set<CompositeMappedFieldType> compositeMappedFieldTypes;
    private final Set<String> compositeFieldSet;
    private DocValuesConsumer composite99DocValuesConsumer;

    public IndexOutput dataOut;
    public IndexOutput metaOut;
    private final Set<String> segmentFieldSet;
    private final boolean segmentHasCompositeFields;
    private final AtomicInteger fieldNumberAcrossCompositeFields;

    private final Map<String, DocValuesProducer> fieldProducerMap = new HashMap<>();

    public Composite99DocValuesWriter(DocValuesConsumer delegate, SegmentWriteState segmentWriteState, MapperService mapperService)
        throws IOException {

        this.delegate = delegate;
        this.state = segmentWriteState;
        this.mapperService = mapperService;
        this.fieldNumberAcrossCompositeFields = new AtomicInteger();
        this.compositeMappedFieldTypes = mapperService.getCompositeFieldTypes();
        compositeFieldSet = new HashSet<>();
        segmentFieldSet = new HashSet<>();
        // TODO : add integ test for this
        for (FieldInfo fi : this.state.fieldInfos) {
            if (DocValuesType.SORTED_NUMERIC.equals(fi.getDocValuesType())) {
                segmentFieldSet.add(fi.name);
            } else if (fi.name.equals(DocCountFieldMapper.NAME)) {
                segmentFieldSet.add(fi.name);
            }
        }
        for (CompositeMappedFieldType type : compositeMappedFieldTypes) {
            compositeFieldSet.addAll(type.fields());
        }

        boolean success = false;
        try {

            // Get consumer write state with DocIdSetIterator.NO_MORE_DOCS as segment doc count,
            // so that all the fields are sparse numeric doc values and not dense numeric doc values
            SegmentWriteState consumerWriteState = getSegmentWriteState(segmentWriteState);

            this.composite99DocValuesConsumer = LuceneDocValuesConsumerFactory.getDocValuesConsumerForCompositeCodec(
                consumerWriteState,
                Composite99DocValuesFormat.DATA_DOC_VALUES_CODEC,
                Composite99DocValuesFormat.DATA_DOC_VALUES_EXTENSION,
                Composite99DocValuesFormat.META_DOC_VALUES_CODEC,
                Composite99DocValuesFormat.META_DOC_VALUES_EXTENSION
            );

            String dataFileName = IndexFileNames.segmentFileName(
                this.state.segmentInfo.name,
                this.state.segmentSuffix,
                Composite99DocValuesFormat.DATA_EXTENSION
            );
            dataOut = this.state.directory.createOutput(dataFileName, this.state.context);
            CodecUtil.writeIndexHeader(
                dataOut,
                Composite99DocValuesFormat.DATA_CODEC_NAME,
                Composite99DocValuesFormat.VERSION_CURRENT,
                this.state.segmentInfo.getId(),
                this.state.segmentSuffix
            );

            String metaFileName = IndexFileNames.segmentFileName(
                this.state.segmentInfo.name,
                this.state.segmentSuffix,
                Composite99DocValuesFormat.META_EXTENSION
            );
            metaOut = this.state.directory.createOutput(metaFileName, this.state.context);
            CodecUtil.writeIndexHeader(
                metaOut,
                Composite99DocValuesFormat.META_CODEC_NAME,
                Composite99DocValuesFormat.VERSION_CURRENT,
                this.state.segmentInfo.getId(),
                this.state.segmentSuffix
            );

            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(this);
            }
        }
        // check if there are any composite fields which are part of the segment
        // TODO : add integ test where there are no composite fields in a segment, test both flush and merge cases
        segmentHasCompositeFields = Collections.disjoint(segmentFieldSet, compositeFieldSet) == false;
    }

    @Override
    public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegate.addNumericField(field, valuesProducer);
        // Perform this only during flush flow
        if (mergeState.get() == null && segmentHasCompositeFields) {
            createCompositeIndicesIfPossible(valuesProducer, field);
        }
    }

    @Override
    public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegate.addBinaryField(field, valuesProducer);
    }

    @Override
    public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegate.addSortedField(field, valuesProducer);
    }

    @Override
    public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegate.addSortedNumericField(field, valuesProducer);
        // Perform this only during flush flow
        if (mergeState.get() == null && segmentHasCompositeFields) {
            createCompositeIndicesIfPossible(valuesProducer, field);
        }
    }

    @Override
    public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegate.addSortedSetField(field, valuesProducer);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
        boolean success = false;
        try {
            if (metaOut != null) {
                metaOut.writeLong(-1); // write EOF marker
                CodecUtil.writeFooter(metaOut); // write checksum
            }
            if (dataOut != null) {
                CodecUtil.writeFooter(dataOut); // write checksum
            }

            success = true;
        } finally {
            if (success) {
                IOUtils.close(dataOut, metaOut, composite99DocValuesConsumer);
            } else {
                IOUtils.closeWhileHandlingException(dataOut, metaOut, composite99DocValuesConsumer);
            }
            metaOut = dataOut = null;
            composite99DocValuesConsumer = null;
        }
    }

    private void createCompositeIndicesIfPossible(DocValuesProducer valuesProducer, FieldInfo field) throws IOException {
        if (compositeFieldSet.isEmpty()) return;
        if (compositeFieldSet.contains(field.name)) {
            fieldProducerMap.put(field.name, valuesProducer);
            compositeFieldSet.remove(field.name);
        }
        segmentFieldSet.remove(field.name);
        if (segmentFieldSet.isEmpty()) {
            Set<String> compositeFieldSetCopy = new HashSet<>(compositeFieldSet);
            for (String compositeField : compositeFieldSetCopy) {
                addDocValuesForEmptyField(compositeField);
            }
        }
        // we have all the required fields to build composite fields
        if (compositeFieldSet.isEmpty()) {
            for (CompositeMappedFieldType mappedType : compositeMappedFieldTypes) {
                if (mappedType instanceof StarTreeMapper.StarTreeFieldType) {
                    try (StarTreesBuilder starTreesBuilder = new StarTreesBuilder(state, mapperService, fieldNumberAcrossCompositeFields)) {
                        starTreesBuilder.build(metaOut, dataOut, fieldProducerMap, composite99DocValuesConsumer);
                    }
                }
            }
        }
    }

    /**
     * Add empty doc values for fields not present in segment
     */
    private void addDocValuesForEmptyField(String compositeField) {
        if (compositeField.equals(DocCountFieldMapper.NAME)) {
            fieldProducerMap.put(compositeField, new EmptyDocValuesProducer() {
                @Override
                public NumericDocValues getNumeric(FieldInfo field) {
                    return DocValues.emptyNumeric();
                }
            });
        } else {
            fieldProducerMap.put(compositeField, new EmptyDocValuesProducer() {
                @Override
                public SortedNumericDocValues getSortedNumeric(FieldInfo field) {
                    return DocValues.emptySortedNumeric();
                }
            });
        }
        compositeFieldSet.remove(compositeField);
    }

    @Override
    public void merge(MergeState mergeState) throws IOException {
        this.mergeState.compareAndSet(null, mergeState);
        super.merge(mergeState);
        mergeCompositeFields(mergeState);
    }

    /**
     * Merges composite fields from multiple segments
     *
     * @param mergeState merge state
     */
    private void mergeCompositeFields(MergeState mergeState) throws IOException {
        mergeStarTreeFields(mergeState);
    }

    /**
     * Merges star tree data fields from multiple segments
     *
     * @param mergeState merge state
     */
    private void mergeStarTreeFields(MergeState mergeState) throws IOException {
        Map<String, List<StarTreeValues>> starTreeSubsPerField = new HashMap<>();
        StarTreeField starTreeField = null;
        for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
            CompositeIndexReader reader = null;
            if (mergeState.docValuesProducers[i] == null) {
                continue;
            }
            if (mergeState.docValuesProducers[i] instanceof CompositeIndexReader) {
                reader = (CompositeIndexReader) mergeState.docValuesProducers[i];
            } else {
                continue;
            }

            List<CompositeIndexFieldInfo> compositeFieldInfo = reader.getCompositeIndexFields();
            for (CompositeIndexFieldInfo fieldInfo : compositeFieldInfo) {
                if (fieldInfo.getType().equals(CompositeMappedFieldType.CompositeFieldType.STAR_TREE)) {
                    CompositeIndexValues compositeIndexValues = reader.getCompositeIndexValues(fieldInfo);
                    if (compositeIndexValues instanceof StarTreeValues) {
                        StarTreeValues starTreeValues = (StarTreeValues) compositeIndexValues;
                        List<StarTreeValues> fieldsList = starTreeSubsPerField.getOrDefault(fieldInfo.getField(), new ArrayList<>());
                        if (starTreeField == null) {
                            starTreeField = starTreeValues.getStarTreeField();
                        }
                        // assert star tree configuration is same across segments
                        else {
                            if (starTreeField.equals(starTreeValues.getStarTreeField()) == false) {
                                throw new IllegalArgumentException(
                                    "star tree field configuration must match the configuration of the field being merged"
                                );
                            }
                        }
                        fieldsList.add(starTreeValues);
                        starTreeSubsPerField.put(fieldInfo.getField(), fieldsList);
                    }
                }
            }
        }
        try (StarTreesBuilder starTreesBuilder = new StarTreesBuilder(state, mapperService, fieldNumberAcrossCompositeFields)) {
            starTreesBuilder.buildDuringMerge(metaOut, dataOut, starTreeSubsPerField, composite99DocValuesConsumer);
        }
    }

    private static SegmentWriteState getSegmentWriteState(SegmentWriteState segmentWriteState) {

        SegmentInfo segmentInfo = new SegmentInfo(
            segmentWriteState.segmentInfo.dir,
            segmentWriteState.segmentInfo.getVersion(),
            segmentWriteState.segmentInfo.getMinVersion(),
            segmentWriteState.segmentInfo.name,
            DocIdSetIterator.NO_MORE_DOCS,
            segmentWriteState.segmentInfo.getUseCompoundFile(),
            segmentWriteState.segmentInfo.getHasBlocks(),
            segmentWriteState.segmentInfo.getCodec(),
            segmentWriteState.segmentInfo.getDiagnostics(),
            segmentWriteState.segmentInfo.getId(),
            segmentWriteState.segmentInfo.getAttributes(),
            segmentWriteState.segmentInfo.getIndexSort()
        );

        return new SegmentWriteState(
            segmentWriteState.infoStream,
            segmentWriteState.directory,
            segmentInfo,
            segmentWriteState.fieldInfos,
            segmentWriteState.segUpdates,
            segmentWriteState.context
        );
    }

}
