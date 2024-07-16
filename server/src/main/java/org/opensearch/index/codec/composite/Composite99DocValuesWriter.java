/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.composite;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.codec.composite.datacube.startree.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.builder.StarTreesBuilder;
import org.opensearch.index.mapper.CompositeMappedFieldType;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

    private final Map<String, DocValuesProducer> fieldProducerMap = new HashMap<>();
    private static final Logger logger = LogManager.getLogger(Composite99DocValuesWriter.class);

    public Composite99DocValuesWriter(DocValuesConsumer delegate, SegmentWriteState segmentWriteState, MapperService mapperService) {

        this.delegate = delegate;
        this.state = segmentWriteState;
        this.mapperService = mapperService;
        this.compositeMappedFieldTypes = mapperService.getCompositeFieldTypes();
        compositeFieldSet = new HashSet<>();
        for (CompositeMappedFieldType type : compositeMappedFieldTypes) {
            compositeFieldSet.addAll(type.fields());
        }
    }

    @Override
    public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegate.addNumericField(field, valuesProducer);
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
        if (mergeState.get() == null) {
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
    }

    private void createCompositeIndicesIfPossible(DocValuesProducer valuesProducer, FieldInfo field) throws IOException {
        if (compositeFieldSet.isEmpty()) return;
        if (compositeFieldSet.contains(field.name)) {
            fieldProducerMap.put(field.name, valuesProducer);
            compositeFieldSet.remove(field.name);
        }
        // we have all the required fields to build composite fields
        if (compositeFieldSet.isEmpty()) {
            for (CompositeMappedFieldType mappedType : compositeMappedFieldTypes) {
                if (mappedType.getCompositeIndexType().equals(CompositeMappedFieldType.CompositeFieldType.STAR_TREE)) {
                    try (StarTreesBuilder starTreesBuilder = new StarTreesBuilder(state, mapperService)) {
                        starTreesBuilder.build(fieldProducerMap);
                    }
                }
            }
        }
    }

    @Override
    public void merge(MergeState mergeState) throws IOException {
        this.mergeState.compareAndSet(null, mergeState);
        super.merge(mergeState);
        mergeCompositeFields(mergeState);
    }

    /**
     * Merges composite fields from multiple segments
     * @param mergeState merge state
     */
    private void mergeCompositeFields(MergeState mergeState) throws IOException {
        mergeStarTreeFields(mergeState);
    }

    /**
     * Merges star tree data fields from multiple segments
     * @param mergeState merge state
     */
    private void mergeStarTreeFields(MergeState mergeState) throws IOException {
        Map<String, List<StarTreeValues>> starTreeSubsPerField = new HashMap<>();
        Map<String, StarTreeField> starTreeFieldMap = new HashMap<>();
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
                        List<StarTreeValues> fieldsList = starTreeSubsPerField.getOrDefault(fieldInfo.getField(), Collections.emptyList());

                        if (!starTreeFieldMap.containsKey(fieldInfo.getField())) {
                            starTreeFieldMap.put(fieldInfo.getField(), ((StarTreeValues) compositeIndexValues).getStarTreeField());
                        }
                        // assert star tree configuration is same across segments
                        else {
                            assert starTreeFieldMap.get(fieldInfo.getField())
                                .equals(((StarTreeValues) compositeIndexValues).getStarTreeField());
                            logger.error("Star tree configuration is not same for segments during merge");
                        }
                        fieldsList.add((StarTreeValues) compositeIndexValues);
                        starTreeSubsPerField.put(fieldInfo.getField(), fieldsList);
                    }
                }
            }
        }
        final StarTreesBuilder starTreesBuilder = new StarTreesBuilder(state, mapperService);
        starTreesBuilder.buildDuringMerge(starTreeFieldMap, starTreeSubsPerField);
    }
}
