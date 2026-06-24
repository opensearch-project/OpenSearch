/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.composite.composite912;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesProducerUtil;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.EmptyDocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.codec.composite.CompositeIndexReader;
import org.opensearch.index.codec.composite.LuceneDocValuesConsumerFactory;
import org.opensearch.index.compositeindex.datacube.startree.LiveDocsFilteredDocValuesProducer;
import org.opensearch.index.compositeindex.datacube.startree.builder.StarTreesBuilder;
import org.opensearch.index.compositeindex.datacube.startree.index.CompositeIndexValues;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.fielddata.IndexNumericFieldData;
import org.opensearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.opensearch.index.mapper.CompositeMappedFieldType;
import org.opensearch.index.mapper.DocCountFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
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
public class Composite912DocValuesWriter extends DocValuesConsumer {
    private final DocValuesConsumer delegate;
    private final SegmentWriteState state;
    private final MapperService mapperService;
    AtomicReference<MergeState> mergeState = new AtomicReference<>();
    private final Set<CompositeMappedFieldType> compositeMappedFieldTypes;
    private final Set<String> compositeFieldSet;
    private DocValuesConsumer compositeDocValuesConsumer;

    // Pre-built live docs bitset from __soft_deletes, eagerly consumed during addNumericField
    private FixedBitSet mergedLiveDocsBitset;

    public IndexOutput dataOut;
    public IndexOutput metaOut;
    private final Set<String> segmentFieldSet;
    private final boolean segmentHasCompositeFields;
    private final AtomicInteger fieldNumberAcrossCompositeFields;

    private final Map<String, DocValuesProducer> fieldProducerMap = new HashMap<>();
    private final Map<String, DocValuesProducer> mergedFieldProducerMap = new HashMap<>();
    private final Map<String, SortedSetDocValues> fieldDocIdSetIteratorMap = new HashMap<>();

    public Composite912DocValuesWriter(DocValuesConsumer delegate, SegmentWriteState segmentWriteState, MapperService mapperService)
        throws IOException {

        this.delegate = delegate;
        this.state = segmentWriteState;
        this.mapperService = mapperService;
        this.fieldNumberAcrossCompositeFields = new AtomicInteger();
        this.compositeMappedFieldTypes = mapperService.getCompositeFieldTypes();
        compositeFieldSet = new HashSet<>();
        segmentFieldSet = new HashSet<>();
        addStarTreeSupportedFieldsFromSegment();
        for (CompositeMappedFieldType type : compositeMappedFieldTypes) {
            compositeFieldSet.addAll(type.fields());
        }

        boolean success = false;
        try {

            // Get consumer write state with DocIdSetIterator.NO_MORE_DOCS as segment doc count,
            // so that all the fields are sparse numeric doc values and not dense numeric doc values
            SegmentWriteState consumerWriteState = getSegmentWriteState(segmentWriteState);

            this.compositeDocValuesConsumer = LuceneDocValuesConsumerFactory.getDocValuesConsumerForCompositeCodec(
                consumerWriteState,
                4096, /* Lucene90DocValuesFormat#DEFAULT_SKIP_INDEX_INTERVAL_SIZE */
                Composite912DocValuesFormat.DATA_DOC_VALUES_CODEC,
                Composite912DocValuesFormat.DATA_DOC_VALUES_EXTENSION,
                Composite912DocValuesFormat.META_DOC_VALUES_CODEC,
                Composite912DocValuesFormat.META_DOC_VALUES_EXTENSION
            );

            String dataFileName = IndexFileNames.segmentFileName(
                this.state.segmentInfo.name,
                this.state.segmentSuffix,
                Composite912DocValuesFormat.DATA_EXTENSION
            );
            dataOut = this.state.directory.createOutput(dataFileName, this.state.context);
            CodecUtil.writeIndexHeader(
                dataOut,
                Composite912DocValuesFormat.DATA_CODEC_NAME,
                Composite912DocValuesFormat.VERSION_CURRENT,
                this.state.segmentInfo.getId(),
                this.state.segmentSuffix
            );

            String metaFileName = IndexFileNames.segmentFileName(
                this.state.segmentInfo.name,
                this.state.segmentSuffix,
                Composite912DocValuesFormat.META_EXTENSION
            );
            metaOut = this.state.directory.createOutput(metaFileName, this.state.context);
            CodecUtil.writeIndexHeader(
                metaOut,
                Composite912DocValuesFormat.META_CODEC_NAME,
                Composite912DocValuesFormat.VERSION_CURRENT,
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

    private void addStarTreeSupportedFieldsFromSegment() {
        // TODO : add integ test for this
        for (FieldInfo fi : this.state.fieldInfos) {
            if (DocValuesType.SORTED_NUMERIC.equals(fi.getDocValuesType())
                || DocValuesType.SORTED_SET.equals(fi.getDocValuesType())
                || fi.name.equals(DocCountFieldMapper.NAME)) {
                segmentFieldSet.add(fi.name);
            }
        }
    }

    @Override
    public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        // Log all numeric fields during merge to debug __soft_deletes
        if (mergeState.get() != null) {
            System.out.println("[STAR_TREE_MERGE_DEBUG] addNumericField called: " + field.name);
        }
        // Eagerly build live docs bitset from __soft_deletes BEFORE delegate consumes the iterator.
        // The producer is a one-shot anonymous class from Lucene's merge machinery —
        // once delegate.addNumericField() consumes it, getNumeric() returns an exhausted iterator.
        if (mergeState.get() != null && "__soft_deletes".equals(field.name)) {
            try {
                int mergedMaxDoc = this.mergeState.get().segmentInfo.maxDoc();
                NumericDocValues softDelDV = valuesProducer.getNumeric(field);
                if (softDelDV != null && mergedMaxDoc > 0) {
                    FixedBitSet liveBits = new FixedBitSet(mergedMaxDoc);
                    liveBits.set(0, mergedMaxDoc);
                    int doc;
                    int softDelCount = 0;
                    while ((doc = softDelDV.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                        if (softDelDV.longValue() == 1 && doc < mergedMaxDoc) {
                            liveBits.clear(doc);
                            softDelCount++;
                        }
                    }
                    if (liveBits.cardinality() < mergedMaxDoc) {
                        mergedLiveDocsBitset = liveBits;
                    }
                    System.out.println("[STAR_TREE_MERGE_DEBUG] __soft_deletes captured: mergedMaxDoc=" + mergedMaxDoc
                        + " softDelCount=" + softDelCount + " liveCount=" + liveBits.cardinality()
                        + " bitsetSet=" + (mergedLiveDocsBitset != null));
                } else {
                    System.out.println("[STAR_TREE_MERGE_DEBUG] __soft_deletes: softDelDV=" + (softDelDV != null)
                        + " mergedMaxDoc=" + mergedMaxDoc);
                }
            } catch (IOException e) {
                System.out.println("[STAR_TREE_MERGE_DEBUG] __soft_deletes IOException: " + e.getMessage());
            }
        }

        delegate.addNumericField(field, valuesProducer);
        // Perform this only during flush flow
        if (mergeState.get() == null && segmentHasCompositeFields) {
            createCompositeIndicesIfPossible(valuesProducer, field);
        }
        if (mergeState.get() != null) {
            if (compositeFieldSet.contains(field.name)) {
                mergedFieldProducerMap.put(field.name, valuesProducer);
            }
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
        if (mergeState.get() != null) {
            if (compositeFieldSet.contains(field.name)) {
                mergedFieldProducerMap.put(field.name, valuesProducer);
            }
        }
    }

    @Override
    public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegate.addSortedSetField(field, valuesProducer);
        // Perform this only during flush flow
        if (mergeState.get() == null && segmentHasCompositeFields) {
            createCompositeIndicesIfPossible(valuesProducer, field);
        }
        if (mergeState.get() != null) {
            if (compositeFieldSet.contains(field.name)) {
                mergedFieldProducerMap.put(field.name, valuesProducer);
                fieldDocIdSetIteratorMap.put(field.name, valuesProducer.getSortedSet(field));
            }
        }
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
                IOUtils.close(dataOut, metaOut, compositeDocValuesConsumer);
            } else {
                IOUtils.closeWhileHandlingException(dataOut, metaOut, compositeDocValuesConsumer);
            }
            metaOut = dataOut = null;
            compositeDocValuesConsumer = null;
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
            try (StarTreesBuilder starTreesBuilder = new StarTreesBuilder(state, mapperService, fieldNumberAcrossCompositeFields)) {
                starTreesBuilder.build(metaOut, dataOut, fieldProducerMap, compositeDocValuesConsumer);
            }
        }
    }

    /**
     * Add empty doc values for fields not present in segment
     */
    private void addDocValuesForEmptyField(String compositeField) {
        // special case for doc count
        if (compositeField.equals(DocCountFieldMapper.NAME)) {
            fieldProducerMap.put(compositeField, new EmptyDocValuesProducer() {
                @Override
                public NumericDocValues getNumeric(FieldInfo field) {
                    return DocValues.emptyNumeric();
                }
            });
        } else {
            if (isSortedSetField(compositeField)) {
                fieldProducerMap.put(compositeField, new EmptyDocValuesProducer() {
                    @Override
                    public SortedSetDocValues getSortedSet(FieldInfo field) {
                        return DocValues.emptySortedSet();
                    }
                });
            } else if (isSortedNumericField(compositeField)) {
                fieldProducerMap.put(compositeField, new EmptyDocValuesProducer() {
                    @Override
                    public SortedNumericDocValues getSortedNumeric(FieldInfo field) {
                        return DocValues.emptySortedNumeric();
                    }
                });
            } else {
                throw new IllegalStateException(
                    String.format(Locale.ROOT, "Unsupported DocValues field associated with the composite field : %s", compositeField)
                );
            }
        }
        compositeFieldSet.remove(compositeField);
    }

    private boolean isSortedSetField(String field) {
        MappedFieldType ft = mapperService.fieldType(field);
        assert ft.isAggregatable();
        return ft.fielddataBuilder(
            "",
            () -> { throw new UnsupportedOperationException("SearchLookup not available"); }
        ) instanceof SortedSetOrdinalsIndexFieldData.Builder;
    }

    private boolean isSortedNumericField(String field) {
        MappedFieldType ft = mapperService.fieldType(field);
        assert ft.isAggregatable();
        return ft.fielddataBuilder(
            "",
            () -> { throw new UnsupportedOperationException("SearchLookup not available"); }
        ) instanceof IndexNumericFieldData.Builder;
    }

    @Override
    public void merge(MergeState mergeState) throws IOException {
        this.mergeState.compareAndSet(null, mergeState);
        try {
            java.nio.file.Files.writeString(java.nio.file.Path.of("/tmp/merge_debug.log"),
                "[MERGE_DEBUG] merge() called maxDoc=" + mergeState.segmentInfo.maxDoc()
                + " producers=" + mergeState.docValuesProducers.length + "\n",
                java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND);
        } catch (Exception ignored) {}
        super.merge(mergeState);
        try {
            java.nio.file.Files.writeString(java.nio.file.Path.of("/tmp/merge_debug.log"),
                "[MERGE_DEBUG] after super.merge: mergedFieldProducerMap.size=" + mergedFieldProducerMap.size()
                + " mergedLiveDocsBitset=" + (mergedLiveDocsBitset != null ? "card=" + mergedLiveDocsBitset.cardinality() : "null") + "\n",
                java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND);
        } catch (Exception ignored) {}
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
        try {
            StringBuilder sb = new StringBuilder("[MERGE_DEBUG] mergeStarTreeFields: " + mergeState.docValuesProducers.length + " producers\n");
            for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
                sb.append("  source[").append(i).append("] class=")
                    .append(mergeState.docValuesProducers[i] == null ? "null" : mergeState.docValuesProducers[i].getClass().getName())
                    .append(" isComposite=").append(mergeState.docValuesProducers[i] instanceof CompositeIndexReader).append("\n");
            }
            java.nio.file.Files.writeString(java.nio.file.Path.of("/tmp/merge_debug.log"), sb.toString(),
                java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND);
        } catch (Exception ignored) {}
        for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
            CompositeIndexReader reader = null;
            if (mergeState.docValuesProducers[i] == null) {
                continue;
            }
            if (mergeState.docValuesProducers[i] instanceof CompositeIndexReader compositeReader) {
                reader = compositeReader;
            } else {
                Set<DocValuesProducer> docValuesProducers = DocValuesProducerUtil.getSegmentDocValuesProducers(
                    mergeState.docValuesProducers[i]
                );
                for (DocValuesProducer docValuesProducer : docValuesProducers) {
                    if (docValuesProducer instanceof CompositeIndexReader compositeReader) {
                        reader = compositeReader;
                        List<CompositeIndexFieldInfo> compositeFieldInfo = reader.getCompositeIndexFields();
                        if (compositeFieldInfo.isEmpty() == false) {
                            break;
                        }
                    }
                }
            }
            if (reader == null) continue;
            List<CompositeIndexFieldInfo> compositeFieldInfo = reader.getCompositeIndexFields();
            for (CompositeIndexFieldInfo fieldInfo : compositeFieldInfo) {
                if (fieldInfo.getType().equals(CompositeMappedFieldType.CompositeFieldType.STAR_TREE)) {
                    CompositeIndexValues compositeIndexValues = reader.getCompositeIndexValues(fieldInfo);
                    if (compositeIndexValues instanceof StarTreeValues starTreeValues) {
                        List<StarTreeValues> fieldsList = starTreeSubsPerField.getOrDefault(fieldInfo.getField(), new ArrayList<>());
                        fieldsList.add(starTreeValues);
                        starTreeSubsPerField.put(fieldInfo.getField(), fieldsList);
                    }
                }
            }
        }
        // Count eligible source segments (those with non-null doc values producers)
        int eligibleSourceSegments = 0;
        for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
            if (mergeState.docValuesProducers[i] != null) {
                eligibleSourceSegments++;
            }
        }

        // Check if ALL eligible source segments contributed star tree values.
        // If only SOME segments have star tree data (hybrid upgrade case), we must
        // fall back to building from raw merged doc values to include all segments.
        boolean allSegmentsHaveStarTree = false;
        if (starTreeSubsPerField.isEmpty() == false) {
            allSegmentsHaveStarTree = true;
            for (CompositeMappedFieldType type : compositeMappedFieldTypes) {
                List<StarTreeValues> values = starTreeSubsPerField.get(type.name());
                if (values == null || values.size() < eligibleSourceSegments) {
                    allSegmentsHaveStarTree = false;
                    break;
                }
            }
        }

        if (allSegmentsHaveStarTree) {
            // All source segments have star tree data — merge them directly
            try (StarTreesBuilder starTreesBuilder = new StarTreesBuilder(state, mapperService, fieldNumberAcrossCompositeFields)) {
                starTreesBuilder.buildDuringMerge(metaOut, dataOut, starTreeSubsPerField, compositeDocValuesConsumer);
            }
        } else if (compositeMappedFieldTypes.isEmpty() == false) {
            // Some or all source segments lack star tree data (hybrid upgrade case).
            // Fall back to building from raw merged doc values — same as flush path.
            boolean hasAllCompositeFields = true;
            for (CompositeMappedFieldType type : compositeMappedFieldTypes) {
                for (String field : type.fields()) {
                    if (field.equals(DocCountFieldMapper.NAME)) {
                        continue;
                    }
                    if (mergedFieldProducerMap.containsKey(field) == false) {
                        hasAllCompositeFields = false;
                        break;
                    }
                }
                if (hasAllCompositeFields == false) break;
            }

            if (hasAllCompositeFields) {
                Map<String, DocValuesProducer> fieldProducerMapForMerge = buildFieldProducerMapFromMergeState(mergeState);
                FixedBitSet mergedLiveDocs = mergedLiveDocsBitset;
                System.out.println("[STAR_TREE_MERGE_DEBUG] mergeStarTreeFields fallback: mergedLiveDocs="
                    + (mergedLiveDocs != null ? "cardinality=" + mergedLiveDocs.cardinality() : "null")
                    + " fieldCount=" + fieldProducerMapForMerge.size());
                Map<String, DocValuesProducer> buildProducerMap;
                if (mergedLiveDocs != null) {
                    buildProducerMap = new HashMap<>();
                    for (Map.Entry<String, DocValuesProducer> entry : fieldProducerMapForMerge.entrySet()) {
                        buildProducerMap.put(entry.getKey(), new LiveDocsFilteredDocValuesProducer(entry.getValue(), mergedLiveDocs));
                    }
                } else {
                    buildProducerMap = fieldProducerMapForMerge;
                }
                try (StarTreesBuilder starTreesBuilder = new StarTreesBuilder(state, mapperService, fieldNumberAcrossCompositeFields)) {
                    starTreesBuilder.build(metaOut, dataOut, buildProducerMap, compositeDocValuesConsumer);
                }
            }
        }
    }

    /**
     * Builds a fieldProducerMap from the merged segment's doc values for use with the
     * flush-path star tree builder. Uses the doc values producers captured during
     * {@code super.merge()} and adds empty producers for any composite fields not
     * present in the merged segment, following the same pattern as
     * {@link #addDocValuesForEmptyField(String)}.
     *
     * @param mergeState the merge state containing doc values producers for the merged segment
     * @return a map of field name to DocValuesProducer
     */
    private Map<String, DocValuesProducer> buildFieldProducerMapFromMergeState(MergeState mergeState) {
        Map<String, DocValuesProducer> resultMap = new HashMap<>(mergedFieldProducerMap);

        // For any composite fields not present in the merged segment, add empty producers
        for (CompositeMappedFieldType type : compositeMappedFieldTypes) {
            for (String field : type.fields()) {
                if (resultMap.containsKey(field) == false) {
                    addDocValuesForEmptyField(field);
                    resultMap.put(field, fieldProducerMap.get(field));
                }
            }
        }

        return resultMap;
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
            segmentWriteState.context,
            segmentWriteState.segmentSuffix
        );
    }
}
