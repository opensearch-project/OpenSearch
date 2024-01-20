/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opensearch.index.codec.freshstartree.builder;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.IOUtils;
import org.opensearch.index.codec.freshstartree.codec.StarTreeAggregatedValues;
import org.opensearch.index.codec.freshstartree.util.QuickSorter;


/**
 * Off heap implementation of star tree builder Segment records are sorted and aggregated completely
 * off heap Star tree records are using mixed approach where we have a buffer of hashmap to doc ids
 * and also a temp file This is done since star tree records file needs to be read and written at
 * same time, sometimes latest changes are not present during read
 */
public class OffHeapBufferedSingleTreeBuilder extends BaseSingleTreeBuilder {
    private static final String SEGMENT_RECORD_FILE_NAME = "segment.record";
    private static final String STAR_TREE_RECORD_FILE_NAME = "star-tree.record";

    private final List<Long> _starTreeRecordOffsets;

    private int _numReadableStarTreeRecords;

    IndexOutput segmentRecordFileOutput;
    IndexOutput starTreeRecordFileOutput;
    RandomAccessInput segmentRandomInput;
    private RandomAccessInput starTreeRecordRandomInput;

    SegmentWriteState state;

    long currBytes = 0;
    Map<String, Integer> fileToByteSizeMap;
    int starTreeFileCount = 0;
    int  currentStarTreeFileIndex = 0;
    int prevStartDocId = Integer.MAX_VALUE;

    public OffHeapBufferedSingleTreeBuilder(IndexOutput output, List<String> dimensionsSplitOrder,
        Map<String, SortedNumericDocValues> docValuesMap, int maxDoc, DocValuesConsumer consumer,
        SegmentWriteState state)
        throws IOException {
        super(output, dimensionsSplitOrder, docValuesMap, maxDoc, consumer, state);
        this.state = state;
        fileToByteSizeMap = new LinkedHashMap<>(); // maintain order

        // TODO : how to set this dynammically
        String segmentRecordFileName =
            IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, SEGMENT_RECORD_FILE_NAME);
        String starTreeRecordFileName =
            IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, STAR_TREE_RECORD_FILE_NAME) +
                "_" + starTreeFileCount;

        // TODO : create temp output
        starTreeRecordFileOutput = state.directory.createOutput(starTreeRecordFileName, state.context);
        starTreeFileCount++;
        //    CodecUtil.writeIndexHeader(
        //        starTreeRecordFileOutput,
        //        "STARTreeCodec",
        //        0,
        //        state.segmentInfo.getId(),
        //        state.segmentSuffix);
        segmentRecordFileOutput = state.directory.createOutput(segmentRecordFileName, state.context);

        _starTreeRecordOffsets = new ArrayList<>();
        //_starTreeRecordOffsets.add(0L);
    }

    @Override
    public void build(List<StarTreeAggregatedValues> aggrList)
        throws IOException {
        build(mergeRecords(aggrList), true);
    }

    private Iterator<Record> mergeRecords(List<StarTreeAggregatedValues> aggrList)
        throws IOException {
        int recordBytesLength = 0;
        int numDocs = 0;
        Integer[] sortedDocIds;
        try {
            for (StarTreeAggregatedValues starTree : aggrList) {
                boolean endOfDoc = false;
                while (!endOfDoc) {
                    long[] dims = new long[starTree.dimensionValues.size()];
                    int i = 0;
                    for (Map.Entry<String, NumericDocValues> dimValue : starTree.dimensionValues.entrySet()) {
                        endOfDoc = dimValue.getValue().nextDoc() == DocIdSetIterator.NO_MORE_DOCS
                            || dimValue.getValue().longValue() == -1;
                        if (endOfDoc) {
                            break;
                        }
                        long val = dimValue.getValue().longValue();
                        dims[i] = val;
                        i++;
                    }
                    if (endOfDoc) {
                        break;
                    }
                    i = 0;
                    Object[] metrics = new Object[starTree.metricValues.size()];
                    for (Map.Entry<String, NumericDocValues> metricValue : starTree.metricValues.entrySet()) {
                        metricValue.getValue().nextDoc();
                        metrics[i] = metricValue.getValue().longValue();
                        i++;
                    }
                    Record record = new Record(dims, metrics);
                    byte[] bytes = serializeStarTreeRecord(record);
                    numDocs++;
                    recordBytesLength = bytes.length;
                    segmentRecordFileOutput.writeBytes(bytes, bytes.length);
                }
            }
            sortedDocIds = new Integer[numDocs];
            for (int i = 0; i < numDocs; i++) {
                sortedDocIds[i] = i;
            }
        } finally {
            segmentRecordFileOutput.close();
        }

        if(numDocs == 0) return new ArrayList<Record>().iterator();

        return sortRecords(sortedDocIds, numDocs, recordBytesLength);
    }

    private byte[] serializeStarTreeRecord(Record starTreeRecord) {
        int numBytes = _numDimensions * Long.BYTES;
        for (int i = 0; i < _numMetrics; i++) {
            switch (_valueAggregators[i].getAggregatedValueType()) {
                case LONG:
                    numBytes += Long.BYTES;
                    break;
                case DOUBLE:
                    numBytes += Double.BYTES;
                    break;
                case FLOAT:
                case INT:
                default:
                    throw new IllegalStateException();
            }
        }
        byte[] bytes = new byte[numBytes];
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder());
        for (long dimension : starTreeRecord._dimensions) {
            byteBuffer.putLong(dimension);
        }
        for (int i = 0; i < _numMetrics; i++) {
            switch (_valueAggregators[i].getAggregatedValueType()) {
                case LONG:
                    if (starTreeRecord._metrics[i] != null) {
                        byteBuffer.putLong((Long) starTreeRecord._metrics[i]);
                    }
                    break;
                case DOUBLE:
                    //          byteBuffer.putDouble((Double) starTreeRecord._metrics[i]);
                    //          break;
                case INT:
                case FLOAT:
                default:
                    throw new IllegalStateException();
            }
        }
        return bytes;
    }

    private Record deserializeStarTreeRecord(RandomAccessInput buffer, long offset)
        throws IOException {
        long[] dimensions = new long[_numDimensions];
        for (int i = 0; i < _numDimensions; i++) {
            dimensions[i] = buffer.readLong(offset);
            offset += Long.BYTES;
        }
        Object[] metrics = new Object[_numMetrics];
        for (int i = 0; i < _numMetrics; i++) {
            switch (_valueAggregators[i].getAggregatedValueType()) {
                case LONG:
                    metrics[i] = buffer.readLong(offset);
                    offset += Long.BYTES;
                    break;
                case DOUBLE:
                    // TODO : handle double
                    //          metrics[i] = buffer.getDouble((int) offset);
                    //          offset += Double.BYTES;
                    break;
                case FLOAT:
                case INT:
                default:
                    throw new IllegalStateException();
            }
        }
        return new Record(dimensions, metrics);
    }

    //  public void copyTo(ByteBuffer byteBuffer, long offset, byte[] buffer) {
    //    copyTo(offset, byteBuffer, 0, buffer.length);
    //  }

    @Override
    void appendRecord(Record record)
        throws IOException {
        byte[] bytes = serializeStarTreeRecord(record);
        // System.out.println("Appending record : " + record.toString());
        starTreeRecordFileOutput.writeBytes(bytes, bytes.length);
        //System.out.println("Appending doc : " + _numDocs + "curr bytes : " + currBytes + " offset: " + _starTreeRecordOffsets.size());
        _starTreeRecordOffsets.add(currBytes);
        currBytes += bytes.length;
    }

    @Override
    Record getStarTreeRecord(int docId)
        throws IOException {
        ensureBufferReadable(docId);
        //System.out.println("Want star record of id : " + docId);
        return deserializeStarTreeRecord(starTreeRecordRandomInput, _starTreeRecordOffsets.get(docId));
    }

    @Override
    long getDimensionValue(int docId, int dimensionId)
        throws IOException {
        // System.out.println("doc id : " + docId + " _numReadableStarTreeRecords : " +
        // _numReadableStarTreeRecords);
        //System.out.println("Want dimension value record of id : " + docId);
        ensureBufferReadable(docId, false, true);
        // System.out.println("want offset : " + (_starTreeRecordOffsets.get(docId) + (dimensionId *
        // Integer.BYTES)));
        return starTreeRecordRandomInput.readLong(
             (_starTreeRecordOffsets.get(docId) + (dimensionId * Long.BYTES)));
    }

    @Override
    Iterator<Record> sortAndAggregateSegmentRecords(int numDocs)
        throws IOException {
        // Write all dimensions for segment records into the buffer, and sort all records using an int
        // array
        // PinotDataBuffer dataBuffer;
        // long bufferSize = (long) numDocs * _numDimensions * Integer.BYTES;
        int recordBytesLength = 0;
        Integer[] sortedDocIds = new Integer[numDocs];
        for (int i = 0; i < numDocs; i++) {
            sortedDocIds[i] = i;
        }

        try {
            for (int i = 0; i < numDocs; i++) {
                Record record = getNextSegmentRecord();
                byte[] bytes = serializeStarTreeRecord(record);
                recordBytesLength = bytes.length;
                segmentRecordFileOutput.writeBytes(bytes, bytes.length);
            }
        } finally {
            segmentRecordFileOutput.close();
        }

        // Create an iterator for aggregated records
        return sortRecords(sortedDocIds, numDocs, recordBytesLength);
    }

    private Iterator<Record> sortRecords(Integer[] sortedDocIds, int numDocs, int recordBytesLength)
        throws IOException {
        IndexInput segmentRecordFileInput = state.directory.openInput(
            IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, SEGMENT_RECORD_FILE_NAME),
            state.context);
        final long recordBytes = recordBytesLength;
        segmentRandomInput = segmentRecordFileInput.randomAccessSlice(0, segmentRecordFileInput.length());

        try {
            // ArrayUtil.introSort(sortedDocIds, comparator);
            // Arrays.sort(sortedDocIds, comparator);

            QuickSorter.quickSort(0, numDocs, (i1, i2) -> {
                long offset1 = (long) sortedDocIds[i1] * recordBytes;
                long offset2 = (long) sortedDocIds[i2] * recordBytes;
                for (int i = 0; i < _numDimensions; i++) {
                    try {
                        long dimension1 = segmentRandomInput.readLong(offset1 + i * Long.BYTES);
                        long dimension2 = segmentRandomInput.readLong(offset2 + i * Long.BYTES);
                        if (dimension1 != dimension2) {
                            return Long.compare(dimension1, dimension2);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e); // TODO: handle this better
                    }
                }
                return 0;
            }, (i1, i2) -> {
                int temp = sortedDocIds[i1];
                sortedDocIds[i1] = sortedDocIds[i2];
                sortedDocIds[i2] = temp;
            });

            // System.out.println("Sorted doc ids : " + Arrays.toString(sortedDocIds));
        } finally {
            // segmentRecordFileInput.close();
            // state.directory.deleteFile(IndexFileNames.segmentFileName(state.segmentInfo.name,
            // state.segmentSuffix,
            //     SEGMENT_RECORD_FILE_NAME));
            // Files.deleteIfExists(new Path(IndexFileNames.segmentFileName(state.segmentInfo.name,
            // state.segmentSuffix,
            // SEGMENT_RECORD_FILE_NAME)));
        }
        if(sortedDocIds != null)
            System.out.println("Sorted doc ids length" + sortedDocIds.length);
        else
            System.out.println("Sorted doc ids array is null");

        // Create an iterator for aggregated records
        return new Iterator<Record>() {
            boolean _hasNext = true;
            Record _currentRecord = getSegmentRecord(sortedDocIds[0], recordBytes);
            int _docId = 1;

            @Override
            public boolean hasNext() {
                return _hasNext;
            }

            @Override
            public Record next() {
                Record next = mergeSegmentRecord(null, _currentRecord);
                while (_docId < numDocs) {
                    Record record = null;
                    try {
                        record = getSegmentRecord(sortedDocIds[_docId++], recordBytes);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                        // TODO : handle this block better - how to handle exceptions ?
                    }
                    if (!Arrays.equals(record._dimensions, next._dimensions)) {
                        _currentRecord = record;
                        return next;
                    } else {
                        next = mergeSegmentRecord(next, record);
                    }
                }
                _hasNext = false;
                return next;
            }
        };
    }

    public Record getSegmentRecord(int docID, long recordBytes)
        throws IOException {
        return deserializeStarTreeRecord(segmentRandomInput, docID * recordBytes);
    }

    @Override
    Iterator<Record> generateRecordsForStarNode(int startDocId, int endDocId, int dimensionId)
        throws IOException {
        //System.out.println("End doc id " + endDocId);
        ensureBufferReadable(endDocId, true);

        // Sort all records using an int array
        int numDocs = endDocId - startDocId;
        int[] sortedDocIds = new int[numDocs];
        for (int i = 0; i < numDocs; i++) {
            sortedDocIds[i] = startDocId + i;
        }
        QuickSorter.quickSort(0, numDocs, (i1, i2) -> {

            long offset1 = _starTreeRecordOffsets.get(sortedDocIds[i1]);
            long offset2 = _starTreeRecordOffsets.get(sortedDocIds[i2]);
            for (int i = dimensionId + 1; i < _numDimensions; i++) {
                try {
                    long dimension1 = starTreeRecordRandomInput.readLong(offset1 + i * Long.BYTES);
                    long dimension2 = starTreeRecordRandomInput.readLong(offset2 + i * Long.BYTES);
                    if (dimension1 != dimension2) {
                        return Long.compare(dimension1, dimension2);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e); // TODO : do better handling
                }
            }

            return 0;
        }, (i1, i2) -> {
            int temp = sortedDocIds[i1];
            sortedDocIds[i1] = sortedDocIds[i2];
            sortedDocIds[i2] = temp;
        });

        // Create an iterator for aggregated records
        return new Iterator<Record>() {
            boolean _hasNext = true;
            Record _currentRecord = getStarTreeRecord(sortedDocIds[0]);
            int _docId = 1;

            private boolean hasSameDimensions(Record record1, Record record2) {
                for (int i = dimensionId + 1; i < _numDimensions; i++) {
                    if (record1._dimensions[i] != record2._dimensions[i]) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public boolean hasNext() {
                return _hasNext;
            }

            @Override
            public Record next() {
                Record next = mergeStarTreeRecord(null, _currentRecord);
                next._dimensions[dimensionId] = STAR_IN_DOC_VALUES_INDEX;
                while (_docId < numDocs) {
                    Record record;
                    try {
                        record = getStarTreeRecord(sortedDocIds[_docId++]);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    if (!hasSameDimensions(record, _currentRecord)) {
                        _currentRecord = record;
                        return next;
                    } else {
                        next = mergeStarTreeRecord(next, record);
                    }
                }
                _hasNext = false;
                return next;
            }
        };
    }

    private void ensureBufferReadable(int docId) throws IOException {
        ensureBufferReadable(docId, false);
    }

    private void ensureBufferReadable(int docId, boolean endDoc) throws IOException {
        ensureBufferReadable(docId, endDoc, false);
    }

    private void ensureBufferReadable(int docId, boolean endDocCheck, boolean dimIdCheck)
        throws IOException {

        if (docId >= prevStartDocId && (( endDocCheck && docId <= _numReadableStarTreeRecords )
            || (!endDocCheck && docId < _numReadableStarTreeRecords)) ) {
            return;
        }
        //System.out.println("want doc : " + docId + " dim : " + dimIdCheck);
        IndexInput in = null;
        if(docId < _numDocs ) {
            try {
                int prevStartDocId = 0;
                for(Map.Entry<String, Integer> entry : fileToByteSizeMap.entrySet()) {
                    if(docId < entry.getValue() - 1) {
                        in = state.directory.openInput(entry.getKey(), state.context);
                        starTreeRecordRandomInput =
                            in.randomAccessSlice(in.getFilePointer(), in.length() - in.getFilePointer());
                        _numReadableStarTreeRecords = entry.getValue();
                        break;
                    }
                    prevStartDocId = entry.getValue();
                }
                //System.out.println("First loop Current start : " + prevStartDocId + " - Current end : " + _numReadableStarTreeRecords);
                this.prevStartDocId = prevStartDocId;
            } finally {
                //        if (in != null) {
                //          in.close();
                //        }
            }
        }

        if(in != null) return;



        //System.out.println("want doc 1 : " + docId + " num docs : " + _numDocs);
        fileToByteSizeMap.put(starTreeRecordFileOutput.getName(),
            _numDocs);

        //System.out.println("Star tree record file size : " + starTreeRecordFileOutput.getFilePointer());
        //System.out.println("Star tree record file name : " + starTreeRecordFileOutput.getName());

        starTreeRecordFileOutput.close();

        String starTreeRecordFileName =
            IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, STAR_TREE_RECORD_FILE_NAME) +
                "_" + starTreeFileCount;

        // TODO : create temp output
        starTreeRecordFileOutput = state.directory.createOutput(starTreeRecordFileName, state.context);
        starTreeFileCount++;

        currBytes = 0;
        // state.directory.sync(Collections.singleton(starTreeRecordFileOutput.getName()));
        if (starTreeRecordRandomInput != null) {
            starTreeRecordRandomInput = null;
        }

        try {
            int prevStartDocId = 0;
            for(Map.Entry<String, Integer> entry : fileToByteSizeMap.entrySet()) {
                if(docId <= entry.getValue() - 1) {
                    in = state.directory.openInput(entry.getKey(), state.context);
                    starTreeRecordRandomInput =
                        in.randomAccessSlice(in.getFilePointer(), in.length() - in.getFilePointer());
                    _numReadableStarTreeRecords = entry.getValue();
                    break;
                }
                //System.out.println("Setting start value : " + entry.getValue());
                prevStartDocId = entry.getValue();
            }
            //System.out.println("Current start : " + prevStartDocId + " - Current end : " + _numReadableStarTreeRecords);
            this.prevStartDocId = prevStartDocId;
        } finally {
            //        if (in != null) {
            //          in.close();
            //        }
        }

    }

    @Override
    public void close()
        throws IOException {
        boolean success = false;
        try {
            if (starTreeRecordFileOutput != null) {
                IOUtils.deleteFilesIgnoringExceptions(state.directory, starTreeRecordFileOutput.getName());
            }
            success = true;
        } catch (Exception e) {
            throw new RuntimeException(e);
            //      System.out.println(e.getMessage());
        } finally {
            if (success) {
                IOUtils.close(starTreeRecordFileOutput);
            } else {
                IOUtils.closeWhileHandlingException(starTreeRecordFileOutput);
            }
            // starTreeRecordFileOutput = null;
        }
        IOUtils.deleteFilesIgnoringExceptions(state.directory, segmentRecordFileOutput.getName());
        IOUtils.deleteFilesIgnoringExceptions(state.directory, fileToByteSizeMap.keySet());
        super.close();
    }
}
