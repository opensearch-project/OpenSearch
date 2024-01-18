///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package org.opensearch.index.codec.freshstartree.builder;
//
//import java.io.IOException;
//import java.nio.ByteBuffer;
//import java.nio.ByteOrder;
//import java.nio.file.Path;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//import org.apache.lucene.codecs.DocValuesConsumer;
//import org.apache.lucene.index.IndexFileNames;
//import org.apache.lucene.index.SegmentWriteState;
//import org.apache.lucene.index.SortedNumericDocValues;
//import org.apache.lucene.store.IndexInput;
//import org.apache.lucene.store.IndexOutput;
//import org.apache.lucene.store.RandomAccessInput;
//import org.apache.lucene.util.IOUtils;
//import org.opensearch.index.codec.freshstartree.codec.StarTreeAggregatedValues;
//import org.opensearch.index.codec.freshstartree.util.QuickSorter;
//
//
///**
// * Sorting and aggregating segment records is done off heap and star records processing is done on
// * heap
// */
//public class MixedHeapTemporaryFileSingleTreeBuilder extends BaseSingleTreeBuilder {
//    private static final String SEGMENT_RECORD_FILE_NAME = "segment.record";
//    private final List<Record> _records = new ArrayList<>();
//    private final List<Long> _segmentRecordOffsets;
//
//    IndexOutput segmentRecordFileOutput;
//    RandomAccessInput segmentRandomInput;
//
//    SegmentWriteState state;
//
//    public MixedHeapTemporaryFileSingleTreeBuilder(IndexOutput output, List<String> dimensionsSplitOrder,
//        Map<String, SortedNumericDocValues> docValuesMap, int maxDoc, DocValuesConsumer consumer,
//        SegmentWriteState state)
//        throws IOException {
//        super(output, dimensionsSplitOrder, docValuesMap, maxDoc, consumer, state);
//        this.state = state;
//        // TODO : how to set this dynammically
//        //    CodecUtil.writeIndexHeader(
//        //        starTreeRecordFileOutput,
//        //        "STARTreeCodec",
//        //        0,
//        //        state.segmentInfo.getId(),
//        //        state.segmentSuffix);
//        segmentRecordFileOutput = state.directory.createTempOutput("segmentrecord", "sort", state.context);
//
//        _segmentRecordOffsets = new ArrayList<>();
//        _segmentRecordOffsets.add(0L);
//    }
//
//    @Override
//    public void build(List<StarTreeAggregatedValues> aggrList)
//        throws IOException {
//
//    }
//
//    private byte[] serializeStarTreeRecord(Record starTreeRecord) {
//        int numBytes = _numDimensions * Integer.BYTES;
//        for (int i = 0; i < _numMetrics; i++) {
//            switch (_valueAggregators[i].getAggregatedValueType()) {
//                case LONG:
//                    numBytes += Long.BYTES;
//                    break;
//                case DOUBLE:
//                case INT:
//                case FLOAT:
//                    // numBytes += Double.BYTES;
//                    // break;
//                default:
//                    throw new IllegalStateException();
//            }
//        }
//        byte[] bytes = new byte[numBytes];
//        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder());
//        for (int dimension : starTreeRecord._dimensions) {
//            byteBuffer.putInt(dimension);
//        }
//        for (int i = 0; i < _numMetrics; i++) {
//            switch (_valueAggregators[i].getAggregatedValueType()) {
//                case LONG:
//                    byteBuffer.putLong((Long) starTreeRecord._metrics[i]);
//                    break;
//                case DOUBLE:
//                    //          byteBuffer.putDouble((Double) starTreeRecord._metrics[i]);
//                    //          break;
//                case INT:
//                case FLOAT:
//                default:
//                    throw new IllegalStateException();
//            }
//        }
//        return bytes;
//    }
//
//    private Record deserializeStarTreeRecord(RandomAccessInput buffer, long offset)
//        throws IOException {
//        int[] dimensions = new int[_numDimensions];
//        for (int i = 0; i < _numDimensions; i++) {
//            dimensions[i] = buffer.readInt(offset);
//            offset += Integer.BYTES;
//        }
//        Object[] metrics = new Object[_numMetrics];
//        for (int i = 0; i < _numMetrics; i++) {
//            switch (_valueAggregators[i].getAggregatedValueType()) {
//                case LONG:
//                    metrics[i] = buffer.readLong(offset);
//                    offset += Long.BYTES;
//                    break;
//                case DOUBLE:
//                    // TODO : handle double
//                    //          metrics[i] = buffer.getDouble((int) offset);
//                    //          offset += Double.BYTES;
//                case INT:
//                case FLOAT:
//                default:
//                    throw new IllegalStateException();
//            }
//        }
//        return new Record(dimensions, metrics);
//    }
//
//    @Override
//    void appendRecord(Record record)
//        throws IOException {
//        _records.add(record);
//    }
//
//    @Override
//    Record getStarTreeRecord(int docId)
//        throws IOException {
//        return _records.get(docId);
//    }
//
//    @Override
//    long getDimensionValue(int docId, int dimensionId) {
//        return _records.get(docId)._dimensions[dimensionId];
//    }
//
//    @Override
//    Iterator<Record> sortAndAggregateSegmentRecords(int numDocs)
//        throws IOException {
//        // Write all dimensions for segment records into the buffer, and sort all records using an int
//        // array
//        // PinotDataBuffer dataBuffer;
//        // long bufferSize = (long) numDocs * _numDimensions * Integer.BYTES;
//        long recordBytesLength = 0;
//        Integer[] sortedDocIds = new Integer[numDocs];
//        for (int i = 0; i < numDocs; i++) {
//            sortedDocIds[i] = i;
//        }
//
//        try {
//            for (int i = 0; i < numDocs; i++) {
//                Record record = getNextSegmentRecord();
//                byte[] bytes = serializeStarTreeRecord(record);
//                recordBytesLength = bytes.length;
//                segmentRecordFileOutput.writeBytes(bytes, bytes.length);
//            }
//        } finally {
//            segmentRecordFileOutput.close();
//        }
//
//        IndexInput segmentRecordFileInput = state.directory.openInput(segmentRecordFileOutput.getName(), state.context);
//        final long recordBytes = recordBytesLength;
//        segmentRandomInput = segmentRecordFileInput.randomAccessSlice(0, segmentRecordFileInput.length());
//
//        try {
//            // ArrayUtil.introSort(sortedDocIds, comparator);
//            // Arrays.sort(sortedDocIds, comparator);
//
//            QuickSorter.quickSort(0, numDocs, (i1, i2) -> {
//                long offset1 = (long) sortedDocIds[i1] * recordBytes;
//                long offset2 = (long) sortedDocIds[i2] * recordBytes;
//                for (int i = 0; i < _numDimensions; i++) {
//                    try {
//                        int dimension1 = segmentRandomInput.readInt(offset1 + i * Integer.BYTES);
//                        int dimension2 = segmentRandomInput.readInt(offset2 + i * Integer.BYTES);
//                        if (dimension1 != dimension2) {
//                            return dimension1 - dimension2;
//                        }
//                    } catch (IOException e) {
//                        throw new RuntimeException(e);
//                    }
//                }
//                return 0;
//            }, (i1, i2) -> {
//                int temp = sortedDocIds[i1];
//                sortedDocIds[i1] = sortedDocIds[i2];
//                sortedDocIds[i2] = temp;
//            });
//
//            // System.out.println("Sorted doc ids : " + Arrays.toString(sortedDocIds));
//        } finally {
//            segmentRecordFileInput.close();
//        }
//
//        // Create an iterator for aggregated records
//        return new Iterator<Record>() {
//            boolean _hasNext = true;
//            Record _currentRecord = getSegmentRecord(sortedDocIds[0], recordBytes);
//            int _docId = 1;
//
//            @Override
//            public boolean hasNext() {
//                return _hasNext;
//            }
//
//            @Override
//            public Record next() {
//                Record next = mergeSegmentRecord(null, _currentRecord);
//                while (_docId < numDocs) {
//                    Record record = null;
//                    try {
//                        record = getSegmentRecord(sortedDocIds[_docId++], recordBytes);
//                    } catch (IOException e) {
//                        // TODO : handle this block better - how to handle exceptions ?
//                        throw new RuntimeException(e);
//                    }
//                    if (!Arrays.equals(record._dimensions, next._dimensions)) {
//                        _currentRecord = record;
//                        return next;
//                    } else {
//                        next = mergeSegmentRecord(next, record);
//                    }
//                }
//                _hasNext = false;
//
//                IOUtils.closeWhileHandlingException(segmentRecordFileInput, segmentRecordFileOutput);
//                IOUtils.deleteFilesIgnoringExceptions(state.directory, segmentRecordFileOutput.getName());
//                return next;
//            }
//        };
//    }
//
//    public Record getSegmentRecord(int docID, long recordBytes)
//        throws IOException {
//        return deserializeStarTreeRecord(segmentRandomInput, docID * recordBytes);
//    }
//
//    @Override
//    Iterator<Record> generateRecordsForStarNode(int startDocId, int endDocId, int dimensionId)
//        throws IOException {
//
//        int numDocs = endDocId - startDocId;
//        Record[] records = new Record[numDocs];
//        for (int i = 0; i < numDocs; i++) {
//            records[i] = getStarTreeRecord(startDocId + i);
//        }
//        Arrays.sort(records, (o1, o2) -> {
//            for (int i = dimensionId + 1; i < _numDimensions; i++) {
//                if (o1._dimensions[i] != o2._dimensions[i]) {
//                    return o1._dimensions[i] - o2._dimensions[i];
//                }
//            }
//            return 0;
//        });
//        return new Iterator<Record>() {
//            boolean _hasNext = true;
//            Record _currentRecord = records[0];
//            int _docId = 1;
//
//            private boolean hasSameDimensions(Record record1, Record record2) {
//                for (int i = dimensionId + 1; i < _numDimensions; i++) {
//                    if (record1._dimensions[i] != record2._dimensions[i]) {
//                        return false;
//                    }
//                }
//                return true;
//            }
//
//            @Override
//            public boolean hasNext() {
//                return _hasNext;
//            }
//
//            @Override
//            public Record next() {
//                Record next = mergeStarTreeRecord(null, _currentRecord);
//                next._dimensions[dimensionId] = STAR_IN_DOC_VALUES_INDEX;
//                while (_docId < numDocs) {
//                    Record record = records[_docId++];
//                    if (!hasSameDimensions(record, _currentRecord)) {
//                        _currentRecord = record;
//                        return next;
//                    } else {
//                        next = mergeStarTreeRecord(next, record);
//                    }
//                }
//                _hasNext = false;
//                return next;
//            }
//        };
//    }
//
//    @Override
//    public void close()
//        throws IOException {
//        IOUtils.deleteFilesIgnoringExceptions(Path.of(
//            IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, SEGMENT_RECORD_FILE_NAME)));
//        super.close();
//    }
//}
