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
package org.opensearch.index.codec.freshstartree.codec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.apache.lucene.util.packed.DirectReader;


/**
 * Created a copy to initialize producer without field info stored in state which is the case for
 * aggregated doc values fields
 */
public class Lucene90DocValuesProducerCopy extends DocValuesProducer {
    private final Map<String, NumericEntry> numerics;
    private final Map<String, SortedNumericEntry> sortedNumerics;
    private final IndexInput data;
    private final int maxDoc;
    private int version = -1;
    private final boolean merging;

    private FieldInfo[] fieldInfoArr;

    private List<String> dimensionSplitOrder;

    /** expert: instantiates a new reader */
    public Lucene90DocValuesProducerCopy(SegmentReadState state, String dataCodec, String dataExtension,
        String metaCodec, String metaExtension, List<String> dimensionSplitOrder)
        throws IOException {
        String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
        this.maxDoc = state.segmentInfo.maxDoc();
        numerics = new HashMap<>();
        sortedNumerics = new HashMap<>();
        merging = false;
        this.dimensionSplitOrder = dimensionSplitOrder;
        fieldInfoArr = getFieldInfoArr();

        // read in the entries from the metadata file.
        try (ChecksumIndexInput in = state.directory.openChecksumInput(metaName, state.context)) {
            Throwable priorE = null;

            try {
                version = CodecUtil.checkIndexHeader(in, metaCodec, 0,// TODO : don't hardcode
                    0, // TODO : don't hardcode
                    state.segmentInfo.getId(), state.segmentSuffix);

                readFields(in, fieldInfoArr);
            } catch (Throwable exception) {
                priorE = exception;
            } finally {
                CodecUtil.checkFooter(in, priorE);
            }
        }

        String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
        this.data = state.directory.openInput(dataName, state.context);
        boolean success = false;
        try {
            final int version2 = CodecUtil.checkIndexHeader(data, dataCodec, 0, // TODO : don't hardcode
                0, // Todo : don't hardcode
                state.segmentInfo.getId(), state.segmentSuffix);
            if (version != version2) {
                throw new CorruptIndexException("Format versions mismatch: meta=" + version + ", data=" + version2,
                    data);
            }

            // NOTE: data file is too costly to verify checksum against all the bytes on open,
            // but for now we at least verify proper structure of the checksum footer: which looks
            // for FOOTER_MAGIC + algorithmID. This is cheap and can detect some forms of corruption
            // such as file truncation.
            CodecUtil.retrieveChecksum(data);

            success = true;
        } finally {
            if (!success) {
                IOUtils.closeWhileHandlingException(this.data);
            }
        }
    }

    // Used for cloning
    private Lucene90DocValuesProducerCopy(Map<String, NumericEntry> numerics,
        Map<String, SortedNumericEntry> sortedNumerics, IndexInput data, int maxDoc, int version, boolean merging) {
        this.numerics = numerics;
        this.sortedNumerics = sortedNumerics;
        this.data = data.clone();
        this.maxDoc = maxDoc;
        this.version = version;
        this.merging = merging;
    }

    @Override
    public DocValuesProducer getMergeInstance() {
        return new Lucene90DocValuesProducerCopy(numerics, sortedNumerics, data, maxDoc, version, true);
    }

    public FieldInfo[] getFieldInfoArr() {
        List<String> metrics = new ArrayList<>();
        // TODO : remove this
        metrics.add("status_sum");
        //metrics.add("status_count");
        FieldInfo[] fArr = new FieldInfo[dimensionSplitOrder.size() + metrics.size()];
        int fieldNum = 0;
        for (int i = 0; i < dimensionSplitOrder.size(); i++) {
            fArr[fieldNum] = new FieldInfo(dimensionSplitOrder.get(i) + "_dim", fieldNum, false, false, true,
                IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS, DocValuesType.NUMERIC, -1,
                Collections.emptyMap(), 0, 0, 0, 0, VectorEncoding.FLOAT32, VectorSimilarityFunction.EUCLIDEAN, false);
            fieldNum++;
        }
        for (int i = 0; i < metrics.size(); i++) {
            fArr[fieldNum] = new FieldInfo(metrics.get(i) + "_metric", fieldNum, false, false, true,
                IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS, DocValuesType.NUMERIC, -1,
                Collections.emptyMap(), 0, 0, 0, 0, VectorEncoding.FLOAT32, VectorSimilarityFunction.EUCLIDEAN, false);
            fieldNum++;
        }
        return fArr;
    }

    private void readFields(IndexInput meta, FieldInfo[] infos)
        throws IOException {
        for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {
            // System.out.println("Field number : " + fieldNumber);
            FieldInfo info = infos[fieldNumber];
            if (info == null) {
                throw new CorruptIndexException("Invalid field number: " + fieldNumber, meta);
            }
            byte type = meta.readByte();
            if (type == 0) { // Lucene90DocValuesFormat.NUMERIC) {
                numerics.put(info.name, readNumeric(meta));
            } else if (type == 4) { // Lucene90DocValuesFormat.SORTED_NUMERIC
                sortedNumerics.put(info.name, readSortedNumeric(meta));
            } else {
                throw new CorruptIndexException("invalid type: " + type, meta);
            }
        }
    }

    private NumericEntry readNumeric(IndexInput meta)
        throws IOException {
        NumericEntry entry = new NumericEntry();
        readNumeric(meta, entry);
        return entry;
    }

    private void readNumeric(IndexInput meta, NumericEntry entry)
        throws IOException {
        entry.docsWithFieldOffset = meta.readLong();
        entry.docsWithFieldLength = meta.readLong();
        entry.jumpTableEntryCount = meta.readShort();
        entry.denseRankPower = meta.readByte();
        entry.numValues = meta.readLong();
        int tableSize = meta.readInt();
        if (tableSize > 256) {
            throw new CorruptIndexException("invalid table size: " + tableSize, meta);
        }
        if (tableSize >= 0) {
            entry.table = new long[tableSize];
            for (int i = 0; i < tableSize; ++i) {
                entry.table[i] = meta.readLong();
            }
        }
        if (tableSize < -1) {
            entry.blockShift = -2 - tableSize;
        } else {
            entry.blockShift = -1;
        }
        entry.bitsPerValue = meta.readByte();
        entry.minValue = meta.readLong();
        entry.gcd = meta.readLong();
        entry.valuesOffset = meta.readLong();
        entry.valuesLength = meta.readLong();
        entry.valueJumpTableOffset = meta.readLong();
    }

    private SortedNumericEntry readSortedNumeric(IndexInput meta)
        throws IOException {
        SortedNumericEntry entry = new SortedNumericEntry();
        readSortedNumeric(meta, entry);
        return entry;
    }

    private SortedNumericEntry readSortedNumeric(IndexInput meta, SortedNumericEntry entry)
        throws IOException {
        readNumeric(meta, entry);
        entry.numDocsWithField = meta.readInt();
        if (entry.numDocsWithField != entry.numValues) {
            entry.addressesOffset = meta.readLong();
            final int blockShift = meta.readVInt();
            entry.addressesMeta = DirectMonotonicReader.loadMeta(meta, entry.numDocsWithField + 1, blockShift);
            entry.addressesLength = meta.readLong();
        }
        return entry;
    }

    @Override
    public void close()
        throws IOException {
        data.close();
    }

    /** Numeric entry */
    public static class NumericEntry {
        long[] table;
        int blockShift;
        byte bitsPerValue;
        long docsWithFieldOffset;
        long docsWithFieldLength;
        short jumpTableEntryCount;
        byte denseRankPower;
        long numValues;
        long minValue;
        long gcd;
        long valuesOffset;
        long valuesLength;
        long valueJumpTableOffset; // -1 if no jump-table
    }

    private static class SortedNumericEntry extends NumericEntry {
        int numDocsWithField;
        DirectMonotonicReader.Meta addressesMeta;
        long addressesOffset;
        long addressesLength;
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field)
        throws IOException {
        NumericEntry entry = numerics.get(field.name);
        return getNumeric(entry);
    }

    public NumericDocValues getNumeric(String fieldName)
        throws IOException {
        NumericEntry entry = numerics.get(fieldName);
        return getNumeric(entry);
    }

    private abstract static class DenseNumericDocValues extends NumericDocValues {

        final int maxDoc;
        int doc = -1;

        DenseNumericDocValues(int maxDoc) {
            this.maxDoc = maxDoc;
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc()
            throws IOException {
            return advance(doc + 1);
        }

        @Override
        public int advance(int target)
            throws IOException {
            if (target >= maxDoc) {
                return doc = NO_MORE_DOCS;
            }
            return doc = target;
        }

        @Override
        public boolean advanceExact(int target) {
            doc = target;
            return true;
        }

        @Override
        public long cost() {
            return maxDoc;
        }
    }

    private abstract static class SparseNumericDocValues extends NumericDocValues {

        final IndexedDISI disi;

        SparseNumericDocValues(IndexedDISI disi) {
            this.disi = disi;
        }

        @Override
        public int advance(int target)
            throws IOException {
            return disi.advance(target);
        }

        @Override
        public boolean advanceExact(int target)
            throws IOException {
            return disi.advanceExact(target);
        }

        @Override
        public int nextDoc()
            throws IOException {
            return disi.nextDoc();
        }

        @Override
        public int docID() {
            return disi.docID();
        }

        @Override
        public long cost() {
            return disi.cost();
        }
    }

    private LongValues getDirectReaderInstance(RandomAccessInput slice, int bitsPerValue, long offset, long numValues) {
        if (merging) {
            return DirectReader.getMergeInstance(slice, bitsPerValue, offset, numValues);
        } else {
            return DirectReader.getInstance(slice, bitsPerValue, offset);
        }
    }

    private NumericDocValues getNumeric(NumericEntry entry)
        throws IOException {
        if (entry.docsWithFieldOffset == -2) {
            // empty
            return DocValues.emptyNumeric();
        } else if (entry.docsWithFieldOffset == -1) {
            // dense
            if (entry.bitsPerValue == 0) {
                return new DenseNumericDocValues(maxDoc) {
                    @Override
                    public long longValue()
                        throws IOException {
                        return entry.minValue;
                    }
                };
            } else {
                final RandomAccessInput slice = data.randomAccessSlice(entry.valuesOffset, entry.valuesLength);
                if (entry.blockShift >= 0) {
                    // dense but split into blocks of different bits per value
                    return new DenseNumericDocValues(maxDoc) {
                        final VaryingBPVReader vBPVReader = new VaryingBPVReader(entry, slice);

                        @Override
                        public long longValue()
                            throws IOException {
                            return vBPVReader.getLongValue(doc);
                        }
                    };
                } else {
                    final LongValues values = getDirectReaderInstance(slice, entry.bitsPerValue, 0L, entry.numValues);
                    if (entry.table != null) {
                        final long[] table = entry.table;
                        return new DenseNumericDocValues(maxDoc) {
                            @Override
                            public long longValue()
                                throws IOException {
                                return table[(int) values.get(doc)];
                            }
                        };
                    } else if (entry.gcd == 1 && entry.minValue == 0) {
                        // Common case for ordinals, which are encoded as numerics
                        return new DenseNumericDocValues(maxDoc) {
                            @Override
                            public long longValue()
                                throws IOException {
                                return values.get(doc);
                            }
                        };
                    } else {
                        final long mul = entry.gcd;
                        final long delta = entry.minValue;
                        return new DenseNumericDocValues(maxDoc) {
                            @Override
                            public long longValue()
                                throws IOException {
                                return mul * values.get(doc) + delta;
                            }
                        };
                    }
                }
            }
        } else {
            // sparse
            final IndexedDISI disi =
                new IndexedDISI(data, entry.docsWithFieldOffset, entry.docsWithFieldLength, entry.jumpTableEntryCount,
                    entry.denseRankPower, entry.numValues);
            if (entry.bitsPerValue == 0) {
                return new SparseNumericDocValues(disi) {
                    @Override
                    public long longValue()
                        throws IOException {
                        return entry.minValue;
                    }
                };
            } else {
                final RandomAccessInput slice = data.randomAccessSlice(entry.valuesOffset, entry.valuesLength);
                if (entry.blockShift >= 0) {
                    // sparse and split into blocks of different bits per value
                    return new SparseNumericDocValues(disi) {
                        final VaryingBPVReader vBPVReader = new VaryingBPVReader(entry, slice);

                        @Override
                        public long longValue()
                            throws IOException {
                            final int index = disi.index();
                            return vBPVReader.getLongValue(index);
                        }
                    };
                } else {
                    final LongValues values = getDirectReaderInstance(slice, entry.bitsPerValue, 0L, entry.numValues);
                    if (entry.table != null) {
                        final long[] table = entry.table;
                        return new SparseNumericDocValues(disi) {
                            @Override
                            public long longValue()
                                throws IOException {
                                return table[(int) values.get(disi.index())];
                            }
                        };
                    } else if (entry.gcd == 1 && entry.minValue == 0) {
                        return new SparseNumericDocValues(disi) {
                            @Override
                            public long longValue()
                                throws IOException {
                                return values.get(disi.index());
                            }
                        };
                    } else {
                        final long mul = entry.gcd;
                        final long delta = entry.minValue;
                        return new SparseNumericDocValues(disi) {
                            @Override
                            public long longValue()
                                throws IOException {
                                return mul * values.get(disi.index()) + delta;
                            }
                        };
                    }
                }
            }
        }
    }

    private LongValues getNumericValues(NumericEntry entry)
        throws IOException {
        if (entry.bitsPerValue == 0) {
            return new LongValues() {
                @Override
                public long get(long index) {
                    return entry.minValue;
                }
            };
        } else {
            final RandomAccessInput slice = data.randomAccessSlice(entry.valuesOffset, entry.valuesLength);
            if (entry.blockShift >= 0) {
                return new LongValues() {
                    final VaryingBPVReader vBPVReader = new VaryingBPVReader(entry, slice);

                    @Override
                    public long get(long index) {
                        try {
                            return vBPVReader.getLongValue(index);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                };
            } else {
                final LongValues values = getDirectReaderInstance(slice, entry.bitsPerValue, 0L, entry.numValues);
                if (entry.table != null) {
                    final long[] table = entry.table;
                    return new LongValues() {
                        @Override
                        public long get(long index) {
                            return table[(int) values.get(index)];
                        }
                    };
                } else if (entry.gcd != 1) {
                    final long gcd = entry.gcd;
                    final long minValue = entry.minValue;
                    return new LongValues() {
                        @Override
                        public long get(long index) {
                            return values.get(index) * gcd + minValue;
                        }
                    };
                } else if (entry.minValue != 0) {
                    final long minValue = entry.minValue;
                    return new LongValues() {
                        @Override
                        public long get(long index) {
                            return values.get(index) + minValue;
                        }
                    };
                } else {
                    return values;
                }
            }
        }
    }

    private abstract static class DenseBinaryDocValues extends BinaryDocValues {

        final int maxDoc;
        int doc = -1;

        DenseBinaryDocValues(int maxDoc) {
            this.maxDoc = maxDoc;
        }

        @Override
        public int nextDoc()
            throws IOException {
            return advance(doc + 1);
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public long cost() {
            return maxDoc;
        }

        @Override
        public int advance(int target)
            throws IOException {
            if (target >= maxDoc) {
                return doc = NO_MORE_DOCS;
            }
            return doc = target;
        }

        @Override
        public boolean advanceExact(int target)
            throws IOException {
            doc = target;
            return true;
        }
    }

    private abstract static class SparseBinaryDocValues extends BinaryDocValues {

        final IndexedDISI disi;

        SparseBinaryDocValues(IndexedDISI disi) {
            this.disi = disi;
        }

        @Override
        public int nextDoc()
            throws IOException {
            return disi.nextDoc();
        }

        @Override
        public int docID() {
            return disi.docID();
        }

        @Override
        public long cost() {
            return disi.cost();
        }

        @Override
        public int advance(int target)
            throws IOException {
            return disi.advance(target);
        }

        @Override
        public boolean advanceExact(int target)
            throws IOException {
            return disi.advanceExact(target);
        }
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field)
        throws IOException {
        return null; // TODO
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field)
        throws IOException {
        return null; // TODO
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field)
        throws IOException {
        SortedNumericEntry entry = sortedNumerics.get(field.name);
        return getSortedNumeric(entry);
    }

    public SortedNumericDocValues getSortedNumeric(String fieldName)
        throws IOException {
        SortedNumericEntry entry = sortedNumerics.get(fieldName);
        return getSortedNumeric(entry);
    }

    private SortedNumericDocValues getSortedNumeric(SortedNumericEntry entry)
        throws IOException {
        if (entry.numValues == entry.numDocsWithField) {
            return DocValues.singleton(getNumeric(entry));
        }

        final RandomAccessInput addressesInput = data.randomAccessSlice(entry.addressesOffset, entry.addressesLength);
        final LongValues addresses = DirectMonotonicReader.getInstance(entry.addressesMeta, addressesInput, merging);

        final LongValues values = getNumericValues(entry);

        if (entry.docsWithFieldOffset == -1) {
            // dense
            return new SortedNumericDocValues() {

                int doc = -1;
                long start, end;
                int count;

                @Override
                public int nextDoc()
                    throws IOException {
                    return advance(doc + 1);
                }

                @Override
                public int docID() {
                    return doc;
                }

                @Override
                public long cost() {
                    return maxDoc;
                }

                @Override
                public int advance(int target)
                    throws IOException {
                    if (target >= maxDoc) {
                        return doc = NO_MORE_DOCS;
                    }
                    start = addresses.get(target);
                    end = addresses.get(target + 1L);
                    count = (int) (end - start);
                    return doc = target;
                }

                @Override
                public boolean advanceExact(int target)
                    throws IOException {
                    start = addresses.get(target);
                    end = addresses.get(target + 1L);
                    count = (int) (end - start);
                    doc = target;
                    return true;
                }

                @Override
                public long nextValue()
                    throws IOException {
                    return values.get(start++);
                }

                @Override
                public int docValueCount() {
                    return count;
                }
            };
        } else {
            // sparse
            final IndexedDISI disi =
                new IndexedDISI(data, entry.docsWithFieldOffset, entry.docsWithFieldLength, entry.jumpTableEntryCount,
                    entry.denseRankPower, entry.numDocsWithField);
            return new SortedNumericDocValues() {

                boolean set;
                long start, end;
                int count;

                @Override
                public int nextDoc()
                    throws IOException {
                    set = false;
                    return disi.nextDoc();
                }

                @Override
                public int docID() {
                    return disi.docID();
                }

                @Override
                public long cost() {
                    return disi.cost();
                }

                @Override
                public int advance(int target)
                    throws IOException {
                    set = false;
                    return disi.advance(target);
                }

                @Override
                public boolean advanceExact(int target)
                    throws IOException {
                    set = false;
                    return disi.advanceExact(target);
                }

                @Override
                public long nextValue()
                    throws IOException {
                    set();
                    return values.get(start++);
                }

                @Override
                public int docValueCount() {
                    set();
                    return count;
                }

                private void set() {
                    if (set == false) {
                        final int index = disi.index();
                        start = addresses.get(index);
                        end = addresses.get(index + 1L);
                        count = (int) (end - start);
                        set = true;
                    }
                }
            };
        }
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field)
        throws IOException {
        return null;
    }

    @Override
    public void checkIntegrity()
        throws IOException {
        CodecUtil.checksumEntireFile(data);
    }

    /**
     * Reader for longs split into blocks of different bits per values. The longs are requested by
     * index and must be accessed in monotonically increasing order.
     */
    // Note: The order requirement could be removed as the jump-tables allow for backwards iteration
    // Note 2: The rankSlice is only used if an advance of > 1 block is called. Its construction could
    // be lazy
    private class VaryingBPVReader {
        final RandomAccessInput slice; // 2 slices to avoid cache thrashing when using rank
        final RandomAccessInput rankSlice;
        final NumericEntry entry;
        final int shift;
        final long mul;
        final int mask;

        long block = -1;
        long delta;
        long offset;
        long blockEndOffset;
        LongValues values;

        VaryingBPVReader(NumericEntry entry, RandomAccessInput slice)
            throws IOException {
            this.entry = entry;
            this.slice = slice;
            this.rankSlice = entry.valueJumpTableOffset == -1 ? null
                : data.randomAccessSlice(entry.valueJumpTableOffset, data.length() - entry.valueJumpTableOffset);
            shift = entry.blockShift;
            mul = entry.gcd;
            mask = (1 << shift) - 1;
        }

        long getLongValue(long index)
            throws IOException {
            final long block = index >>> shift;
            if (this.block != block) {
                int bitsPerValue;
                do {
                    // If the needed block is the one directly following the current block, it is cheaper to
                    // avoid the cache
                    if (rankSlice != null && block != this.block + 1) {
                        blockEndOffset = rankSlice.readLong(block * Long.BYTES) - entry.valuesOffset;
                        this.block = block - 1;
                    }
                    offset = blockEndOffset;
                    bitsPerValue = slice.readByte(offset++);
                    delta = slice.readLong(offset);
                    offset += Long.BYTES;
                    if (bitsPerValue == 0) {
                        blockEndOffset = offset;
                    } else {
                        final int length = slice.readInt(offset);
                        offset += Integer.BYTES;
                        blockEndOffset = offset + length;
                    }
                    this.block++;
                } while (this.block != block);
                final int numValues = Math.toIntExact(Math.min(1 << shift, entry.numValues - (block << shift)));
                values = bitsPerValue == 0 ? LongValues.ZEROES
                    : getDirectReaderInstance(slice, bitsPerValue, offset, numValues);
            }
            return mul * values.get(index & mask) + delta;
        }
    }
}
