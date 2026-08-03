/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A {@link CodecReader} that presents a KNN vector field which does <em>not exist</em> in the wrapped
 * segment, while delegating every existing field unchanged.
 *
 * <p>This is the counterpart to {@link VectorFieldSubstitutingCodecReader}. That class replaces the
 * values of a field already present; this one <em>adds</em> a field. The distinction matters for the
 * "field pair + alias flip" upgrade shape, where an index declares two vector fields, leaves one
 * empty, and later populates the empty one with a new embedding model:
 *
 * <pre>
 *   embedding_foo (populated, old model)   embedding_bar (declared, empty)
 *                                              |
 *                          populate via this reader, then flip a field alias
 * </pre>
 *
 * <p>Without this class, populating the empty field requires re-adding every document — which
 * re-analyzes text and re-encodes every other field, forfeiting the bulk-copy saving that motivates
 * the whole design. With it, the new field's vectors are written by the codec while stored fields,
 * postings, doc values, points and norms are bulk-copied, exactly as for a substitution.
 *
 * <p><b>How the field is synthesized.</b> A merge reads the field list from
 * {@link CodecReader#getFieldInfos()}, so the added field must appear there or it is never asked for.
 * This reader returns an augmented {@link FieldInfos} containing the delegate's entries plus one new
 * vector-only {@link FieldInfo}, assigned a field number above every existing one (Lucene rejects
 * duplicate field numbers). The vectors themselves come from a supplier, addressed by segment-local
 * document id.
 *
 * <p>As with substitution, no per-field storage engine, lifecycle or merge policy is introduced: the
 * resulting segment is an ordinary segment written by the index's own codec.
 *
 * @opensearch.internal
 */
public final class VectorFieldAddingCodecReader extends FilterCodecReader {

    /**
     * Supplies vectors for the field being added.
     *
     * <p>Returning {@code null} leaves that document without a value for the new field, which is how
     * a sparsely-populated vector field is expressed.
     */
    @FunctionalInterface
    public interface VectorSupplier {
        /**
         * @param docId segment-local document id
         * @return the vector for this document, or {@code null} for no value
         */
        float[] vectorFor(int docId) throws IOException;
    }

    private final String field;
    private final int dimension;
    private final VectorSimilarityFunction similarity;
    private final VectorSupplier supplier;
    private final FieldInfos augmentedFieldInfos;
    private final FieldInfo addedFieldInfo;

    /**
     * @param in the source segment; every existing field is delegated to it
     * @param field name of the vector field to add; must not already exist in {@code in}
     * @param dimension the new field's dimension
     * @param similarity the new field's similarity function
     * @param supplier supplies each document's vector
     */
    public VectorFieldAddingCodecReader(
        CodecReader in,
        String field,
        int dimension,
        VectorSimilarityFunction similarity,
        VectorSupplier supplier
    ) {
        super(in);
        this.field = Objects.requireNonNull(field, "field");
        this.dimension = dimension;
        this.similarity = Objects.requireNonNull(similarity, "similarity");
        this.supplier = Objects.requireNonNull(supplier, "supplier");

        if (dimension <= 0) {
            throw new IllegalArgumentException("dimension must be positive but was [" + dimension + "]");
        }
        FieldInfos existing = in.getFieldInfos();
        if (existing.fieldInfo(field) != null) {
            throw new IllegalArgumentException(
                "field [" + field + "] already exists in this segment; use VectorFieldSubstitutingCodecReader to replace its values"
            );
        }

        // Field numbers must be unique within a segment, so the new field takes one above the max.
        int nextNumber = 0;
        List<FieldInfo> infos = new ArrayList<>();
        for (FieldInfo fi : existing) {
            infos.add(fi);
            nextNumber = Math.max(nextNumber, fi.number + 1);
        }
        this.addedFieldInfo = new FieldInfo(
            field,
            nextNumber,
            false,                              // storeTermVector
            false,                              // omitNorms
            false,                              // storePayloads
            IndexOptions.NONE,                  // not inverted: vector-only field
            DocValuesType.NONE,
            DocValuesSkipIndexType.NONE,
            -1,                                 // dvGen
            Collections.emptyMap(),             // attributes
            0,                                  // pointDimensionCount
            0,                                  // pointIndexDimensionCount
            0,                                  // pointNumBytes
            dimension,                          // vectorDimension
            VectorEncoding.FLOAT32,
            similarity,
            false,                              // softDeletesField
            false                               // isParentField
        );
        infos.add(addedFieldInfo);
        this.augmentedFieldInfos = new FieldInfos(infos.toArray(new FieldInfo[0]));
    }

    /**
     * The merge reads the field list from here, so the added field must be visible or the codec will
     * never request its vectors.
     */
    @Override
    public FieldInfos getFieldInfos() {
        return augmentedFieldInfos;
    }

    @Override
    public KnnVectorsReader getVectorReader() {
        return new FieldAddingVectorsReader(in.getVectorReader(), field, dimension, supplier, maxDoc());
    }

    // The augmented view is not the delegate's content, so it must not share its cache identity.
    @Override
    public CacheHelper getCoreCacheHelper() {
        return null;
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return null;
    }

    /**
     * Serves the added field from the supplier and everything else from the delegate.
     */
    private static final class FieldAddingVectorsReader extends KnnVectorsReader {
        private final KnnVectorsReader delegate;
        private final String field;
        private final int dimension;
        private final VectorSupplier supplier;
        private final int maxDoc;

        FieldAddingVectorsReader(KnnVectorsReader delegate, String field, int dimension, VectorSupplier supplier, int maxDoc) {
            this.delegate = delegate;
            this.field = field;
            this.dimension = dimension;
            this.supplier = supplier;
            this.maxDoc = maxDoc;
        }

        @Override
        public FloatVectorValues getFloatVectorValues(String requestedField) throws IOException {
            if (field.equals(requestedField)) {
                return new SuppliedFloatVectorValues(supplier, dimension, maxDoc);
            }
            // The source segment may have no vector fields at all, in which case there is no delegate.
            return delegate == null ? null : delegate.getFloatVectorValues(requestedField);
        }

        @Override
        public ByteVectorValues getByteVectorValues(String requestedField) throws IOException {
            if (field.equals(requestedField)) {
                throw new UnsupportedOperationException(
                    "field [" + field + "] is being added with float32 encoding; byte encoding is not implemented"
                );
            }
            return delegate == null ? null : delegate.getByteVectorValues(requestedField);
        }

        @Override
        public void search(String requestedField, float[] target, KnnCollector collector, AcceptDocs acceptDocs) throws IOException {
            if (field.equals(requestedField)) {
                throw new UnsupportedOperationException("ANN search is not supported on a field-adding reader");
            }
            if (delegate != null) {
                delegate.search(requestedField, target, collector, acceptDocs);
            }
        }

        @Override
        public void search(String requestedField, byte[] target, KnnCollector collector, AcceptDocs acceptDocs) throws IOException {
            if (field.equals(requestedField)) {
                throw new UnsupportedOperationException("ANN search is not supported on a field-adding reader");
            }
            if (delegate != null) {
                delegate.search(requestedField, target, collector, acceptDocs);
            }
        }

        @Override
        public void checkIntegrity() throws IOException {
            if (delegate != null) {
                delegate.checkIntegrity();
            }
        }

        /**
         * Must keep this wrapper in place: {@code MergeState} calls this on whatever
         * {@code getVectorReader()} returned and then reads vectors from the result, so handing back
         * the delegate's merge instance would drop the added field entirely.
         */
        @Override
        public KnnVectorsReader getMergeInstance() throws IOException {
            if (delegate == null) {
                return this;
            }
            KnnVectorsReader delegateMergeInstance = delegate.getMergeInstance();
            if (delegateMergeInstance == delegate) {
                return this;
            }
            return new FieldAddingVectorsReader(delegateMergeInstance, field, dimension, supplier, maxDoc);
        }

        @Override
        public void finishMerge() throws IOException {
            if (delegate != null) {
                delegate.finishMerge();
            }
        }

        @Override
        public void close() throws IOException {
            // The delegate's lifecycle belongs to the wrapped CodecReader.
        }
    }

    /**
     * Materializes the supplier as a {@link FloatVectorValues} over the segment's documents.
     *
     * <p>Documents for which the supplier returns {@code null} are skipped, so the field may be
     * sparse. Ordinals are assigned densely over the documents that do have a value, which is the
     * layout {@code KnnVectorValues} requires.
     */
    private static final class SuppliedFloatVectorValues extends FloatVectorValues {
        private final VectorSupplier supplier;
        private final int dimension;
        private final int maxDoc;
        private final int[] ordToDoc;
        private final int size;

        SuppliedFloatVectorValues(VectorSupplier supplier, int dimension, int maxDoc) throws IOException {
            this.supplier = supplier;
            this.dimension = dimension;
            this.maxDoc = maxDoc;
            // Establish which documents have a value up front: size() and ordToDoc() must be stable
            // and consistent with the iterator, and the codec queries them before reading vectors.
            int[] mapping = new int[maxDoc];
            int n = 0;
            for (int doc = 0; doc < maxDoc; doc++) {
                if (supplier.vectorFor(doc) != null) {
                    mapping[n++] = doc;
                }
            }
            this.ordToDoc = mapping;
            this.size = n;
        }

        private SuppliedFloatVectorValues(VectorSupplier supplier, int dimension, int maxDoc, int[] ordToDoc, int size) {
            this.supplier = supplier;
            this.dimension = dimension;
            this.maxDoc = maxDoc;
            this.ordToDoc = ordToDoc;
            this.size = size;
        }

        @Override
        public float[] vectorValue(int ord) throws IOException {
            if (ord < 0 || ord >= size) {
                throw new IllegalArgumentException("ord [" + ord + "] out of range [0, " + size + ")");
            }
            float[] v = supplier.vectorFor(ordToDoc[ord]);
            if (v == null) {
                // Would mean the supplier is not deterministic; the ord->doc map was built from it.
                throw new IllegalStateException("supplier returned no vector for doc [" + ordToDoc[ord] + "] after reporting one");
            }
            if (v.length != dimension) {
                throw new IllegalArgumentException(
                    "vector for doc [" + ordToDoc[ord] + "] has dimension [" + v.length + "] but field expects [" + dimension + "]"
                );
            }
            return v;
        }

        @Override
        public int dimension() {
            return dimension;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public int ordToDoc(int ord) {
            return ordToDoc[ord];
        }

        @Override
        public DocIndexIterator iterator() {
            return new DocIndexIterator() {
                private int ord = -1;

                @Override
                public int index() {
                    return ord;
                }

                @Override
                public int docID() {
                    if (ord < 0) {
                        return -1;
                    }
                    return ord >= size ? NO_MORE_DOCS : ordToDoc[ord];
                }

                @Override
                public int nextDoc() {
                    if (++ord >= size) {
                        ord = size;
                        return NO_MORE_DOCS;
                    }
                    return ordToDoc[ord];
                }

                @Override
                public int advance(int target) {
                    while (nextDoc() < target && docID() != NO_MORE_DOCS) {
                        // advance until at or past target
                    }
                    return docID();
                }

                @Override
                public long cost() {
                    return size;
                }
            };
        }

        @Override
        public VectorEncoding getEncoding() {
            return VectorEncoding.FLOAT32;
        }

        @Override
        public FloatVectorValues copy() {
            return new SuppliedFloatVectorValues(supplier, dimension, maxDoc, ordToDoc, size);
        }
    }
}
