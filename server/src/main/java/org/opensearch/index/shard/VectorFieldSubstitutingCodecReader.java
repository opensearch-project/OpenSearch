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
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;

import java.io.IOException;
import java.util.Objects;

/**
 * A {@link CodecReader} that substitutes the values of a single KNN vector field while delegating
 * every other field to the wrapped reader unchanged.
 *
 * <p>This is the mechanism behind a whole-field vector rewrite (for example, re-embedding a corpus
 * after an embedding-model upgrade). When such a wrapped reader is passed to
 * {@link org.apache.lucene.index.IndexWriter#addIndexes(CodecReader...)}, the merge machinery:
 *
 * <ul>
 *   <li>reads the substituted vectors for the target field, so the destination segment's vector
 *       files ({@code .vec}/{@code .vex}/{@code .vem}, or the engine-native equivalent) are
 *       rebuilt from the new values; and
 *   <li>bulk-copies stored fields, postings, doc values, points, norms and term vectors from the
 *       source segment, so no text is re-analyzed and no {@code _source} is re-parsed.
 * </ul>
 *
 * <p>The important property is what this class does <em>not</em> introduce: there is no per-field
 * storage engine, no per-field merge policy, and no per-field lifecycle. One index, one engine, one
 * merge policy — the substitution happens for the duration of a single merge and leaves no
 * abstraction behind in the resulting segment, which is an ordinary segment written by the index's
 * own codec.
 *
 * <p>Only the named field is affected. A different vector field in the same index continues to
 * resolve through the delegate, so multi-vector indices rewrite one field at a time.
 *
 * @opensearch.internal
 */
public final class VectorFieldSubstitutingCodecReader extends FilterCodecReader {

    /**
     * Supplies replacement vectors for the field being rewritten.
     *
     * <p>Implementations map a document to its new embedding. The PoC uses a synthetic transform;
     * a production caller would invoke the new embedding model here.
     */
    @FunctionalInterface
    public interface VectorSupplier {
        /**
         * Returns the replacement vector for the given segment-local document id, or {@code null}
         * to leave the document without a vector for this field.
         *
         * @param docId segment-local document id
         * @param dimension the field's configured dimension
         */
        float[] vectorFor(int docId, int dimension) throws IOException;
    }

    private final String field;
    private final VectorSupplier supplier;

    /**
     * @param in the source segment reader; every non-target field is delegated to it
     * @param field the KNN vector field whose values are replaced
     * @param supplier supplies the replacement vector per document
     */
    public VectorFieldSubstitutingCodecReader(CodecReader in, String field, VectorSupplier supplier) {
        super(in);
        this.field = Objects.requireNonNull(field, "field");
        this.supplier = Objects.requireNonNull(supplier, "supplier");
    }

    @Override
    public KnnVectorsReader getVectorReader() {
        final KnnVectorsReader delegate = in.getVectorReader();
        return new SubstitutingVectorsReader(delegate, field, supplier);
    }

    // Cache helpers are intentionally not inherited: the substituted view is not the same content
    // as the delegate, so it must never share the delegate's cache identity.
    @Override
    public CacheHelper getCoreCacheHelper() {
        return null;
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return null;
    }

    /**
     * Routes one field to freshly supplied vectors and everything else to the delegate.
     */
    private static final class SubstitutingVectorsReader extends KnnVectorsReader {
        private final KnnVectorsReader delegate;
        private final String field;
        private final VectorSupplier supplier;

        SubstitutingVectorsReader(KnnVectorsReader delegate, String field, VectorSupplier supplier) {
            this.delegate = delegate;
            this.field = field;
            this.supplier = supplier;
        }

        @Override
        public FloatVectorValues getFloatVectorValues(String requestedField) throws IOException {
            if (field.equals(requestedField) == false) {
                return delegate == null ? null : delegate.getFloatVectorValues(requestedField);
            }
            if (delegate == null) {
                return null;
            }
            FloatVectorValues base = delegate.getFloatVectorValues(requestedField);
            if (base == null) {
                return null;
            }
            return new SubstitutedFloatVectorValues(base, supplier);
        }

        @Override
        public ByteVectorValues getByteVectorValues(String requestedField) throws IOException {
            if (field.equals(requestedField)) {
                // Byte-encoded fields are out of scope for this PoC; failing loudly is safer than
                // silently passing through the old values and reporting a successful rewrite.
                throw new UnsupportedOperationException(
                    "vector field substitution is only implemented for float32 encoding; field [" + field + "] is byte-encoded"
                );
            }
            return delegate == null ? null : delegate.getByteVectorValues(requestedField);
        }

        @Override
        public void search(String requestedField, float[] target, KnnCollector collector, AcceptDocs acceptDocs) throws IOException {
            // addIndexes()/merge reads vectors through get*VectorValues, never through search().
            if (field.equals(requestedField)) {
                throw new UnsupportedOperationException("ANN search is not supported on a substituting reader");
            }
            if (delegate != null) {
                delegate.search(requestedField, target, collector, acceptDocs);
            }
        }

        @Override
        public void search(String requestedField, byte[] target, KnnCollector collector, AcceptDocs acceptDocs) throws IOException {
            if (field.equals(requestedField)) {
                throw new UnsupportedOperationException("ANN search is not supported on a substituting reader");
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
         * The merge path calls this on whatever {@code getVectorReader()} returned
         * ({@code MergeState} does so immediately after acquiring the reader) and then reads vectors
         * from the result. Returning a merge instance of the <em>delegate</em> here would hand the
         * merge the original vectors and silently discard the substitution, so the substituting view
         * must remain in place.
         *
         * <p>A delegate merge instance is still obtained and wrapped, so any read optimizations the
         * underlying format applies during merge are preserved.
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
            return new SubstitutingVectorsReader(delegateMergeInstance, field, supplier);
        }

        @Override
        public void finishMerge() throws IOException {
            if (delegate != null) {
                delegate.finishMerge();
            }
        }

        @Override
        public void close() throws IOException {
            // The delegate's lifecycle belongs to the wrapped CodecReader, not to this view.
        }
    }

    /**
     * Presents the delegate's document/ordinal layout with replacement vector values.
     *
     * <p>Reusing the delegate's iterator and {@code ordToDoc} mapping is deliberate: which
     * documents have a vector is unchanged by a whole-field re-embedding, only the values differ.
     */
    private static final class SubstitutedFloatVectorValues extends FloatVectorValues {
        private final FloatVectorValues base;
        private final VectorSupplier supplier;

        SubstitutedFloatVectorValues(FloatVectorValues base, VectorSupplier supplier) {
            this.base = base;
            this.supplier = supplier;
        }

        @Override
        public float[] vectorValue(int ord) throws IOException {
            int docId = base.ordToDoc(ord);
            float[] replacement = supplier.vectorFor(docId, base.dimension());
            if (replacement == null) {
                return base.vectorValue(ord);
            }
            if (replacement.length != base.dimension()) {
                throw new IllegalArgumentException(
                    "replacement vector for doc ["
                        + docId
                        + "] has dimension ["
                        + replacement.length
                        + "] but field expects ["
                        + base.dimension()
                        + "]"
                );
            }
            return replacement;
        }

        @Override
        public int dimension() {
            return base.dimension();
        }

        @Override
        public int size() {
            return base.size();
        }

        @Override
        public int ordToDoc(int ord) {
            return base.ordToDoc(ord);
        }

        @Override
        public DocIndexIterator iterator() {
            return base.iterator();
        }

        @Override
        public VectorEncoding getEncoding() {
            return base.getEncoding();
        }

        @Override
        public FloatVectorValues copy() throws IOException {
            return new SubstitutedFloatVectorValues(base.copy(), supplier);
        }
    }
}
