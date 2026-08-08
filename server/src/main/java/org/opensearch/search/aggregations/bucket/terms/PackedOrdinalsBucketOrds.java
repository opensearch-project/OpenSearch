/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.terms;

import org.apache.lucene.util.BytesRef;
import org.opensearch.common.util.BigArrays;
import org.opensearch.search.aggregations.CardinalityUpperBound;

/**
 * {@link MultiTermsBucketOrds} for 3 or more keyword fields.
 * Serializes ordinals as fixed-width 8-byte longs into a {@link BytesRef}
 * (no type tags or length prefixes) and delegates storage to
 * {@link BytesKeyedBucketOrds}.
 *
 * @opensearch.internal
 */
public class PackedOrdinalsBucketOrds implements MultiTermsBucketOrds {

    private final BytesKeyedBucketOrds delegate;
    private final int numFields;
    /** Reusable scratch buffer sized to exactly {@code numFields * 8} bytes. */
    private final byte[] scratch;
    /** Reusable BytesRef wrapping {@link #scratch}. */
    private final BytesRef scratchRef;

    /**
     * Create a new instance.
     *
     * @param bigArrays   for memory allocation
     * @param cardinality upper bound on owning bucket cardinality
     * @param numFields   the number of fields (must be &ge; 2)
     */
    public PackedOrdinalsBucketOrds(BigArrays bigArrays, CardinalityUpperBound cardinality, int numFields) {
        assert numFields >= 2 : "PackedOrdinalsBucketOrds requires at least 2 fields";
        this.delegate = BytesKeyedBucketOrds.build(bigArrays, cardinality);
        this.numFields = numFields;
        this.scratch = new byte[numFields * Long.BYTES];
        this.scratchRef = new BytesRef(scratch);
    }

    /**
     * Pack ordinals into the scratch buffer and return the wrapping BytesRef.
     */
    private BytesRef packOrdinals(long[] ordinals) {
        assert ordinals.length == numFields;
        for (int i = 0; i < numFields; i++) {
            long v = ordinals[i];
            int off = i * Long.BYTES;
            scratch[off] = (byte) (v >>> 56);
            scratch[off + 1] = (byte) (v >>> 48);
            scratch[off + 2] = (byte) (v >>> 40);
            scratch[off + 3] = (byte) (v >>> 32);
            scratch[off + 4] = (byte) (v >>> 24);
            scratch[off + 5] = (byte) (v >>> 16);
            scratch[off + 6] = (byte) (v >>> 8);
            scratch[off + 7] = (byte) v;
        }
        return scratchRef;
    }

    /**
     * Unpack ordinals from a BytesRef that was produced by {@link #packOrdinals}.
     */
    static long[] unpackOrdinals(BytesRef packed, int numFields) {
        assert packed.length == numFields * Long.BYTES;
        long[] ordinals = new long[numFields];
        for (int i = 0; i < numFields; i++) {
            int off = packed.offset + i * Long.BYTES;
            ordinals[i] = ((long) (packed.bytes[off] & 0xFF) << 56) | ((long) (packed.bytes[off + 1] & 0xFF) << 48)
                | ((long) (packed.bytes[off + 2] & 0xFF) << 40) | ((long) (packed.bytes[off + 3] & 0xFF) << 32) | ((long) (packed.bytes[off
                    + 4] & 0xFF) << 24) | ((long) (packed.bytes[off + 5] & 0xFF) << 16) | ((long) (packed.bytes[off + 6] & 0xFF) << 8)
                | ((long) (packed.bytes[off + 7] & 0xFF));
        }
        return ordinals;
    }

    @Override
    public long add(long owningBucketOrd, long[] ordinals) {
        return delegate.add(owningBucketOrd, packOrdinals(ordinals));
    }

    @Override
    public long bucketsInOrd(long owningBucketOrd) {
        return delegate.bucketsInOrd(owningBucketOrd);
    }

    @Override
    public long size() {
        return delegate.size();
    }

    @Override
    public BucketOrdsEnum ordsEnum(long owningBucketOrd) {
        BytesKeyedBucketOrds.BucketOrdsEnum inner = delegate.ordsEnum(owningBucketOrd);
        int fields = this.numFields;
        return new BucketOrdsEnum() {
            private final BytesRef readBuffer = new BytesRef();

            @Override
            public boolean next() {
                return inner.next();
            }

            @Override
            public long ord() {
                return inner.ord();
            }

            @Override
            public long[] ordinals() {
                inner.readValue(readBuffer);
                return unpackOrdinals(readBuffer, fields);
            }
        };
    }

    @Override
    public void close() {
        delegate.close();
    }
}
