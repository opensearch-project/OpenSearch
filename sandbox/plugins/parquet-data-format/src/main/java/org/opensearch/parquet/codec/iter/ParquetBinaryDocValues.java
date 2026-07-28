/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.iter;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;
import org.opensearch.parquet.bridge.ParquetColumnReader;
import org.opensearch.parquet.codec.cache.PageCache;

import java.io.IOException;

/**
 * Cache-aware {@link BinaryDocValues} over a single-valued Parquet {@code BYTE_ARRAY} column.
 *
 * <p>Hot path: a presence bit-test plus a CSR-offset slice into the cached page's backing
 * byte buffer. The returned {@link BytesRef} is a single reused instance whose
 * {@code bytes}/{@code offset}/{@code length} are repointed each call (Layer 5). UTF-8 bytes
 * are preserved byte-for-byte.
 */
public final class ParquetBinaryDocValues extends BinaryDocValues {

    private final ParquetColumnReader reader;
    private final int maxDoc;
    private final BytesRef scratch = new BytesRef();

    private int doc = -1;
    private boolean currentPresent;

    public ParquetBinaryDocValues(ParquetColumnReader reader, int maxDoc) {
        this.reader = reader;
        this.maxDoc = maxDoc;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        if (target >= maxDoc) {
            doc = NO_MORE_DOCS;
            currentPresent = false;
            return false;
        }
        doc = target;
        PageCache cache = reader.cache();
        if (cache != null && target <= cache.lastRow && target >= cache.firstRow) {
            // Layer 1/2 hit — served from the resident page, no FFM crossing.
        } else {
            // Layer 1/2 miss — load the page containing this row (Layer 3 → 4 → FFM).
            reader.loadPageContaining(target);
            cache = reader.cache();
            if (cache == null) { // Layer 4: page is all-nulls.
                currentPresent = false;
                return false;
            }
        }
        currentPresent = cache.isPresent(target);
        if (currentPresent) {
            int rel = (int) (target - cache.firstRow);
            int start = cache.byteOffsets[rel];
            int end = cache.byteOffsets[rel + 1];
            scratch.bytes = cache.byteBuf;
            scratch.offset = start;
            scratch.length = end - start;
        }
        return currentPresent;
    }

    @Override
    public BytesRef binaryValue() {
        return scratch;
    }

    @Override
    public int docID() {
        return doc;
    }

    @Override
    public int nextDoc() throws IOException {
        return advance(doc + 1);
    }

    @Override
    public int advance(int target) throws IOException {
        for (int d = target; d < maxDoc; d++) {
            if (advanceExact(d)) {
                doc = d;
                return d;
            }
        }
        doc = NO_MORE_DOCS;
        return NO_MORE_DOCS;
    }

    @Override
    public long cost() {
        return maxDoc;
    }
}
