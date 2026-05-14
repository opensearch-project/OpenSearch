/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.index.translog.Translog;

import java.util.Objects;

/**
 * Extends {@link IndexVersionValue} with a writer generation for data-format-aware engines.
 * The writer generation identifies which writer (and thus which Parquet file / Lucene segment)
 * the document belongs to, used by the delete path to route deletes to the correct writer.
 *
 * @opensearch.internal
 */
final class DataFormatVersionValue extends IndexVersionValue {

    private static final long RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DataFormatVersionValue.class);

    final long writerGeneration;

    DataFormatVersionValue(Translog.Location translogLocation, long version, long seqNo, long term, long writerGeneration) {
        super(translogLocation, version, seqNo, term);
        this.writerGeneration = writerGeneration;
    }

    @Override
    public long ramBytesUsed() {
        return RAM_BYTES_USED;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        DataFormatVersionValue that = (DataFormatVersionValue) o;
        return writerGeneration == that.writerGeneration;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), writerGeneration);
    }

    @Override
    public String toString() {
        return "DataFormatVersionValue{"
            + "version="
            + version
            + ", seqNo="
            + seqNo
            + ", term="
            + term
            + ", writerGeneration="
            + writerGeneration
            + '}';
    }
}
