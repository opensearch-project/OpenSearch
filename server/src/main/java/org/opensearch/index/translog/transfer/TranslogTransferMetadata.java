/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.common.SetOnce;
import org.opensearch.index.remote.RemoteStoreUtils;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/**
 * The metadata associated with every transfer {@link TransferSnapshot}. The metadata is uploaded at the end of the
 * tranlog and generational checkpoint uploads to mark the latest generation and the translog/checkpoint files that are
 * still referenced by the last checkpoint.
 *
 * @opensearch.internal
 */
public class TranslogTransferMetadata {

    private final long primaryTerm;

    private final long generation;

    private final long minTranslogGeneration;

    private int count;

    private final SetOnce<Map<String, String>> generationToPrimaryTermMapper = new SetOnce<>();

    public static final String METADATA_SEPARATOR = "__";

    public static final String METADATA_PREFIX = "metadata";

    static final int BUFFER_SIZE = 4096;

    static final int CURRENT_VERSION = 1;

    static final String METADATA_CODEC = "md";

    private final long createdAt;

    public TranslogTransferMetadata(long primaryTerm, long generation, long minTranslogGeneration, int count) {
        this.primaryTerm = primaryTerm;
        this.generation = generation;
        this.minTranslogGeneration = minTranslogGeneration;
        this.count = count;
        this.createdAt = System.currentTimeMillis();
    }

    public long getPrimaryTerm() {
        return primaryTerm;
    }

    public long getGeneration() {
        return generation;
    }

    public long getMinTranslogGeneration() {
        return minTranslogGeneration;
    }

    public int getCount() {
        return count;
    }

    public void setGenerationToPrimaryTermMapper(Map<String, String> generationToPrimaryTermMap) {
        generationToPrimaryTermMapper.set(generationToPrimaryTermMap);
    }

    public Map<String, String> getGenerationToPrimaryTermMapper() {
        return generationToPrimaryTermMapper.get();
    }

    /*
    This should be used only at the time of creation.
     */
    public String getFileName() {
        return String.join(
            METADATA_SEPARATOR,
            Arrays.asList(
                METADATA_PREFIX,
                RemoteStoreUtils.invertLong(primaryTerm),
                RemoteStoreUtils.invertLong(generation),
                RemoteStoreUtils.invertLong(createdAt),
                String.valueOf(CURRENT_VERSION)
            )
        );
    }

    @Override
    public int hashCode() {
        return Objects.hash(primaryTerm, generation);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TranslogTransferMetadata other = (TranslogTransferMetadata) o;
        return Objects.equals(this.primaryTerm, other.primaryTerm) && Objects.equals(this.generation, other.generation);
    }
}
