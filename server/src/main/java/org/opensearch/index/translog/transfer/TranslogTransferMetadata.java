/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.common.SetOnce;

import java.util.Arrays;
import java.util.Comparator;
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

    static final int BUFFER_SIZE = 4096;

    static final int CURRENT_VERSION = 1;

    static final String METADATA_CODEC = "md";

    public static final Comparator<String> METADATA_FILENAME_COMPARATOR = new MetadataFilenameComparator();

    public TranslogTransferMetadata(long primaryTerm, long generation, long minTranslogGeneration, int count) {
        this.primaryTerm = primaryTerm;
        this.generation = generation;
        this.minTranslogGeneration = minTranslogGeneration;
        this.count = count;
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

    public static String getFileName(long primaryTerm, long generation) {
        return String.join(METADATA_SEPARATOR, Arrays.asList(String.valueOf(primaryTerm), String.valueOf(generation)));
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

    private static class MetadataFilenameComparator implements Comparator<String> {
        @Override
        public int compare(String first, String second) {
            // Format of metadata filename is <Primary Term>__<Generation>
            String[] filenameTokens1 = first.split(METADATA_SEPARATOR);
            String[] filenameTokens2 = second.split(METADATA_SEPARATOR);
            // Here, we are comparing only primary term and generation.
            for (int i = 0; i < filenameTokens1.length; i++) {
                if (filenameTokens1[i].equals(filenameTokens2[i]) == false) {
                    return Long.compare(Long.parseLong(filenameTokens1[i]), Long.parseLong(filenameTokens2[i]));
                }
            }
            throw new IllegalArgumentException(
                "TranslogTransferMetadata files " + first + " and " + second + " have same primary term and generation"
            );
        }
    }
}
