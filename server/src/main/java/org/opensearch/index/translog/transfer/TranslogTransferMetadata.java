/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.Version;
import org.opensearch.common.SetOnce;
import org.opensearch.common.collect.Tuple;
import org.opensearch.index.remote.RemoteStoreUtils;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The metadata associated with every transfer {@link TransferSnapshot}. The metadata is uploaded at the end of the
 * tranlog and generational checkpoint uploads to mark the latest generation and the translog/checkpoint files that are
 * still referenced by the last checkpoint.
 *
 * @opensearch.internal
 */
public class TranslogTransferMetadata {

    public static final Logger logger = LogManager.getLogger(TranslogTransferMetadata.class);

    private final long primaryTerm;

    private final long generation;

    private final long minTranslogGeneration;

    private final int count;

    private final SetOnce<Map<String, String>> generationToPrimaryTermMapper = new SetOnce<>();

    public static final String METADATA_SEPARATOR = "__";

    public static final String METADATA_PREFIX = "metadata";

    static final int BUFFER_SIZE = 4096;

    static final int CURRENT_VERSION = 1;

    static final String METADATA_CODEC = "md";

    private final long createdAt;

    private final String nodeId;

    public TranslogTransferMetadata(long primaryTerm, long generation, long minTranslogGeneration, int count, String nodeId) {
        this.primaryTerm = primaryTerm;
        this.generation = generation;
        this.minTranslogGeneration = minTranslogGeneration;
        this.count = count;
        this.createdAt = System.currentTimeMillis();
        this.nodeId = nodeId;
    }

    /*
    Used only at the time of download . Since details are read from content , nodeId is not available
     */
    public TranslogTransferMetadata(long primaryTerm, long generation, long minTranslogGeneration, int count) {
        this(primaryTerm, generation, minTranslogGeneration, count, "");
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
                String.valueOf(Objects.hash(nodeId)),
                RemoteStoreUtils.invertLong(minTranslogGeneration),
                String.valueOf(getMinPrimaryTermReferred()),
                String.valueOf(CURRENT_VERSION)
            )
        );
    }

    private long getMinPrimaryTermReferred() {
        if (generationToPrimaryTermMapper.get() == null || generationToPrimaryTermMapper.get().values().isEmpty()) {
            return -1;
        }
        Optional<Long> minPrimaryTerm = generationToPrimaryTermMapper.get()
            .values()
            .stream()
            .map(s -> Long.parseLong(s))
            .min(Long::compareTo);
        if (minPrimaryTerm.isPresent()) {
            return minPrimaryTerm.get();
        } else {
            return -1;
        }
    }

    public static Tuple<Tuple<Long, Long>, String> getNodeIdByPrimaryTermAndGeneration(String filename) {
        String[] tokens = filename.split(METADATA_SEPARATOR);
        if (tokens.length < 6) {
            // For versions < 2.11, we don't have node id
            return null;
        }
        return new Tuple<>(new Tuple<>(RemoteStoreUtils.invertLong(tokens[1]), RemoteStoreUtils.invertLong(tokens[2])), tokens[4]);
    }

    public static Tuple<String, String> getNodeIdByPrimaryTermAndGen(String filename) {
        String[] tokens = filename.split(METADATA_SEPARATOR);
        if (tokens.length < 6) {
            // For versions < 2.11, we don't have node id.
            return null;
        }
        String primaryTermAndGen = String.join(METADATA_SEPARATOR, tokens[1], tokens[2]);

        String nodeId = tokens[4];
        return new Tuple<>(primaryTermAndGen, nodeId);
    }

    public static Tuple<Long, Long> getMinMaxTranslogGenerationFromFilename(String filename) {
        String[] tokens = filename.split(METADATA_SEPARATOR);
        if (tokens.length < 7) {
            // For versions < 2.17, we don't have min translog generation.
            return null;
        }
        assert Version.CURRENT.onOrAfter(Version.V_2_17_0);
        try {
            // instead of direct index, we go backwards to avoid running into same separator in nodeId
            String minGeneration = tokens[tokens.length - 3];
            String maxGeneration = tokens[2];
            return new Tuple<>(RemoteStoreUtils.invertLong(minGeneration), RemoteStoreUtils.invertLong(maxGeneration));
        } catch (Exception e) {
            logger.error(() -> new ParameterizedMessage("Exception while getting min and max translog generation from: {}", filename), e);
            return null;
        }
    }

    public static Tuple<Long, Long> getMinMaxPrimaryTermFromFilename(String filename) {
        String[] tokens = filename.split(METADATA_SEPARATOR);
        if (tokens.length < 7) {
            // For versions < 2.17, we don't have min primary term.
            return null;
        }
        assert Version.CURRENT.onOrAfter(Version.V_2_17_0);
        try {
            // instead of direct index, we go backwards to avoid running into same separator in nodeId
            String minPrimaryTerm = tokens[tokens.length - 2];
            String maxPrimaryTerm = tokens[1];
            return new Tuple<>(Long.parseLong(minPrimaryTerm), RemoteStoreUtils.invertLong(maxPrimaryTerm));
        } catch (Exception e) {
            logger.error(() -> new ParameterizedMessage("Exception while getting min and max primary term from: {}", filename), e);
            return null;
        }
    }

    public static long getPrimaryTermFromFileName(String filename) {
        String[] tokens = filename.split(METADATA_SEPARATOR);
        try {
            return RemoteStoreUtils.invertLong(tokens[1]);
        } catch (Exception e) {
            logger.error(() -> new ParameterizedMessage("Exception while getting max primary term from: {}", filename), e);
            return -1;
        }
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
