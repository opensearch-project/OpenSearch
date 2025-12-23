/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.opensearch.common.collect.Tuple;
import org.opensearch.index.remote.RemoteStoreUtils;

import java.util.Objects;

/**
 * Contains utility methods for metadata filename handling
 *
 * @opensearch.internal
 */
public class MetadataFilenameUtils {
    public static final String SEPARATOR = "__";
    public static final String METADATA_PREFIX = "metadata";

    public static String getMetadataFilePrefixForCommit(long primaryTerm, long generation) {
        return String.join(SEPARATOR, METADATA_PREFIX,
            RemoteStoreUtils.invertLong(primaryTerm), RemoteStoreUtils.invertLong(generation));
    }

    public static String getMetadataFilename(long primaryTerm, long generation, long translogGeneration,
                                            long uploadCounter, int metadataVersion, String nodeId) {
        return getMetadataFilename(primaryTerm, generation, translogGeneration, uploadCounter,
            metadataVersion, nodeId, System.currentTimeMillis());
    }

    public static String getMetadataFilename(long primaryTerm, long generation, long translogGeneration,
                                            long uploadCounter, int metadataVersion, String nodeId,
                                            long creationTimestamp) {
        return String.join(SEPARATOR,
            METADATA_PREFIX,
            RemoteStoreUtils.invertLong(primaryTerm),
            RemoteStoreUtils.invertLong(generation),
            RemoteStoreUtils.invertLong(translogGeneration),
            RemoteStoreUtils.invertLong(uploadCounter),
            String.valueOf(Objects.hash(nodeId)),
            RemoteStoreUtils.invertLong(creationTimestamp),
            String.valueOf(metadataVersion)
        );
    }

    public static long getTimestamp(String filename) {
        String[] filenameTokens = filename.split(SEPARATOR);
        return RemoteStoreUtils.invertLong(filenameTokens[filenameTokens.length - 2]);
    }

    public static Tuple<String, String> getNodeIdByPrimaryTermAndGen(String filename) {
        String[] tokens = filename.split(SEPARATOR);
        if (tokens.length < 8) {
            return null; // For versions < 2.11, we don't have node id
        }
        String primaryTermAndGen = String.join(SEPARATOR, tokens[1], tokens[2], tokens[3]);
        String nodeId = tokens[5];
        return new Tuple<>(primaryTermAndGen, nodeId);
    }

    // Visible for testing
    public static long getPrimaryTerm(String[] filenameTokens) {
        return RemoteStoreUtils.invertLong(filenameTokens[1]);
    }

    // Visible for testing
    public static long getGeneration(String[] filenameTokens) {
        return RemoteStoreUtils.invertLong(filenameTokens[2]);
    }
}
