/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.apache.lucene.util.Version;
import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Converts a DFA format-version string to a Lucene Version.
 * Returns Version.LATEST for null/empty/non-Lucene strings.
 * Used only at 3 legacy boundaries (Store.loadMetadata, RemoteStoreReplicationSource,
 * RemoteSegmentStoreDirectory) where writtenBy is an inert stamp.
 */
@ExperimentalApi
public final class LuceneVersionConverter {

    private LuceneVersionConverter() {}

    /**
     * Parses a format-version string as a Lucene {@link Version}, falling back to
     * {@link Version#LATEST} for null, empty, or unparseable values.
     */
    public static Version toLuceneOrLatest(String formatVersion) {
        if (formatVersion == null || formatVersion.isEmpty()) {
            return Version.LATEST;
        }
        try {
            return Version.parse(formatVersion);
        } catch (Exception e) {
            return Version.LATEST;
        }
    }
}
