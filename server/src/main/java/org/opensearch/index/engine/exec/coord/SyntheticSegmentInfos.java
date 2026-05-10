/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Builds the Lucene {@code SegmentInfos} payload uploaded to the remote store by the DFA primary.
 *
 * <p>Uses the provided in-memory {@link SegmentInfos} as the base when non-null so uploaded bytes
 * carry real Lucene segment references; otherwise falls back to an empty base. In both cases the
 * snapshot userData plus the serialized DFA catalog are layered into the result's {@code userData}.
 */
final class SyntheticSegmentInfos {

    private SyntheticSegmentInfos() {}

    /** Equivalent to {@code serialize(snapshot, null)}. */
    static byte[] serialize(DataformatAwareCatalogSnapshot snapshot) throws IOException {
        return serialize(snapshot, null);
    }

    /**
     * Serializes {@code snapshot} using {@code base} (if non-null) as the base {@link SegmentInfos}.
     * A non-null base preserves real Lucene segment references across the remote-store round-trip.
     */
    static byte[] serialize(DataformatAwareCatalogSnapshot snapshot, SegmentInfos base) throws IOException {
        SegmentInfos segmentInfos = (base != null) ? base.clone() : new SegmentInfos(Version.LATEST.major);

        Map<String, String> userData = new HashMap<>(segmentInfos.getUserData());
        userData.putAll(snapshot.getUserData());
        userData.put(CatalogSnapshot.CATALOG_SNAPSHOT_KEY, snapshot.serializeToString());
        segmentInfos.setUserData(userData, false);
        segmentInfos.setNextWriteGeneration(snapshot.getLastCommitGeneration());

        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        segmentInfos.write(new ByteBuffersIndexOutput(out, "DFA upload SegmentInfos", "DFA upload SegmentInfos"));
        return out.toArrayCopy();
    }
}
