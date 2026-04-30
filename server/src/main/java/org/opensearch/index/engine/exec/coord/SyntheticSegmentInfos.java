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
 * Builds the synthetic Lucene {@code SegmentInfos} payload the DFA primary uploads to the remote
 * store. The synthetic {@code SegmentInfos} has zero segment entries — it is a transport envelope
 * whose {@code userData} carries the serialized {@link DataformatAwareCatalogSnapshot} plus any
 * engine-side keys the snapshot already holds (history/translog UUIDs, checkpoints, etc. — set
 * by flush / by the upload listener's pre-upload userData overlay).
 *
 * <p>All Lucene-codec dependencies are confined to this class so
 * {@link DataformatAwareCatalogSnapshot} can stay a pure data carrier. The generation used for
 * {@code setNextWriteGeneration} is the Lucene {@code segments_N} generation from the snapshot's
 * {@link CatalogSnapshot#getLastCommitGeneration()}, ensuring the replica writes the correct
 * {@code segments_N} file on commit.</p>
 */
final class SyntheticSegmentInfos {

    private SyntheticSegmentInfos() {}

    static byte[] serialize(DataformatAwareCatalogSnapshot snapshot) throws IOException {
        SegmentInfos segmentInfos = new SegmentInfos(Version.LATEST.major);
        Map<String, String> userData = new HashMap<>(snapshot.getUserData());
        userData.put(CatalogSnapshot.CATALOG_SNAPSHOT_KEY, snapshot.serializeToString());
        segmentInfos.setUserData(userData, false);
        segmentInfos.setNextWriteGeneration(snapshot.getLastCommitGeneration());
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        segmentInfos.write(new ByteBuffersIndexOutput(out, "synthetic SegmentInfos", "synthetic SegmentInfos"));
        return out.toArrayCopy();
    }
}
