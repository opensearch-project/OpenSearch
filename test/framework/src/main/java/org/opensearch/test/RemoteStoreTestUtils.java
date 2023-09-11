/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.apache.lucene.util.Version;
import org.opensearch.common.UUIDs;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import static org.opensearch.test.OpenSearchTestCase.randomIntBetween;

/**
 * Utilities for remote store related operations used across one or more tests.
 */
public final class RemoteStoreTestUtils {

    private RemoteStoreTestUtils() {

    }

    /**
     * Prepares metadata file bytes with header and footer
     *
     * @param segmentFilesMap: actual metadata content
     * @return ByteArrayIndexInput: metadata file bytes with header and footer
     * @throws IOException IOException
     */
    public static InputStream createMetadataFileBytes(
        Map<String, String> segmentFilesMap,
        ReplicationCheckpoint replicationCheckpoint,
        SegmentInfos segmentInfos
    ) throws IOException {
        ByteBuffersDataOutput byteBuffersIndexOutput = new ByteBuffersDataOutput();
        segmentInfos.write(new ByteBuffersIndexOutput(byteBuffersIndexOutput, "", ""));
        byte[] byteArray = byteBuffersIndexOutput.toArrayCopy();

        BytesStreamOutput output = new BytesStreamOutput();
        OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput("segment metadata", "metadata output stream", output, 4096);
        CodecUtil.writeHeader(indexOutput, RemoteSegmentMetadata.METADATA_CODEC, RemoteSegmentMetadata.CURRENT_VERSION);
        indexOutput.writeMapOfStrings(segmentFilesMap);
        RemoteSegmentMetadata.writeCheckpointToIndexOutput(replicationCheckpoint, indexOutput);
        indexOutput.writeLong(byteArray.length);
        indexOutput.writeBytes(byteArray, byteArray.length);
        CodecUtil.writeFooter(indexOutput);
        indexOutput.close();
        return new ByteArrayInputStream(BytesReference.toBytes(output.bytes()));
    }

    public static Map<String, String> getDummyMetadata(String prefix, int commitGeneration) {
        Map<String, String> metadata = new HashMap<>();

        metadata.put(
            prefix + ".cfe",
            prefix
                + ".cfe::"
                + prefix
                + ".cfe__"
                + UUIDs.base64UUID()
                + "::"
                + randomIntBetween(1000, 5000)
                + "::"
                + randomIntBetween(512000, 1024000)
                + "::"
                + Version.MIN_SUPPORTED_MAJOR
        );
        metadata.put(
            prefix + ".cfs",
            prefix
                + ".cfs::"
                + prefix
                + ".cfs__"
                + UUIDs.base64UUID()
                + "::"
                + randomIntBetween(1000, 5000)
                + "::"
                + randomIntBetween(512000, 1024000)
                + "::"
                + Version.MIN_SUPPORTED_MAJOR
        );
        metadata.put(
            prefix + ".si",
            prefix
                + ".si::"
                + prefix
                + ".si__"
                + UUIDs.base64UUID()
                + "::"
                + randomIntBetween(1000, 5000)
                + "::"
                + randomIntBetween(512000, 1024000)
                + "::"
                + Version.LATEST.major
        );
        metadata.put(
            "segments_" + commitGeneration,
            "segments_"
                + commitGeneration
                + "::segments_"
                + commitGeneration
                + "__"
                + UUIDs.base64UUID()
                + "::"
                + randomIntBetween(1000, 5000)
                + "::"
                + randomIntBetween(1024, 5120)
                + "::"
                + Version.LATEST.major
        );
        return metadata;
    }
}
