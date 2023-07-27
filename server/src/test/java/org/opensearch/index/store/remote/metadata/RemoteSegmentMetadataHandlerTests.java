/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.metadata;

import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.apache.lucene.util.Version;
import org.junit.After;
import org.junit.Before;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.UUIDs;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.NRTReplicationEngineFactory;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.index.store.Store;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.ReplicationType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Unit Tests for {@link RemoteSegmentMetadataHandler}
 */
public class RemoteSegmentMetadataHandlerTests extends IndexShardTestCase {
    private RemoteSegmentMetadataHandler remoteSegmentMetadataHandler;
    private IndexShard indexShard;
    private SegmentInfos segmentInfos;

    private ReplicationCheckpoint replicationCheckpoint;

    @Before
    public void setup() throws IOException {
        remoteSegmentMetadataHandler = new RemoteSegmentMetadataHandler();

        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
            .build();

        indexShard = newStartedShard(false, indexSettings, new NRTReplicationEngineFactory());
        try (Store store = indexShard.store()) {
            segmentInfos = store.readLastCommittedSegmentsInfo();
        }
        replicationCheckpoint = indexShard.getLatestReplicationCheckpoint();
    }

    @After
    public void tearDown() throws Exception {
        indexShard.close("test tearDown", true, false);
        super.tearDown();
    }

    public void testReadContentNoSegmentInfos() throws IOException {
        BytesStreamOutput output = new BytesStreamOutput();
        OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput("dummy bytes", "dummy stream", output, 4096);
        Map<String, String> expectedOutput = getDummyData();
        indexOutput.writeMapOfStrings(expectedOutput);
        RemoteSegmentMetadata.writeCheckpointToIndexOutput(replicationCheckpoint, indexOutput);
        indexOutput.writeLong(0);
        indexOutput.writeBytes(new byte[0], 0);
        indexOutput.close();
        RemoteSegmentMetadata metadata = remoteSegmentMetadataHandler.readContent(
            new ByteArrayIndexInput("dummy bytes", BytesReference.toBytes(output.bytes()))
        );
        assertEquals(expectedOutput, metadata.toMapOfStrings());
        assertEquals(replicationCheckpoint.getSegmentsGen(), metadata.getGeneration());
    }

    public void testReadContentWithSegmentInfos() throws IOException {
        BytesStreamOutput output = new BytesStreamOutput();
        OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput("dummy bytes", "dummy stream", output, 4096);
        Map<String, String> expectedOutput = getDummyData();
        indexOutput.writeMapOfStrings(expectedOutput);
        RemoteSegmentMetadata.writeCheckpointToIndexOutput(replicationCheckpoint, indexOutput);
        ByteBuffersIndexOutput segmentInfosOutput = new ByteBuffersIndexOutput(new ByteBuffersDataOutput(), "test", "resource");
        segmentInfos.write(segmentInfosOutput);
        byte[] segmentInfosBytes = segmentInfosOutput.toArrayCopy();
        indexOutput.writeLong(segmentInfosBytes.length);
        indexOutput.writeBytes(segmentInfosBytes, 0, segmentInfosBytes.length);
        indexOutput.close();
        RemoteSegmentMetadata metadata = remoteSegmentMetadataHandler.readContent(
            new ByteArrayIndexInput("dummy bytes", BytesReference.toBytes(output.bytes()))
        );
        assertEquals(expectedOutput, metadata.toMapOfStrings());
        assertEquals(replicationCheckpoint.getSegmentsGen(), metadata.getGeneration());
        assertArrayEquals(segmentInfosBytes, metadata.getSegmentInfosBytes());
    }

    public void testWriteContent() throws IOException {
        BytesStreamOutput output = new BytesStreamOutput();
        OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput("dummy bytes", "dummy stream", output, 4096);

        Map<String, String> expectedOutput = getDummyData();
        ByteBuffersIndexOutput segmentInfosOutput = new ByteBuffersIndexOutput(new ByteBuffersDataOutput(), "test", "resource");
        segmentInfos.write(segmentInfosOutput);
        byte[] segmentInfosBytes = segmentInfosOutput.toArrayCopy();

        RemoteSegmentMetadata remoteSegmentMetadata = new RemoteSegmentMetadata(
            RemoteSegmentMetadata.fromMapOfStrings(expectedOutput),
            segmentInfosBytes,
            indexShard.getLatestReplicationCheckpoint()
        );
        remoteSegmentMetadataHandler.writeContent(indexOutput, remoteSegmentMetadata);
        indexOutput.close();

        RemoteSegmentMetadata metadata = remoteSegmentMetadataHandler.readContent(
            new ByteArrayIndexInput("dummy bytes", BytesReference.toBytes(output.bytes()))
        );
        assertEquals(expectedOutput, metadata.toMapOfStrings());
        assertEquals(replicationCheckpoint.getSegmentsGen(), metadata.getGeneration());
        assertEquals(replicationCheckpoint.getPrimaryTerm(), metadata.getPrimaryTerm());
        assertArrayEquals(segmentInfosBytes, metadata.getSegmentInfosBytes());
    }

    private Map<String, String> getDummyData() {
        Map<String, String> expectedOutput = new HashMap<>();
        String prefix = "_0";
        expectedOutput.put(
            prefix + ".cfe",
            prefix
                + ".cfe::"
                + prefix
                + ".cfe__"
                + UUIDs.base64UUID()
                + "::"
                + randomIntBetween(1000, 5000)
                + "::"
                + randomIntBetween(1024, 2048)
                + "::"
                + Version.LATEST.major
        );
        expectedOutput.put(
            prefix + ".cfs",
            prefix
                + ".cfs::"
                + prefix
                + ".cfs__"
                + UUIDs.base64UUID()
                + "::"
                + randomIntBetween(1000, 5000)
                + "::"
                + randomIntBetween(1024, 2048)
                + "::"
                + Version.LATEST.major
        );
        return expectedOutput;
    }
}
