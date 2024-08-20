/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.model;

import org.opensearch.common.compress.DeflateCompressor;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.compress.Compressor;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RemotePinnedTimestampsTests extends OpenSearchTestCase {

    private RemotePinnedTimestamps remotePinnedTimestamps;

    @Before
    public void setup() {
        Compressor compressor = new DeflateCompressor();
        remotePinnedTimestamps = new RemotePinnedTimestamps("testClusterUUID", compressor);
    }

    public void testGenerateBlobFileName() {
        String fileName = remotePinnedTimestamps.generateBlobFileName();
        assertTrue(fileName.startsWith(RemotePinnedTimestamps.PINNED_TIMESTAMPS));
        assertEquals(fileName, remotePinnedTimestamps.getBlobFileName());
    }

    public void testSerializeAndDeserialize() throws IOException {
        RemotePinnedTimestamps.PinnedTimestamps pinnedTimestamps = new RemotePinnedTimestamps.PinnedTimestamps(new HashMap<>());
        pinnedTimestamps.pin(1000L, "entity1");
        pinnedTimestamps.pin(2000L, "entity2");
        remotePinnedTimestamps.setPinnedTimestamps(pinnedTimestamps);

        InputStream serialized = remotePinnedTimestamps.serialize();
        RemotePinnedTimestamps.PinnedTimestamps deserialized = remotePinnedTimestamps.deserialize(serialized);

        assertEquals(pinnedTimestamps.getPinnedTimestampPinningEntityMap(), deserialized.getPinnedTimestampPinningEntityMap());
    }

    public void testSetAndGetPinnedTimestamps() {
        RemotePinnedTimestamps.PinnedTimestamps pinnedTimestamps = new RemotePinnedTimestamps.PinnedTimestamps(new HashMap<>());
        remotePinnedTimestamps.setPinnedTimestamps(pinnedTimestamps);
        assertEquals(pinnedTimestamps, remotePinnedTimestamps.getPinnedTimestamps());
    }

    public void testPinnedTimestampsPin() {
        RemotePinnedTimestamps.PinnedTimestamps pinnedTimestamps = new RemotePinnedTimestamps.PinnedTimestamps(new HashMap<>());
        pinnedTimestamps.pin(1000L, "entity1");
        pinnedTimestamps.pin(1000L, "entity2");
        pinnedTimestamps.pin(2000L, "entity3");

        Map<Long, List<String>> expected = new HashMap<>();
        expected.put(1000L, Arrays.asList("entity1", "entity2"));
        expected.put(2000L, List.of("entity3"));

        assertEquals(expected, pinnedTimestamps.getPinnedTimestampPinningEntityMap());
    }

    public void testPinnedTimestampsUnpin() {
        RemotePinnedTimestamps.PinnedTimestamps pinnedTimestamps = new RemotePinnedTimestamps.PinnedTimestamps(new HashMap<>());
        pinnedTimestamps.pin(1000L, "entity1");
        pinnedTimestamps.pin(1000L, "entity2");
        pinnedTimestamps.pin(2000L, "entity3");

        pinnedTimestamps.unpin(1000L, "entity1");
        pinnedTimestamps.unpin(2000L, "entity3");

        Map<Long, List<String>> expected = new HashMap<>();
        expected.put(1000L, List.of("entity2"));

        assertEquals(expected, pinnedTimestamps.getPinnedTimestampPinningEntityMap());
    }

    public void testPinnedTimestampsReadFromAndWriteTo() throws IOException {
        RemotePinnedTimestamps.PinnedTimestamps original = new RemotePinnedTimestamps.PinnedTimestamps(new HashMap<>());
        original.pin(1000L, "entity1");
        original.pin(2000L, "entity2");

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = new BytesStreamInput(out.bytes().toBytesRef().bytes);
        RemotePinnedTimestamps.PinnedTimestamps deserialized = RemotePinnedTimestamps.PinnedTimestamps.readFrom(in);

        assertEquals(original.getPinnedTimestampPinningEntityMap(), deserialized.getPinnedTimestampPinningEntityMap());
    }
}
