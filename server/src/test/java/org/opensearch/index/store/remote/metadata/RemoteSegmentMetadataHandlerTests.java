/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.metadata;

import org.apache.lucene.store.OutputStreamIndexOutput;
import org.junit.Before;
import org.opensearch.common.UUIDs;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Unit Tests for {@link RemoteSegmentMetadataHandler}
 */
public class RemoteSegmentMetadataHandlerTests extends OpenSearchTestCase {
    private RemoteSegmentMetadataHandler remoteSegmentMetadataHandler;

    @Before
    public void setup() throws IOException {
        remoteSegmentMetadataHandler = new RemoteSegmentMetadataHandler();
    }

    public void testReadContent() throws IOException {
        BytesStreamOutput output = new BytesStreamOutput();
        OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput("dummy bytes", "dummy stream", output, 4096);
        Map<String, String> expectedOutput = getDummyData();
        indexOutput.writeMapOfStrings(expectedOutput);
        indexOutput.close();
        RemoteSegmentMetadata metadata = remoteSegmentMetadataHandler.readContent(
            new ByteArrayIndexInput("dummy bytes", BytesReference.toBytes(output.bytes()))
        );
        assertEquals(expectedOutput, metadata.toMapOfStrings());
    }

    public void testWriteContent() throws IOException {
        BytesStreamOutput output = new BytesStreamOutput();
        OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput("dummy bytes", "dummy stream", output, 4096);
        Map<String, String> expectedOutput = getDummyData();
        remoteSegmentMetadataHandler.writeContent(indexOutput, RemoteSegmentMetadata.fromMapOfStrings(expectedOutput));
        indexOutput.close();
        Map<String, String> actualOutput = new ByteArrayIndexInput("dummy bytes", BytesReference.toBytes(output.bytes()))
            .readMapOfStrings();
        assertEquals(expectedOutput, actualOutput);
    }

    private Map<String, String> getDummyData() {
        Map<String, String> expectedOutput = new HashMap<>();
        String prefix = "_0";
        expectedOutput.put(
            prefix + ".cfe",
            prefix + ".cfe::" + prefix + ".cfe__" + UUIDs.base64UUID() + "::" + randomIntBetween(1000, 5000)
        );
        expectedOutput.put(
            prefix + ".cfs",
            prefix + ".cfs::" + prefix + ".cfs__" + UUIDs.base64UUID() + "::" + randomIntBetween(1000, 5000)
        );
        return expectedOutput;
    }
}
