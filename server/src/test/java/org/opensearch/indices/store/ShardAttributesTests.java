/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.store;

import org.opensearch.core.common.io.stream.DataOutputStreamOutput;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class ShardAttributesTests extends OpenSearchTestCase {

    Index index = new Index("index", "test-uid");
    ShardId shardId = new ShardId(index, 0);
    String customDataPath = "/path/to/data";

    public void testShardAttributesConstructor() {
        ShardAttributes attributes = new ShardAttributes(customDataPath);
        assertEquals(attributes.getCustomDataPath(), customDataPath);
    }

    public void testSerialization() throws IOException {
        ShardAttributes attributes1 = new ShardAttributes(customDataPath);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        StreamOutput output = new DataOutputStreamOutput(new DataOutputStream(bytes));
        attributes1.writeTo(output);
        output.close();
        StreamInput input = new InputStreamStreamInput(new ByteArrayInputStream(bytes.toByteArray()));
        ShardAttributes attributes2 = new ShardAttributes(input);
        input.close();
        assertEquals(attributes1.getCustomDataPath(), attributes2.getCustomDataPath());
    }

}
