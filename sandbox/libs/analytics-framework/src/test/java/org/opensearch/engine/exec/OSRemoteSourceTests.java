/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.engine.exec;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

public class OSRemoteSourceTests extends OpenSearchTestCase {

    private Schema testSchema() {
        return new Schema(List.of(new Field("col", FieldType.nullable(new ArrowType.Utf8()), null)));
    }

    public void testConstructionAndAccessors() {
        Schema schema = testSchema();
        OSRemoteSource remoteSource = new OSRemoteSource(42, schema);

        assertEquals(42, remoteSource.upstreamStageId());
        assertSame(schema, remoteSource.outputSchema());
    }

    public void testIsLeafNode() {
        OSRemoteSource remoteSource = new OSRemoteSource(0, testSchema());
        assertTrue(remoteSource.children().isEmpty());
    }

    public void testChildrenListIsUnmodifiable() {
        OSRemoteSource remoteSource = new OSRemoteSource(0, testSchema());
        expectThrows(UnsupportedOperationException.class, () -> remoteSource.children().add(new OSScan("x", List.of(), testSchema())));
    }

    public void testVisitorDispatch() {
        OSRemoteSource remoteSource = new OSRemoteSource(0, testSchema());
        ExecNodeVisitor<String, Void> visitor = new ExecNodeVisitor<>() {
            @Override
            public String visitRemoteSource(OSRemoteSource node, Void context) {
                return "visited_remote_source";
            }
        };
        assertEquals("visited_remote_source", remoteSource.accept(visitor, null));
    }

    public void testVisitorDefaultThrows() {
        OSRemoteSource remoteSource = new OSRemoteSource(0, testSchema());
        ExecNodeVisitor<String, Void> visitor = new ExecNodeVisitor<>() {
        };
        expectThrows(UnsupportedOperationException.class, () -> remoteSource.accept(visitor, null));
    }

    public void testWriteToReadFromRoundTrip() throws IOException {
        Schema schema = testSchema();
        OSRemoteSource original = new OSRemoteSource(42, schema);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        OSNode deserialized = OSNode.readFrom(in);

        assertTrue(deserialized instanceof OSRemoteSource);
        OSRemoteSource result = (OSRemoteSource) deserialized;
        assertEquals(42, result.upstreamStageId());
        assertEquals(1, result.outputSchema().getFields().size());
    }
}
