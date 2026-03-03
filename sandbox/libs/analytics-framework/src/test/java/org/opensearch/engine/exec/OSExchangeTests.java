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
import java.util.ArrayList;
import java.util.List;

public class OSExchangeTests extends OpenSearchTestCase {

    private Schema testSchema() {
        return new Schema(List.of(new Field("col", FieldType.nullable(new ArrowType.Utf8()), null)));
    }

    private OSScan testChild() {
        return new OSScan("test_index", List.of("col"), testSchema());
    }

    public void testConstructionAndAccessors() {
        OSScan child = testChild();
        List<Integer> hashCols = List.of(0, 1);

        OSExchange exchange = new OSExchange(child, Distribution.HASH, hashCols);

        assertSame(child, exchange.child());
        assertEquals(Distribution.HASH, exchange.distribution());
        assertEquals(List.of(0, 1), exchange.hashColumns());
    }

    public void testOutputSchemaDelegatesToChild() {
        OSScan child = testChild();
        OSExchange exchange = new OSExchange(child, Distribution.SINGLETON, List.of());

        assertSame(child.outputSchema(), exchange.outputSchema());
    }

    public void testChildrenReturnsSingleChild() {
        OSScan child = testChild();
        OSExchange exchange = new OSExchange(child, Distribution.SINGLETON, List.of());

        assertEquals(1, exchange.children().size());
        assertSame(child, exchange.children().get(0));
    }

    public void testHashColumnsDefensivelyCopied() {
        List<Integer> cols = new ArrayList<>(List.of(0, 1));
        OSExchange exchange = new OSExchange(testChild(), Distribution.HASH, cols);

        // Mutate the original list
        cols.add(2);

        // Exchange should still have original 2 elements
        assertEquals(2, exchange.hashColumns().size());
    }

    public void testHashColumnsUnmodifiable() {
        OSExchange exchange = new OSExchange(testChild(), Distribution.HASH, List.of(0));
        expectThrows(UnsupportedOperationException.class, () -> exchange.hashColumns().add(1));
    }

    public void testSingletonDistribution() {
        OSExchange exchange = new OSExchange(testChild(), Distribution.SINGLETON, List.of());
        assertEquals(Distribution.SINGLETON, exchange.distribution());
        assertTrue(exchange.hashColumns().isEmpty());
    }

    public void testVisitorDispatch() {
        OSExchange exchange = new OSExchange(testChild(), Distribution.SINGLETON, List.of());
        ExecNodeVisitor<String, Void> visitor = new ExecNodeVisitor<>() {
            @Override
            public String visitExchange(OSExchange node, Void context) {
                return "visited_exchange";
            }
        };
        assertEquals("visited_exchange", exchange.accept(visitor, null));
    }

    public void testVisitorDefaultThrows() {
        OSExchange exchange = new OSExchange(testChild(), Distribution.SINGLETON, List.of());
        ExecNodeVisitor<String, Void> visitor = new ExecNodeVisitor<>() {
        };
        expectThrows(UnsupportedOperationException.class, () -> exchange.accept(visitor, null));
    }

    public void testWriteToReadFromRoundTrip() throws IOException {
        OSScan child = testChild();
        OSExchange original = new OSExchange(child, Distribution.HASH, List.of(0, 1));

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        OSNode deserialized = OSNode.readFrom(in);

        assertTrue(deserialized instanceof OSExchange);
        OSExchange result = (OSExchange) deserialized;
        assertEquals(Distribution.HASH, result.distribution());
        assertEquals(List.of(0, 1), result.hashColumns());
        assertTrue(result.child() instanceof OSScan);
        assertEquals("test_index", ((OSScan) result.child()).indexName());
    }

    public void testWriteToReadFromSingletonDistribution() throws IOException {
        OSExchange original = new OSExchange(testChild(), Distribution.SINGLETON, List.of());

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        OSExchange result = (OSExchange) OSNode.readFrom(in);
        assertEquals(Distribution.SINGLETON, result.distribution());
        assertTrue(result.hashColumns().isEmpty());
    }
}
