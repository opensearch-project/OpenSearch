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
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class OSNodePrinterTests extends OpenSearchTestCase {

    private Schema testSchema() {
        return new Schema(
            List.of(
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("age", FieldType.nullable(new ArrowType.Int(64, true)), null)
            )
        );
    }

    public void testPrintOSScan() {
        OSScan scan = new OSScan("test_table", List.of("name", "age"), testSchema());
        String result = ExecNodePrinter.print(scan);
        assertTrue(result.contains("OSScan"));
        assertTrue(result.contains("test_table"));
        assertTrue(result.contains("name"));
        assertTrue(result.contains("age"));
    }

    public void testPrintOSNativeEngineWithChild() {
        OSScan scan = new OSScan("test_table", List.of("name"), testSchema());
        OSNativeEngine engine = new OSNativeEngine("datafusion", new byte[] { 1, 2, 3, 4, 5 }, scan, testSchema());
        String result = ExecNodePrinter.print(engine);
        assertTrue(result.contains("OSNativeEngine"));
        assertTrue(result.contains("datafusion"));
        assertTrue(result.contains("planBytes=5"));
        // Child should be indented
        assertTrue(result.contains("OSScan"));
    }

    public void testPrintOSExchange() {
        OSScan scan = new OSScan("test_table", List.of("name"), testSchema());
        OSExchange exchange = new OSExchange(scan, Distribution.SINGLETON, List.of());
        String result = ExecNodePrinter.print(exchange);
        assertTrue(result.contains("OSExchange"));
        assertTrue(result.contains("SINGLETON"));
    }

    public void testPrintOSRemoteSource() {
        OSRemoteSource remoteSource = new OSRemoteSource(0, testSchema());
        String result = ExecNodePrinter.print(remoteSource);
        assertTrue(result.contains("OSRemoteSource"));
        assertTrue(result.contains("stageId=0"));
    }

    public void testPrintNestedTree() {
        OSScan scan = new OSScan("orders", List.of("region", "amount"), testSchema());
        OSNativeEngine partialAgg = new OSNativeEngine("datafusion", new byte[10], scan, testSchema());
        OSExchange exchange = new OSExchange(partialAgg, Distribution.SINGLETON, List.of());
        OSNativeEngine finalAgg = new OSNativeEngine("datafusion", new byte[20], exchange, testSchema());

        String result = ExecNodePrinter.print(finalAgg);

        // Verify nesting: outer engine -> exchange -> inner engine -> scan
        // Each child should be indented more than its parent
        String[] lines = result.split("\n");
        assertTrue("Should have at least 4 lines", lines.length >= 4);
        // First line should not be indented
        assertTrue(lines[0].startsWith("OSNativeEngine"));
        // Second line should be indented
        assertTrue(lines[1].startsWith("  "));
    }

    public void testPrintHashExchangeIncludesColumns() {
        OSScan scan = new OSScan("test_table", List.of("name"), testSchema());
        OSExchange exchange = new OSExchange(scan, Distribution.HASH, List.of(0, 1));
        String result = ExecNodePrinter.print(exchange);
        assertTrue(result.contains("HASH"));
        assertTrue(result.contains("hashColumns=[0, 1]"));
    }
}
