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

public class OSNodeVisitorTests extends OpenSearchTestCase {

    private Schema testSchema() {
        return new Schema(List.of(new Field("col", FieldType.nullable(new ArrowType.Utf8()), null)));
    }

    public void testVisitScanDispatches() {
        OSScan scan = new OSScan("idx", List.of("col"), testSchema());
        ExecNodeVisitor<String, Void> visitor = new ExecNodeVisitor<>() {
            @Override
            public String visitScan(OSScan node, Void context) {
                return "visited_scan";
            }
        };
        assertEquals("visited_scan", scan.accept(visitor, null));
    }

    public void testVisitDefaultThrowsUnsupportedOperation() {
        OSScan scan = new OSScan("idx", List.of("col"), testSchema());
        ExecNodeVisitor<String, Void> visitor = new ExecNodeVisitor<>() {
        };
        expectThrows(UnsupportedOperationException.class, () -> scan.accept(visitor, null));
    }
}
