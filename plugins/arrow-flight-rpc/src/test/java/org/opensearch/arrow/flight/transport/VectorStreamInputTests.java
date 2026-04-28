/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.List;

public class VectorStreamInputTests extends OpenSearchTestCase {

    private BufferAllocator allocator;
    private NamedWriteableRegistry registry;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        allocator = new RootAllocator();
        registry = new NamedWriteableRegistry(Collections.emptyList());
    }

    @After
    @Override
    public void tearDown() throws Exception {
        allocator.close();
        super.tearDown();
    }

    public void testGetRootReturnsRoot() {
        Schema schema = new Schema(List.of(new Field("0", FieldType.nullable(new ArrowType.Binary()), null)));
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        VarBinaryVector vec = (VarBinaryVector) root.getVector("0");
        vec.allocateNew();
        vec.setValueCount(0);
        root.setRowCount(0);

        VectorStreamInput input = new VectorStreamInput(root, registry);
        assertSame(root, input.getRoot());
        root.close();
    }

    public void testCloseWithNullVector() throws Exception {
        // Create a root with no vector named "0" so vector field is null
        Schema schema = new Schema(List.of(new Field("other", FieldType.nullable(new ArrowType.Utf8()), null)));
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);

        VectorStreamInput input = new VectorStreamInput(root, registry);
        // close() should not throw even though vector is null
        input.close();
        root.close();
    }
}
