/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.arrow;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.OpenSearchParseException;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.test.OpenSearchTestCase;

public class ArrowIpcSourceFactoryTests extends OpenSearchTestCase {

    public void testGetType() {
        assertEquals("ARROW_IPC", new ArrowIpcSourceFactory().getType());
    }

    public void testCreateArrowSourceReadsConfiguredPath() throws Exception {
        Path file = createTempDir().resolve("data.arrow");
        Schema schema = new Schema(List.of(new Field("id", FieldType.nullable(new ArrowType.Utf8()), null)));
        try (BufferAllocator allocator = new RootAllocator(); VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            VarCharVector idVector = (VarCharVector) root.getVector("id");
            idVector.allocateNew(2);
            idVector.setSafe(0, "a".getBytes(StandardCharsets.UTF_8));
            idVector.setSafe(1, "b".getBytes(StandardCharsets.UTF_8));
            root.setRowCount(2);
            Files.write(file, ArrowTestUtils.writeIpc(root));
        }

        Map<String, Object> params = new HashMap<>();
        params.put(ArrowIpcSourceFactory.ARROW_IPC_PATH, file.toString());
        ArrowIngestionConfig ingestionConfig = ingestionConfig();

        try (ArrowSource source = new ArrowIpcSourceFactory().createArrowSource(params, ingestionConfig)) {
            assertEquals(2, source.getRowCount());
            assertEquals(file.toString(), source.getVersion());
            assertNotNull(source.getVersionTimestamp());
        }
    }

    public void testMissingPathParamThrows() {
        Map<String, Object> params = new HashMap<>();
        ArrowIngestionConfig ingestionConfig = ingestionConfig();
        expectThrows(
                OpenSearchParseException.class,
                () -> new ArrowIpcSourceFactory().createArrowSource(params, ingestionConfig));
    }

    private static ArrowIngestionConfig ingestionConfig() {
        Map<String, Object> params = new HashMap<>();
        params.put(ArrowIngestionConfig.DATASET_TYPE_PROP_KEY, "arrow_ipc");
        params.put(ArrowIngestionConfig.SHARDING_KEY_PROP_KEY, "id");
        IndexMetadata indexMetadata = IndexMetadata.builder("test-index")
                .settings(settings(org.opensearch.Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build();
        return new ArrowIngestionConfig(params, indexMetadata);
    }
}
