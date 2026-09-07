/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.bridge;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IndexSettings;
import org.opensearch.nativebridge.spi.ArrowExport;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * End-to-end LIST index-sort tests across the Java settings layer, FFM writer bridge,
 * native per-file sort, native multi-file merge, and persisted Parquet row order.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class ParquetListSortModeIntegrationTests extends OpenSearchTestCase {

    private static final String INDEX_NAME = "list-sort-mode-it";
    private static final List<List<String>> DISTINCT_MODE_ROWS = List.of(List.of("a", "z"), List.of("m", "n"), List.of("b"));

    private BufferAllocator allocator;
    private Schema schema;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        RustBridge.initLogger();
        allocator = new RootAllocator();
        schema = new Schema(
            List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field(
                    "tags",
                    FieldType.nullable(ArrowType.List.INSTANCE),
                    List.of(new Field("item", FieldType.nullable(new ArrowType.Utf8()), null))
                )
            )
        );
    }

    @Override
    public void tearDown() throws Exception {
        RustBridge.removeSettings(INDEX_NAME);
        allocator.close();
        super.tearDown();
    }

    public void testDefaultAscendingUsesMin() throws Exception {
        assertEquals(List.of(1L, 3L, 2L), writeMergeAndReadIds(sortConfig(null, null, "_last"), DISTINCT_MODE_ROWS));
    }

    public void testDefaultDescendingUsesMax() throws Exception {
        assertEquals(List.of(1L, 2L, 3L), writeMergeAndReadIds(sortConfig("desc", null, "_last"), DISTINCT_MODE_ROWS));
    }

    public void testExplicitMinAscending() throws Exception {
        assertEquals(List.of(1L, 3L, 2L), writeMergeAndReadIds(sortConfig("asc", "min", "_last"), DISTINCT_MODE_ROWS));
    }

    public void testExplicitMaxAscendingOverridesDefault() throws Exception {
        assertEquals(List.of(3L, 2L, 1L), writeMergeAndReadIds(sortConfig("asc", "max", "_last"), DISTINCT_MODE_ROWS));
    }

    public void testExplicitMinDescendingOverridesDefault() throws Exception {
        assertEquals(List.of(2L, 3L, 1L), writeMergeAndReadIds(sortConfig("desc", "min", "_last"), DISTINCT_MODE_ROWS));
    }

    public void testExplicitMaxDescending() throws Exception {
        assertEquals(List.of(1L, 2L, 3L), writeMergeAndReadIds(sortConfig("desc", "max", "_last"), DISTINCT_MODE_ROWS));
    }

    public void testNullEmptyAndAllNullListsSortFirst() throws Exception {
        List<Long> ids = writeMergeAndReadIds(sortConfig("asc", "min", "_first"), nullLikeRows());
        assertEquals(Set.of(2L, 3L, 4L), new HashSet<>(ids.subList(0, 3)));
        assertEquals(List.of(5L, 1L), ids.subList(3, 5));
    }

    public void testNullEmptyAndAllNullListsSortLast() throws Exception {
        List<Long> ids = writeMergeAndReadIds(sortConfig("desc", "max", "_last"), nullLikeRows());
        assertEquals(List.of(5L, 1L), ids.subList(0, 2));
        assertEquals(Set.of(2L, 3L, 4L), new HashSet<>(ids.subList(2, 5)));
    }

    private List<List<String>> nullLikeRows() {
        return java.util.Arrays.asList(List.of("b", "d"), null, List.of(), java.util.Arrays.asList(null, null), List.of("a", "z"));
    }

    private ParquetSortConfig sortConfig(String order, String mode, String missing) {
        Settings.Builder settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .putList("index.sort.field", "tags")
            .putList("index.sort.missing", missing);
        if (order != null) {
            settings.putList("index.sort.order", order);
        }
        if (mode != null) {
            settings.putList("index.sort.mode", mode);
        }
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(INDEX_NAME, settings.build());
        return new ParquetSortConfig(indexSettings);
    }

    private List<Long> writeMergeAndReadIds(ParquetSortConfig sortConfig, List<List<String>> rows) throws Exception {
        RustBridge.onSettingsUpdate(NativeSettings.builder().indexName(INDEX_NAME).build());
        try {
            List<Long> firstIds = new ArrayList<>();
            List<List<String>> firstRows = new ArrayList<>();
            List<Long> secondIds = new ArrayList<>();
            List<List<String>> secondRows = new ArrayList<>();
            for (int row = 0; row < rows.size(); row++) {
                if (row % 2 == 0) {
                    firstIds.add((long) row + 1);
                    firstRows.add(rows.get(row));
                } else {
                    secondIds.add((long) row + 1);
                    secondRows.add(rows.get(row));
                }
            }

            Path dir = createTempDir();
            String first = writeSortedFile(dir.resolve("first.parquet"), sortConfig, toLongArray(firstIds), firstRows);
            String second = writeSortedFile(dir.resolve("second.parquet"), sortConfig, toLongArray(secondIds), secondRows);

            String merged = dir.resolve("merged.parquet").toString();
            RustBridge.mergeParquetFilesInRust(List.of(Path.of(first), Path.of(second)), merged, INDEX_NAME, 1L);
            assertEquals(rows.size(), RustBridge.getFileMetadata(merged).numRows());
            return readIds(merged);
        } finally {
            RustBridge.removeSettings(INDEX_NAME);
        }
    }

    private long[] toLongArray(List<Long> values) {
        return values.stream().mapToLong(Long::longValue).toArray();
    }

    private String writeSortedFile(Path path, ParquetSortConfig sortConfig, long[] ids, List<List<String>> tags) throws Exception {
        String file = path.toString();
        try (ArrowExport schemaExport = exportSchema()) {
            NativeParquetWriter writer = new NativeParquetWriter(file);
            writer.initialize(INDEX_NAME, schemaExport.getSchemaAddress(), sortConfig, 0L);
            try (ArrowExport data = exportData(ids, tags)) {
                writer.write(data.getArrayAddress(), data.getSchemaAddress());
            }
            writer.flush();
        }
        return file;
    }

    private ArrowExport exportSchema() {
        ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
        Data.exportSchema(allocator, schema, null, arrowSchema);
        return new ArrowExport(null, arrowSchema);
    }

    private ArrowExport exportData(long[] ids, List<List<String>> tags) {
        assertEquals(ids.length, tags.size());
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            root.allocateNew();
            BigIntVector idVector = (BigIntVector) root.getVector("id");
            ListVector tagsVector = (ListVector) root.getVector("tags");
            VarCharVector values = (VarCharVector) tagsVector.getDataVector();

            for (int row = 0; row < ids.length; row++) {
                idVector.setSafe(row, ids[row]);
                List<String> rowValues = tags.get(row);
                if (rowValues == null) {
                    tagsVector.setNull(row);
                    continue;
                }

                int childIndex = tagsVector.startNewValue(row);
                for (String value : rowValues) {
                    if (value == null) {
                        values.setNull(childIndex);
                    } else {
                        values.setSafe(childIndex, value.getBytes(StandardCharsets.UTF_8));
                    }
                    childIndex++;
                }
                tagsVector.endValue(row, rowValues.size());
            }
            root.setRowCount(ids.length);

            ArrowArray array = ArrowArray.allocateNew(allocator);
            ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
            Data.exportVectorSchemaRoot(allocator, root, null, array, arrowSchema);
            return new ArrowExport(array, arrowSchema);
        }
    }

    @SuppressForbidden(reason = "JSON parsing for persisted physical row order verification")
    private List<Long> readIds(String parquetFile) throws Exception {
        String json = RustBridge.readAsJson(parquetFile);
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                json
            )
        ) {
            return parser.list().stream().map(row -> {
                @SuppressWarnings("unchecked")
                Map<String, Object> values = (Map<String, Object>) row;
                return ((Number) values.get("id")).longValue();
            }).toList();
        }
    }
}
