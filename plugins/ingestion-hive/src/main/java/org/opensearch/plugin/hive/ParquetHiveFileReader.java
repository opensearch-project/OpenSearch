/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads rows from a Parquet file, returning each row as a map of column names to values.
 */
public class ParquetHiveFileReader implements HiveFileReader {

    private final ParquetFileReader reader;
    private final MessageType projectionSchema;
    private RecordReader<Group> recordReader;
    private long rowsRemainingInGroup;

    /**
     * Opens a Parquet file for reading.
     *
     * @param filePath the path to the Parquet file
     * @param hadoopConf Hadoop configuration for filesystem access
     * @param projectionSchema the schema to project (from Metastore table definition)
     * @throws IOException if the file cannot be opened
     */
    public ParquetHiveFileReader(String filePath, Configuration hadoopConf, MessageType projectionSchema) throws IOException {
        this.reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(filePath), hadoopConf));
        this.projectionSchema = projectionSchema;
        this.rowsRemainingInGroup = 0;
    }

    @Override
    public Map<String, Object> readNext() throws IOException {
        // Parquet files are divided into row groups. When the current row group is exhausted,
        // advance to the next one. If no more row groups exist, the file is fully read.
        if (recordReader == null || rowsRemainingInGroup <= 0) {
            PageReadStore rowGroup = reader.readNextRowGroup();
            if (rowGroup == null) {
                return null; // End of file
            }
            // Build a record reader that maps file columns to the projection schema.
            // This handles schema evolution: missing columns become null, extra columns are ignored.
            MessageType fileSchema = reader.getFooter().getFileMetaData().getSchema();
            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(projectionSchema, fileSchema);
            recordReader = columnIO.getRecordReader(rowGroup, new GroupRecordConverter(projectionSchema));
            rowsRemainingInGroup = rowGroup.getRowCount();
        }

        // Read one row from the current row group and convert to a Map
        Group record = recordReader.read();
        rowsRemainingInGroup--;
        return groupToMap(record);
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    /**
     * Converts a Parquet Group (one row) to a Map of column name -> value.
     * Handles three cases per field:
     *   - repetitionCount == 0: field is absent in this row -> null
     *   - repetitionCount == 1: single value (most common)
     *   - repetitionCount > 1: repeated field (Hive ARRAY) -> List of values
     */
    private Map<String, Object> groupToMap(Group record) {
        Map<String, Object> row = new LinkedHashMap<>();
        GroupType schema = record.getType();
        for (int i = 0; i < schema.getFieldCount(); i++) {
            String fieldName = schema.getFieldName(i);
            int repetitionCount = record.getFieldRepetitionCount(i);
            if (repetitionCount == 0) {
                row.put(fieldName, null);
                continue;
            }
            if (schema.getType(i).isPrimitive()) {
                if (repetitionCount == 1) {
                    row.put(fieldName, readPrimitiveValue(record, schema, i));
                } else {
                    List<Object> values = new ArrayList<>();
                    for (int r = 0; r < repetitionCount; r++) {
                        values.add(readPrimitiveValueAt(record, schema, i, r));
                    }
                    row.put(fieldName, values);
                }
            } else {
                // Non-primitive (nested struct/group): serialize to string representation
                if (repetitionCount == 1) {
                    row.put(fieldName, groupToString(record.getGroup(i, 0)));
                } else {
                    List<Object> values = new ArrayList<>();
                    for (int r = 0; r < repetitionCount; r++) {
                        values.add(groupToString(record.getGroup(i, r)));
                    }
                    row.put(fieldName, values);
                }
            }
        }
        return row;
    }

    private String groupToString(Group group) {
        return group.toString();
    }

    private Object readPrimitiveValue(Group record, GroupType schema, int fieldIndex) {
        return readPrimitiveValueAt(record, schema, fieldIndex, 0);
    }

    private Object readPrimitiveValueAt(Group record, GroupType schema, int fieldIndex, int valueIndex) {
        return switch (schema.getType(fieldIndex).asPrimitiveType().getPrimitiveTypeName()) {
            case BOOLEAN -> record.getBoolean(fieldIndex, valueIndex);
            case INT32 -> record.getInteger(fieldIndex, valueIndex);
            case INT64 -> record.getLong(fieldIndex, valueIndex);
            case FLOAT -> record.getFloat(fieldIndex, valueIndex);
            case DOUBLE -> record.getDouble(fieldIndex, valueIndex);
            case BINARY, FIXED_LEN_BYTE_ARRAY -> {
                LogicalTypeAnnotation annotation = schema.getType(fieldIndex).getLogicalTypeAnnotation();
                if (annotation instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
                    yield record.getString(fieldIndex, valueIndex);
                }
                yield Base64.getEncoder().encodeToString(record.getBinary(fieldIndex, valueIndex).getBytes());
            }
            case INT96 -> record.getValueToString(fieldIndex, valueIndex);
        };
    }
}
