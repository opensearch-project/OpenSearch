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
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.LinkedHashMap;
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
        if (recordReader == null || rowsRemainingInGroup <= 0) {
            PageReadStore rowGroup = reader.readNextRowGroup();
            if (rowGroup == null) {
                return null;
            }
            MessageType fileSchema = reader.getFooter().getFileMetaData().getSchema();
            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(projectionSchema, fileSchema);
            recordReader = columnIO.getRecordReader(rowGroup, new GroupRecordConverter(projectionSchema));
            rowsRemainingInGroup = rowGroup.getRowCount();
        }

        Group record = recordReader.read();
        rowsRemainingInGroup--;
        return groupToMap(record);
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    private Map<String, Object> groupToMap(Group record) {
        Map<String, Object> row = new LinkedHashMap<>();
        GroupType schema = record.getType();
        for (int i = 0; i < schema.getFieldCount(); i++) {
            String fieldName = schema.getFieldName(i);
            if (record.getFieldRepetitionCount(i) == 0) {
                row.put(fieldName, null);
                continue;
            }
            if (schema.getType(i).isPrimitive()) {
                row.put(fieldName, readPrimitiveValue(record, schema, i));
            } else {
                row.put(fieldName, record.getValueToString(i, 0));
            }
        }
        return row;
    }

    private Object readPrimitiveValue(Group record, GroupType schema, int fieldIndex) {
        switch (schema.getType(fieldIndex).asPrimitiveType().getPrimitiveTypeName()) {
            case BOOLEAN:
                return record.getBoolean(fieldIndex, 0);
            case INT32:
                return record.getInteger(fieldIndex, 0);
            case INT64:
                return record.getLong(fieldIndex, 0);
            case FLOAT:
                return record.getFloat(fieldIndex, 0);
            case DOUBLE:
                return record.getDouble(fieldIndex, 0);
            case BINARY:
            case FIXED_LEN_BYTE_ARRAY:
                return record.getString(fieldIndex, 0);
            default:
                return record.getValueToString(fieldIndex, 0);
        }
    }
}
