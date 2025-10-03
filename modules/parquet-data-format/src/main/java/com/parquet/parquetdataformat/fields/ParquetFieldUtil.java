/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.fields;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.MetadataFieldMapper;

import java.util.ArrayList;
import java.util.List;

public class ParquetFieldUtil {

    public static Schema getSchema(MapperService mapperService) {
        List<Field> fields = new ArrayList<>();

        for (Mapper mapper : mapperService.documentMapper().mappers()) {
            if (mapper instanceof MetadataFieldMapper) continue;
            fields.add(new Field(mapper.name(), ArrowFieldRegistry.getFieldType(mapper.typeName()), null));
        }

        // Create the most minimal schema possible - just one string field
        return new Schema(fields);
    }
}
