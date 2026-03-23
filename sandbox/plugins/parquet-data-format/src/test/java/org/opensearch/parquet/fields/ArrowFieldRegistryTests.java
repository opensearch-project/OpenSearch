/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields;

import org.opensearch.index.mapper.BinaryFieldMapper;
import org.opensearch.index.mapper.BooleanFieldMapper;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.DocCountFieldMapper;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.IgnoredFieldMapper;
import org.opensearch.index.mapper.IpFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.RoutingFieldMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.index.mapper.VersionFieldMapper;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

public class ArrowFieldRegistryTests extends OpenSearchTestCase {

    public void testAllCoreDataFieldsRegistered() {
        String[] expectedTypes = {
            NumberFieldMapper.NumberType.HALF_FLOAT.typeName(),
            NumberFieldMapper.NumberType.FLOAT.typeName(),
            NumberFieldMapper.NumberType.DOUBLE.typeName(),
            NumberFieldMapper.NumberType.BYTE.typeName(),
            NumberFieldMapper.NumberType.SHORT.typeName(),
            NumberFieldMapper.NumberType.INTEGER.typeName(),
            NumberFieldMapper.NumberType.LONG.typeName(),
            NumberFieldMapper.NumberType.UNSIGNED_LONG.typeName(),
            "token_count",
            "scaled_float",
            DateFieldMapper.CONTENT_TYPE,
            DateFieldMapper.DATE_NANOS_CONTENT_TYPE,
            BooleanFieldMapper.CONTENT_TYPE,
            TextFieldMapper.CONTENT_TYPE,
            KeywordFieldMapper.CONTENT_TYPE,
            IpFieldMapper.CONTENT_TYPE,
            BinaryFieldMapper.CONTENT_TYPE, };
        for (String type : expectedTypes) {
            assertNotNull("Missing registration for: " + type, ArrowFieldRegistry.getParquetField(type));
        }
    }

    public void testAllMetadataFieldsRegistered() {
        String[] expectedTypes = {
            DocCountFieldMapper.CONTENT_TYPE,
            "_size",
            RoutingFieldMapper.CONTENT_TYPE,
            IgnoredFieldMapper.CONTENT_TYPE,
            IdFieldMapper.CONTENT_TYPE,
            SeqNoFieldMapper.CONTENT_TYPE,
            VersionFieldMapper.CONTENT_TYPE,
            SeqNoFieldMapper.PRIMARY_TERM_NAME, };
        for (String type : expectedTypes) {
            assertNotNull("Missing registration for: " + type, ArrowFieldRegistry.getParquetField(type));
        }
    }

    public void testUnknownFieldTypeReturnsNull() {
        assertNull(ArrowFieldRegistry.getParquetField("nonexistent_type"));
    }

    public void testGetRegisteredFieldsIsUnmodifiable() {
        Map<String, ParquetField> fields = ArrowFieldRegistry.getRegisteredFields();
        expectThrows(UnsupportedOperationException.class, () -> fields.put("test", null));
    }
}
