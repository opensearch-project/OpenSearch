/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.writer;

import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.test.OpenSearchTestCase;

public class FieldValuePairTests extends OpenSearchTestCase {

    public void testConstructionAndGetters() {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("age", NumberFieldMapper.NumberType.INTEGER);
        FieldValuePair pair = new FieldValuePair(fieldType, 25);
        assertSame(fieldType, pair.getFieldType());
        assertEquals(25, pair.getValue());
    }

    public void testNullFieldTypeThrows() {
        expectThrows(IllegalArgumentException.class, () -> new FieldValuePair(null, "value"));
    }

    public void testNullValueIsAllowed() {
        MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("name");
        FieldValuePair pair = new FieldValuePair(fieldType, null);
        assertSame(fieldType, pair.getFieldType());
        assertNull(pair.getValue());
    }
}
