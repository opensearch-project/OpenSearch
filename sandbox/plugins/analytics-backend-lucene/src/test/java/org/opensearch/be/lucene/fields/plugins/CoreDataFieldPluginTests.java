/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.fields.plugins;

import org.opensearch.be.lucene.fields.LuceneField;
import org.opensearch.index.mapper.BinaryFieldMapper;
import org.opensearch.index.mapper.BooleanFieldMapper;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.IpFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

public class CoreDataFieldPluginTests extends OpenSearchTestCase {

    private Map<String, LuceneField> fields;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        fields = new CoreDataFieldPlugin().getLuceneFields();
    }

    public void testFieldCount() {
        // 10 numeric + 2 temporal + 1 boolean + 3 text + 1 binary = 17
        assertEquals(17, fields.size());
    }

    public void testAllNumericTypesPresent() {
        for (NumberFieldMapper.NumberType type : NumberFieldMapper.NumberType.values()) {
            assertNotNull("Missing: " + type.typeName(), fields.get(type.typeName()));
        }
        assertNotNull(fields.get("token_count"));
        assertNotNull(fields.get("scaled_float"));
    }

    public void testTemporalTypesPresent() {
        assertNotNull(fields.get(DateFieldMapper.CONTENT_TYPE));
        assertNotNull(fields.get(DateFieldMapper.DATE_NANOS_CONTENT_TYPE));
    }

    public void testTextTypesPresent() {
        assertNotNull(fields.get(TextFieldMapper.CONTENT_TYPE));
        assertNotNull(fields.get(KeywordFieldMapper.CONTENT_TYPE));
        assertNotNull(fields.get(IpFieldMapper.CONTENT_TYPE));
    }

    public void testBooleanAndBinaryPresent() {
        assertNotNull(fields.get(BooleanFieldMapper.CONTENT_TYPE));
        assertNotNull(fields.get(BinaryFieldMapper.CONTENT_TYPE));
    }

    public void testAllValuesNonNull() {
        for (Map.Entry<String, LuceneField> entry : fields.entrySet()) {
            assertNotNull("Null LuceneField for: " + entry.getKey(), entry.getValue());
        }
    }
}
