/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields.plugins;

import org.opensearch.index.mapper.DocCountFieldMapper;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.IgnoredFieldMapper;
import org.opensearch.index.mapper.RoutingFieldMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.VersionFieldMapper;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

public class MetadataFieldPluginTests extends OpenSearchTestCase {

    private Map<String, ParquetField> fields;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        fields = new MetadataFieldPlugin().getParquetFields();
    }

    public void testFieldCount() {
        assertEquals(8, fields.size());
    }

    public void testAllMetadataTypesPresent() {
        assertNotNull(fields.get(DocCountFieldMapper.CONTENT_TYPE));
        assertNotNull(fields.get("_size"));
        assertNotNull(fields.get(RoutingFieldMapper.CONTENT_TYPE));
        assertNotNull(fields.get(IgnoredFieldMapper.CONTENT_TYPE));
        assertNotNull(fields.get(IdFieldMapper.CONTENT_TYPE));
        assertNotNull(fields.get(SeqNoFieldMapper.CONTENT_TYPE));
        assertNotNull(fields.get(SeqNoFieldMapper.PRIMARY_TERM_NAME));
        assertNotNull(fields.get(VersionFieldMapper.CONTENT_TYPE));
    }

    public void testAllValuesNonNull() {
        for (Map.Entry<String, ParquetField> entry : fields.entrySet()) {
            assertNotNull("Null ParquetField for: " + entry.getKey(), entry.getValue());
        }
    }
}
