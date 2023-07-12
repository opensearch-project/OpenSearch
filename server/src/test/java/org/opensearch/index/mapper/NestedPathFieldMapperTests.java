/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

/** tests for {@link org.opensearch.index.mapper.NestedPathFieldMapper} */
public class NestedPathFieldMapperTests extends OpenSearchSingleNodeTestCase {

    public void testDefaultConfig() throws IOException {
        Settings indexSettings = Settings.EMPTY;
        MapperService mapperService = createIndex("test", indexSettings).mapperService();
        DocumentMapper mapper = mapperService.merge(
            MapperService.SINGLE_MAPPING_NAME,
            new CompressedXContent("{\"" + MapperService.SINGLE_MAPPING_NAME + "\":{}}"),
            MapperService.MergeReason.MAPPING_UPDATE
        );
        ParsedDocument document = mapper.parse(new SourceToParse("index", "id", new BytesArray("{}"), XContentType.JSON));
        assertEquals(Collections.<IndexableField>emptyList(), Arrays.asList(document.rootDoc().getFields(NestedPathFieldMapper.NAME)));
    }

    public void testUpdatesWithSameMappings() throws IOException {
        Settings indexSettings = Settings.EMPTY;
        MapperService mapperService = createIndex("test", indexSettings).mapperService();
        DocumentMapper mapper = mapperService.merge(
            MapperService.SINGLE_MAPPING_NAME,
            new CompressedXContent("{\"" + MapperService.SINGLE_MAPPING_NAME + "\":{}}"),
            MapperService.MergeReason.MAPPING_UPDATE
        );
        mapper.merge(mapper.mapping(), MapperService.MergeReason.MAPPING_UPDATE);
    }
}
