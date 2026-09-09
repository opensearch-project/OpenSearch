/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat.stub;

import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.mapper.DocCountFieldMapper;
import org.opensearch.index.mapper.FieldNamesFieldMapper;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.IgnoredFieldMapper;
import org.opensearch.index.mapper.IndexFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.NestedPathFieldMapper;
import org.opensearch.index.mapper.RoutingFieldMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.index.mapper.VersionFieldMapper;

import java.util.Set;

import static org.opensearch.index.engine.dataformat.FieldTypeCapabilities.Capability.COLUMNAR_STORAGE;
import static org.opensearch.index.engine.dataformat.FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH;
import static org.opensearch.index.engine.dataformat.FieldTypeCapabilities.Capability.POINT_RANGE;
import static org.opensearch.index.engine.dataformat.FieldTypeCapabilities.Capability.STORED_FIELDS;

/**
 * A mock {@link MockDataFormatPlugin} that registers "parquet" as a data format for testing.
 */
public class MockParquetDataFormatPlugin extends MockDataFormatPlugin {

    public MockParquetDataFormatPlugin() {
        super(
            new MockDataFormat(
                "parquet",
                100L,
                Set.of(
                    new FieldTypeCapabilities(KeywordFieldMapper.CONTENT_TYPE, Set.of(COLUMNAR_STORAGE, STORED_FIELDS, FULL_TEXT_SEARCH)),
                    new FieldTypeCapabilities(TextFieldMapper.CONTENT_TYPE, Set.of(COLUMNAR_STORAGE, STORED_FIELDS, FULL_TEXT_SEARCH)),
                    new FieldTypeCapabilities(DocCountFieldMapper.CONTENT_TYPE, Set.of(COLUMNAR_STORAGE)),
                    new FieldTypeCapabilities(RoutingFieldMapper.CONTENT_TYPE, Set.of(STORED_FIELDS, FULL_TEXT_SEARCH)),
                    new FieldTypeCapabilities(IgnoredFieldMapper.CONTENT_TYPE, Set.of(STORED_FIELDS, FULL_TEXT_SEARCH)),
                    new FieldTypeCapabilities(IdFieldMapper.CONTENT_TYPE, Set.of(STORED_FIELDS, FULL_TEXT_SEARCH)),
                    new FieldTypeCapabilities(SeqNoFieldMapper.CONTENT_TYPE, Set.of(COLUMNAR_STORAGE, POINT_RANGE)),
                    new FieldTypeCapabilities(SeqNoFieldMapper.PRIMARY_TERM_NAME, Set.of(COLUMNAR_STORAGE)),
                    new FieldTypeCapabilities(VersionFieldMapper.CONTENT_TYPE, Set.of(COLUMNAR_STORAGE)),
                    new FieldTypeCapabilities(IndexFieldMapper.CONTENT_TYPE, Set.of(COLUMNAR_STORAGE, FULL_TEXT_SEARCH)),
                    new FieldTypeCapabilities(NestedPathFieldMapper.NAME, Set.of(FULL_TEXT_SEARCH)),
                    new FieldTypeCapabilities(SourceFieldMapper.NAME, Set.of(STORED_FIELDS)),
                    new FieldTypeCapabilities(FieldNamesFieldMapper.NAME, Set.of(FULL_TEXT_SEARCH))
                )
            )
        );
    }
}
