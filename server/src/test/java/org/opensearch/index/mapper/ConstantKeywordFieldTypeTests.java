/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryShardException;

import java.util.function.Predicate;

import static org.hamcrest.Matchers.containsString;

public class ConstantKeywordFieldTypeTests extends FieldTypeTestCase {

    public void testPrefixQuery() {
        MappedFieldType ft = new ConstantKeywordFieldMapper.ConstantKeywordFieldType("field", "default");

        assertEquals(new MatchAllDocsQuery(), ft.termQuery("default", createContext()));
        assertEquals(new MatchNoDocsQuery(), ft.prefixQuery("not_default", null, createContext()));
    }

    public void testWildcardQuery() {
        ConstantKeywordFieldMapper.ConstantKeywordFieldType ft = new ConstantKeywordFieldMapper.ConstantKeywordFieldType("field", "default");

        QueryShardException e = expectThrows(
            QueryShardException.class,
            () -> assertEquals(new MatchAllDocsQuery(), ft.wildcardQuery("ind*x", null, createContext()))
        );
        assertThat(e.getMessage(), containsString("Fields of type [constant_keyword], does not support wildcard queries"));
    }

    public void testRegexpQuery() {
        MappedFieldType ft = IndexFieldMapper.IndexFieldType.INSTANCE;

        QueryShardException e = expectThrows(
            QueryShardException.class,
            () -> assertEquals(new MatchAllDocsQuery(), ft.regexpQuery("ind.x", 0, 0, 10, null, createContext()))
        );
        assertThat(e.getMessage(), containsString("Can only use regexp queries on keyword and text fields"));
    }

    private QueryShardContext createContext() {
        IndexMetadata indexMetadata = IndexMetadata.builder("index")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);

        Predicate<String> indexNameMatcher = pattern -> Regex.simpleMatch(pattern, "index");
        return new QueryShardContext(
            0,
            indexSettings,
            null,
            null,
            null,
            null,
            null,
            null,
            xContentRegistry(),
            writableRegistry(),
            null,
            null,
            System::currentTimeMillis,
            null,
            indexNameMatcher,
            () -> true,
            null
        );
    }
}
