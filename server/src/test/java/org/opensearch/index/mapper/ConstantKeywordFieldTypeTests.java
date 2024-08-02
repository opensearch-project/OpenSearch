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
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.query.QueryShardContext;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

public class ConstantKeywordFieldTypeTests extends FieldTypeTestCase {

    final ConstantKeywordFieldMapper.ConstantKeywordFieldType ft = new ConstantKeywordFieldMapper.ConstantKeywordFieldType(
        "field",
        "default"
    );

    public void testTermQuery() {
        assertEquals(new MatchAllDocsQuery(), ft.termQuery("default", createContext()));
        assertEquals(new MatchNoDocsQuery(), ft.termQuery("not_default", createContext()));
    }

    public void testTermsQuery() {
        assertEquals(new MatchAllDocsQuery(), ft.termsQuery(Arrays.asList("default", "not_default"), createContext()));
        assertEquals(new MatchNoDocsQuery(), ft.termsQuery(Arrays.asList("no_default", "not_default"), createContext()));
        assertEquals(new MatchNoDocsQuery(), ft.termsQuery(List.of(), createContext()));
    }

    public void testInsensitiveTermQuery() {
        assertEquals(new MatchAllDocsQuery(), ft.termQueryCaseInsensitive("defaUlt", createContext()));
        assertEquals(new MatchNoDocsQuery(), ft.termQueryCaseInsensitive("not_defaUlt", createContext()));
    }

    public void testPrefixQuery() {
        assertEquals(new MatchAllDocsQuery(), ft.prefixQuery("defau", null, createContext()));
        assertEquals(new MatchNoDocsQuery(), ft.prefixQuery("not_default", null, createContext()));
    }

    public void testWildcardQuery() {
        assertEquals(new MatchAllDocsQuery(), ft.wildcardQuery("defa*lt", null, createContext()));
        assertEquals(new MatchNoDocsQuery(), ft.wildcardQuery("no_defa*lt", null, createContext()));
        assertEquals(new MatchAllDocsQuery(), ft.wildcardQuery("defa*", null, createContext()));
        assertEquals(new MatchAllDocsQuery(), ft.wildcardQuery("*ult", null, createContext()));

    }

    public void testExistsQuery() {
        assertEquals(new MatchAllDocsQuery(), ft.existsQuery(createContext()));
    }

    public void testRangeQuery() {
        Query actual = ft.rangeQuery("default", null, true, false, null, null, null, MOCK_QSC);
        assertEquals(new MatchAllDocsQuery(), actual);

        actual = ft.rangeQuery("default", null, false, false, null, null, null, MOCK_QSC);
        assertEquals(new MatchNoDocsQuery(), actual);

        actual = ft.rangeQuery(null, "default", true, true, null, null, null, MOCK_QSC);
        assertEquals(new MatchAllDocsQuery(), actual);

        actual = ft.rangeQuery(null, "default", false, false, null, null, null, MOCK_QSC);
        assertEquals(new MatchNoDocsQuery(), actual);

        actual = ft.rangeQuery("default", "default", false, true, null, null, null, MOCK_QSC);
        assertEquals(new MatchNoDocsQuery(), actual);

        actual = ft.rangeQuery("default", "default", true, false, null, null, null, MOCK_QSC);
        assertEquals(new MatchNoDocsQuery(), actual);

        actual = ft.rangeQuery(null, null, false, false, null, null, null, MOCK_QSC);
        assertEquals(new MatchAllDocsQuery(), actual);

        actual = ft.rangeQuery("default", "default", true, true, null, null, null, MOCK_QSC);
        assertEquals(new MatchAllDocsQuery(), actual);

        actual = ft.rangeQuery("defaul", "default1", true, true, null, null, null, MOCK_QSC);
        assertEquals(new MatchAllDocsQuery(), actual);
    }

    public void testRegexpQuery() {
        final ConstantKeywordFieldMapper.ConstantKeywordFieldType ft = new ConstantKeywordFieldMapper.ConstantKeywordFieldType(
            "field",
            "d3efault"
        );
        // test .*
        Query query = ft.regexpQuery("d.*", 0, 0, 10, MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE, MOCK_QSC);
        assertEquals(new MatchAllDocsQuery(), query);
        // test \d and ?
        query = ft.regexpQuery("d\\defau[a-z]?t", 0, 0, 10, MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE, MOCK_QSC);
        assertEquals(new MatchAllDocsQuery(), query);

        // test \d and ?
        query = ft.regexpQuery("d\\defa[a-z]?t", 0, 0, 10, MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE, MOCK_QSC);
        assertEquals(new MatchNoDocsQuery(), query);
        // \w{m,n}
        query = ft.regexpQuery("d3efa[a-z]{3,3}", 0, 0, 10, MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE, MOCK_QSC);
        assertEquals(new MatchAllDocsQuery(), query);
        // \w{m,n}
        query = ft.regexpQuery("d3efa[a-z]{4,4}", 0, 0, 10, MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE, MOCK_QSC);
        assertEquals(new MatchNoDocsQuery(), query);
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
