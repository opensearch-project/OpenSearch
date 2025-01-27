/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.query;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.sandbox.search.CoveringQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.index.fielddata.ScriptDocValues;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.plugins.Plugin;
import org.opensearch.script.MockScriptEngine;
import org.opensearch.script.MockScriptPlugin;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.test.AbstractQueryTestCase;
import org.opensearch.test.TestGeoShapeFieldMapperPlugin;
import org.opensearch.test.rest.yaml.ObjectPath;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class TermsSetQueryBuilderTests extends AbstractQueryTestCase<TermsSetQueryBuilder> {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(CustomScriptPlugin.class, TestGeoShapeFieldMapperPlugin.class);
    }

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        String docType = "_doc";
        mapperService.merge(
            docType,
            new CompressedXContent(PutMappingRequest.simpleMapping("m_s_m", "type=long").toString()),
            MapperService.MergeReason.MAPPING_UPDATE
        );
    }

    @Override
    protected TermsSetQueryBuilder doCreateTestQueryBuilder() {
        String fieldName = randomValueOtherThanMany(
            value -> value.equals(GEO_POINT_FIELD_NAME) || value.equals(GEO_SHAPE_FIELD_NAME),
            () -> randomFrom(MAPPED_FIELD_NAMES)
        );
        List<?> randomTerms = randomValues(fieldName);
        TermsSetQueryBuilder queryBuilder = new TermsSetQueryBuilder(TEXT_FIELD_NAME, randomTerms);
        if (randomBoolean()) {
            queryBuilder.setMinimumShouldMatchField("m_s_m");
        } else {
            queryBuilder.setMinimumShouldMatchScript(new Script(ScriptType.INLINE, MockScriptEngine.NAME, "_script", emptyMap()));
        }
        return queryBuilder;
    }

    @Override
    protected void doAssertLuceneQuery(TermsSetQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        if (queryBuilder.getValues().isEmpty()) {
            assertThat(query, instanceOf(MatchNoDocsQuery.class));
            MatchNoDocsQuery matchNoDocsQuery = (MatchNoDocsQuery) query;
            assertThat(matchNoDocsQuery.toString(), containsString("No terms supplied for \"terms_set\" query."));
        } else {
            assertThat(query, instanceOf(CoveringQuery.class));
        }
    }

    /**
     * Check that this query is generally not cacheable and explicitly testing the two conditions when it is not as well
     */
    @Override
    public void testCacheability() throws IOException {
        TermsSetQueryBuilder queryBuilder = createTestQueryBuilder();
        boolean isCacheable = queryBuilder.getMinimumShouldMatchField() != null
            || (queryBuilder.getMinimumShouldMatchScript() != null && queryBuilder.getValues().isEmpty());
        QueryShardContext context = createShardContext();
        rewriteQuery(queryBuilder, new QueryShardContext(context));
        assertNotNull(queryBuilder.doToQuery(context));
        assertEquals(
            "query should " + (isCacheable ? "" : "not") + " be cacheable: " + queryBuilder.toString(),
            isCacheable,
            context.isCacheable()
        );

        // specifically trigger the two cases where query is cacheable
        queryBuilder = new TermsSetQueryBuilder(TEXT_FIELD_NAME, Collections.singletonList("foo"));
        queryBuilder.setMinimumShouldMatchField("m_s_m");
        context = createShardContext();
        rewriteQuery(queryBuilder, new QueryShardContext(context));
        assertNotNull(queryBuilder.doToQuery(context));
        assertTrue("query should be cacheable: " + queryBuilder.toString(), context.isCacheable());

        queryBuilder = new TermsSetQueryBuilder(TEXT_FIELD_NAME, Collections.emptyList());
        queryBuilder.setMinimumShouldMatchScript(new Script(ScriptType.INLINE, MockScriptEngine.NAME, "_script", emptyMap()));
        context = createShardContext();
        rewriteQuery(queryBuilder, new QueryShardContext(context));
        assertNotNull(queryBuilder.doToQuery(context));
        assertTrue("query should be cacheable: " + queryBuilder.toString(), context.isCacheable());

        // also test one case where query is not cacheable
        queryBuilder = new TermsSetQueryBuilder(TEXT_FIELD_NAME, Collections.singletonList("foo"));
        queryBuilder.setMinimumShouldMatchScript(new Script(ScriptType.INLINE, MockScriptEngine.NAME, "_script", emptyMap()));
        context = createShardContext();
        rewriteQuery(queryBuilder, new QueryShardContext(context));
        assertNotNull(queryBuilder.doToQuery(context));
        assertFalse("query should be cacheable: " + queryBuilder.toString(), context.isCacheable());
    }

    @Override
    public TermsSetQueryBuilder mutateInstance(final TermsSetQueryBuilder instance) throws IOException {
        String fieldName = instance.getFieldName();
        List<?> values = instance.getValues();
        String minimumShouldMatchField = null;
        Script minimumShouldMatchScript = null;

        switch (randomIntBetween(0, 3)) {
            case 0:
                Predicate<String> predicate = s -> s.equals(instance.getFieldName()) == false
                    && s.equals(GEO_POINT_FIELD_NAME) == false
                    && s.equals(GEO_SHAPE_FIELD_NAME) == false;
                fieldName = randomValueOtherThanMany(predicate, () -> randomFrom(MAPPED_FIELD_NAMES));
                values = randomValues(fieldName);
                break;
            case 1:
                values = randomValues(fieldName);
                break;
            case 2:
                minimumShouldMatchField = randomAlphaOfLengthBetween(1, 10);
                break;
            case 3:
                minimumShouldMatchScript = new Script(ScriptType.INLINE, MockScriptEngine.NAME, randomAlphaOfLength(10), emptyMap());
                break;
        }

        TermsSetQueryBuilder newInstance = new TermsSetQueryBuilder(fieldName, values);
        if (minimumShouldMatchField != null) {
            newInstance.setMinimumShouldMatchField(minimumShouldMatchField);
        }
        if (minimumShouldMatchScript != null) {
            newInstance.setMinimumShouldMatchScript(minimumShouldMatchScript);
        }
        return newInstance;
    }

    public void testBothFieldAndScriptSpecified() {
        TermsSetQueryBuilder queryBuilder = new TermsSetQueryBuilder("_field", Collections.emptyList());
        queryBuilder.setMinimumShouldMatchScript(new Script(""));
        expectThrows(IllegalArgumentException.class, () -> queryBuilder.setMinimumShouldMatchField("_field"));

        queryBuilder.setMinimumShouldMatchScript(null);
        queryBuilder.setMinimumShouldMatchField("_field");
        expectThrows(IllegalArgumentException.class, () -> queryBuilder.setMinimumShouldMatchScript(new Script("")));
    }

    public void testDoToQuery() throws Exception {
        try (Directory directory = newDirectory()) {
            IndexWriterConfig config = new IndexWriterConfig(new WhitespaceAnalyzer());
            config.setMergePolicy(NoMergePolicy.INSTANCE);
            try (IndexWriter iw = new IndexWriter(directory, config)) {
                Document document = new Document();
                document.add(new TextField("message", "a b", Field.Store.NO));
                document.add(new SortedNumericDocValuesField("m_s_m", 1));
                iw.addDocument(document);

                document = new Document();
                document.add(new TextField("message", "a b c", Field.Store.NO));
                document.add(new SortedNumericDocValuesField("m_s_m", 1));
                iw.addDocument(document);

                document = new Document();
                document.add(new TextField("message", "a b c", Field.Store.NO));
                document.add(new SortedNumericDocValuesField("m_s_m", 2));
                iw.addDocument(document);

                document = new Document();
                document.add(new TextField("message", "a b c d", Field.Store.NO));
                document.add(new SortedNumericDocValuesField("m_s_m", 1));
                iw.addDocument(document);

                document = new Document();
                document.add(new TextField("message", "a b c d", Field.Store.NO));
                document.add(new SortedNumericDocValuesField("m_s_m", 2));
                iw.addDocument(document);

                document = new Document();
                document.add(new TextField("message", "a b c d", Field.Store.NO));
                document.add(new SortedNumericDocValuesField("m_s_m", 3));
                iw.addDocument(document);
            }

            try (IndexReader ir = DirectoryReader.open(directory)) {
                QueryShardContext context = createShardContext();
                Query query = new TermsSetQueryBuilder("message", Arrays.asList("c", "d")).setMinimumShouldMatchField("m_s_m")
                    .doToQuery(context);
                IndexSearcher searcher = new IndexSearcher(ir);
                TopDocs topDocs = searcher.search(query, 10, new Sort(SortField.FIELD_DOC));
                assertThat(topDocs.totalHits.value(), equalTo(3L));
                assertThat(topDocs.scoreDocs[0].doc, equalTo(1));
                assertThat(topDocs.scoreDocs[1].doc, equalTo(3));
                assertThat(topDocs.scoreDocs[2].doc, equalTo(4));
            }
        }
    }

    public void testDoToQuery_msmScriptField() throws Exception {
        try (Directory directory = newDirectory()) {
            IndexWriterConfig config = new IndexWriterConfig(new WhitespaceAnalyzer());
            config.setMergePolicy(NoMergePolicy.INSTANCE);
            try (IndexWriter iw = new IndexWriter(directory, config)) {
                Document document = new Document();
                document.add(new TextField("message", "a b x y", Field.Store.NO));
                document.add(new SortedNumericDocValuesField("m_s_m", 50));
                iw.addDocument(document);

                document = new Document();
                document.add(new TextField("message", "a b x y", Field.Store.NO));
                document.add(new SortedNumericDocValuesField("m_s_m", 75));
                iw.addDocument(document);

                document = new Document();
                document.add(new TextField("message", "a b c x", Field.Store.NO));
                document.add(new SortedNumericDocValuesField("m_s_m", 75));
                iw.addDocument(document);

                document = new Document();
                document.add(new TextField("message", "a b c x", Field.Store.NO));
                document.add(new SortedNumericDocValuesField("m_s_m", 100));
                iw.addDocument(document);

                document = new Document();
                document.add(new TextField("message", "a b c d", Field.Store.NO));
                document.add(new SortedNumericDocValuesField("m_s_m", 100));
                iw.addDocument(document);
            }

            try (IndexReader ir = DirectoryReader.open(directory)) {
                QueryShardContext context = createShardContext();
                Script script = new Script(ScriptType.INLINE, MockScriptEngine.NAME, "_script", emptyMap());
                Query query = new TermsSetQueryBuilder("message", Arrays.asList("a", "b", "c", "d")).setMinimumShouldMatchScript(script)
                    .doToQuery(context);
                IndexSearcher searcher = new IndexSearcher(ir);
                TopDocs topDocs = searcher.search(query, 10, new Sort(SortField.FIELD_DOC));
                assertThat(topDocs.totalHits.value(), equalTo(3L));
                assertThat(topDocs.scoreDocs[0].doc, equalTo(0));
                assertThat(topDocs.scoreDocs[1].doc, equalTo(2));
                assertThat(topDocs.scoreDocs[2].doc, equalTo(4));
            }
        }
    }

    public void testFieldAlias() {
        List<String> randomTerms = Arrays.asList(generateRandomStringArray(5, 10, false, false));
        TermsSetQueryBuilder queryBuilder = new TermsSetQueryBuilder(TEXT_ALIAS_FIELD_NAME, randomTerms).setMinimumShouldMatchField(
            "m_s_m"
        );

        QueryShardContext context = createShardContext();
        List<Query> termQueries = queryBuilder.createTermQueries(context);
        assertEquals(randomTerms.size(), termQueries.size());

        String expectedFieldName = expectedFieldName(queryBuilder.getFieldName());
        for (int i = 0; i < randomTerms.size(); i++) {
            Term term = new Term(expectedFieldName, randomTerms.get(i));
            assertThat(termQueries.get(i), equalTo(new TermQuery(term)));
        }
    }

    private static List<?> randomValues(final String fieldName) {
        final int numValues = randomIntBetween(0, 10);
        final List<Object> values = new ArrayList<>(numValues);

        for (int i = 0; i < numValues; i++) {
            values.add(getRandomValueForFieldName(fieldName));
        }
        return values;
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap("_script", args -> {
                try {
                    int clauseCount = ObjectPath.evaluate(args, "params.num_terms");
                    long msm = ((ScriptDocValues.Longs) ObjectPath.evaluate(args, "doc.m_s_m")).getValue();
                    return clauseCount * (msm / 100d);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }
    }

}
