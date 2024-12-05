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

package org.opensearch.index.mapper;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NormsFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.SetOnce;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.IndexFieldDataCache;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Base class for testing {@link Mapper}s.
 */
public abstract class MapperTestCase extends MapperServiceTestCase {
    protected abstract void minimalMapping(XContentBuilder b) throws IOException;

    /**
     * Writes the field and a sample value for it to the provided {@link XContentBuilder}.
     * To be overridden in case the field should not be written at all in documents,
     * like in the case of runtime fields.
     */
    protected void writeField(XContentBuilder builder) throws IOException {
        builder.field("field");
        writeFieldValue(builder);
    }

    /**
     * Writes a sample value for the field to the provided {@link XContentBuilder}.
     */
    protected abstract void writeFieldValue(XContentBuilder builder) throws IOException;

    /**
     * This test verifies that the exists query created is the appropriate one, and aligns with the data structures
     * being created for a document with a value for the field. This can only be verified for the minimal mapping.
     * Field types that allow configurable doc_values or norms should write their own tests that creates the different
     * mappings combinations and invoke {@link #assertExistsQuery(MapperService)} to verify the behaviour.
     */
    public final void testExistsQueryMinimalMapping() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));
        assertExistsQuery(mapperService);
        assertParseMinimalWarnings();
    }

    protected void assertExistsQuery(MapperService mapperService) throws IOException {
        ParseContext.Document fields = mapperService.documentMapper().parse(source(this::writeField)).rootDoc();
        QueryShardContext queryShardContext = createQueryShardContext(mapperService);
        MappedFieldType fieldType = mapperService.fieldType("field");
        Query query = fieldType.existsQuery(queryShardContext);
        assertExistsQuery(fieldType, query, fields);
    }

    protected void assertExistsQuery(MappedFieldType fieldType, Query query, ParseContext.Document fields) {
        if (fieldType.hasDocValues()) {
            assertThat(query, instanceOf(DocValuesFieldExistsQuery.class));
            DocValuesFieldExistsQuery fieldExistsQuery = (DocValuesFieldExistsQuery) query;
            assertEquals("field", fieldExistsQuery.getField());
            assertDocValuesField(fields, "field");
            assertNoFieldNamesField(fields);
        } else if (fieldType.getTextSearchInfo().hasNorms()) {
            assertThat(query, instanceOf(NormsFieldExistsQuery.class));
            NormsFieldExistsQuery normsFieldExistsQuery = (NormsFieldExistsQuery) query;
            assertEquals("field", normsFieldExistsQuery.getField());
            assertHasNorms(fields, "field");
            assertNoDocValuesField(fields, "field");
            assertNoFieldNamesField(fields);
        } else {
            assertThat(query, instanceOf(TermQuery.class));
            TermQuery termQuery = (TermQuery) query;
            assertEquals(FieldNamesFieldMapper.NAME, termQuery.getTerm().field());
            // we always perform a term query against _field_names, even when the field
            // is not added to _field_names because it is not indexed nor stored
            assertEquals("field", termQuery.getTerm().text());
            assertNoDocValuesField(fields, "field");
            if (fieldType.isSearchable() || fieldType.isStored()) {
                assertNotNull(fields.getField(FieldNamesFieldMapper.NAME));
            } else {
                assertNoFieldNamesField(fields);
            }
        }
    }

    protected static void assertNoFieldNamesField(ParseContext.Document fields) {
        assertNull(fields.getField(FieldNamesFieldMapper.NAME));
    }

    protected static void assertHasNorms(ParseContext.Document doc, String field) {
        IndexableField[] fields = doc.getFields(field);
        for (IndexableField indexableField : fields) {
            IndexableFieldType indexableFieldType = indexableField.fieldType();
            if (indexableFieldType.indexOptions() != IndexOptions.NONE) {
                assertFalse(indexableFieldType.omitNorms());
                return;
            }
        }
        fail("field [" + field + "] should be indexed but it isn't");
    }

    protected static void assertDocValuesField(ParseContext.Document doc, String field) {
        IndexableField[] fields = doc.getFields(field);
        for (IndexableField indexableField : fields) {
            if (indexableField.fieldType().docValuesType().equals(DocValuesType.NONE) == false) {
                return;
            }
        }
        fail("doc_values not present for field [" + field + "]");
    }

    protected static void assertNoDocValuesField(ParseContext.Document doc, String field) {
        IndexableField[] fields = doc.getFields(field);
        for (IndexableField indexableField : fields) {
            assertEquals(DocValuesType.NONE, indexableField.fieldType().docValuesType());
        }
    }

    public void testEmptyName() {
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(mapping(b -> {
            b.startObject("");
            minimalMapping(b);
            b.endObject();
        })));
        assertThat(e.getMessage(), containsString("name cannot be empty string"));
        assertParseMinimalWarnings();
    }

    public final void testMinimalSerializesToItself() throws IOException {
        XContentBuilder orig = JsonXContent.contentBuilder().startObject();
        createMapperService(fieldMapping(this::minimalMapping)).documentMapper().mapping().toXContent(orig, ToXContent.EMPTY_PARAMS);
        orig.endObject();
        XContentBuilder parsedFromOrig = JsonXContent.contentBuilder().startObject();
        createMapperService(orig).documentMapper().mapping().toXContent(parsedFromOrig, ToXContent.EMPTY_PARAMS);
        parsedFromOrig.endObject();
        assertEquals(orig.toString(), parsedFromOrig.toString());
        assertParseMinimalWarnings();
    }

    // TODO make this final once we remove FieldMapperTestCase2
    public void testMinimalToMaximal() throws IOException {
        XContentBuilder orig = JsonXContent.contentBuilder().startObject();
        createMapperService(fieldMapping(this::minimalMapping)).documentMapper().mapping().toXContent(orig, INCLUDE_DEFAULTS);
        orig.endObject();
        XContentBuilder parsedFromOrig = JsonXContent.contentBuilder().startObject();
        createMapperService(orig).documentMapper().mapping().toXContent(parsedFromOrig, INCLUDE_DEFAULTS);
        parsedFromOrig.endObject();
        assertEquals(orig.toString(), parsedFromOrig.toString());
        assertParseMaximalWarnings();
    }

    protected void assertParseMinimalWarnings() {
        // Most mappers don't emit any warnings
    }

    protected void assertParseMaximalWarnings() {
        // Most mappers don't emit any warnings
    }

    /**
     * Override to disable testing {@code meta} in fields that don't support it.
     */
    protected boolean supportsMeta() {
        return true;
    }

    protected void metaMapping(XContentBuilder b) throws IOException {
        minimalMapping(b);
    }

    public void testMeta() throws IOException {
        assumeTrue("Field doesn't support meta", supportsMeta());
        XContentBuilder mapping = fieldMapping(b -> {
            metaMapping(b);
            b.field("meta", Collections.singletonMap("foo", "bar"));
        });
        MapperService mapperService = createMapperService(mapping);
        assertEquals(
            XContentHelper.convertToMap(BytesReference.bytes(mapping), false, mapping.contentType()).v2(),
            XContentHelper.convertToMap(mapperService.documentMapper().mappingSource().uncompressed(), false, mapping.contentType()).v2()
        );

        mapping = fieldMapping(this::metaMapping);
        merge(mapperService, mapping);
        assertEquals(
            XContentHelper.convertToMap(BytesReference.bytes(mapping), false, mapping.contentType()).v2(),
            XContentHelper.convertToMap(mapperService.documentMapper().mappingSource().uncompressed(), false, mapping.contentType()).v2()
        );

        mapping = fieldMapping(b -> {
            metaMapping(b);
            b.field("meta", Collections.singletonMap("baz", "quux"));
        });
        merge(mapperService, mapping);
        assertEquals(
            XContentHelper.convertToMap(BytesReference.bytes(mapping), false, mapping.contentType()).v2(),
            XContentHelper.convertToMap(mapperService.documentMapper().mappingSource().uncompressed(), false, mapping.contentType()).v2()
        );
    }

    protected String typeName() throws IOException {
        MapperService ms = createMapperService(fieldMapping(this::minimalMapping));
        return ms.fieldType("field").typeName();
    }

    protected boolean supportsOrIgnoresBoost() {
        return true;
    }

    public void testDeprecatedBoost() throws IOException {
        assumeTrue("Does not support [boost] parameter", supportsOrIgnoresBoost());
        createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("boost", 2.0);
        }));
        String type = typeName();
        String[] warnings = new String[] {
            "Parameter [boost] on field [field] is deprecated and will be removed in 3.0",
            "Parameter [boost] has no effect on type [" + type + "] and will be removed in future" };
        allowedWarnings(warnings);
    }

    /**
     * Use a {@linkplain ValueFetcher} to extract values from doc values.
     */
    protected final List<?> fetchFromDocValues(MapperService mapperService, MappedFieldType ft, DocValueFormat format, Object sourceValue)
        throws IOException {

        BiFunction<MappedFieldType, Supplier<SearchLookup>, IndexFieldData<?>> fieldDataLookup = (mft, lookupSource) -> mft
            .fielddataBuilder("test", () -> {
                throw new UnsupportedOperationException();
            })
            .build(new IndexFieldDataCache.None(), new NoneCircuitBreakerService());
        SetOnce<List<?>> result = new SetOnce<>();
        withLuceneIndex(mapperService, iw -> {
            iw.addDocument(mapperService.documentMapper().parse(source(b -> b.field(ft.name(), sourceValue))).rootDoc());
        }, iw -> {
            SearchLookup lookup = new SearchLookup(mapperService, fieldDataLookup, SearchLookup.UNKNOWN_SHARD_ID);
            ValueFetcher valueFetcher = new DocValueFetcher(format, lookup.doc().getForField(ft));
            IndexSearcher searcher = newSearcher(iw);
            LeafReaderContext context = searcher.getIndexReader().leaves().get(0);
            lookup.source().setSegmentAndDocument(context, 0);
            valueFetcher.setNextReader(context);
            result.set(valueFetcher.fetchValues(lookup.source()));
        });
        return result.get();
    }

    private class UpdateCheck {
        final XContentBuilder init;
        final XContentBuilder update;
        final Consumer<FieldMapper> check;

        private UpdateCheck(CheckedConsumer<XContentBuilder, IOException> update, Consumer<FieldMapper> check) throws IOException {
            this.init = fieldMapping(MapperTestCase.this::minimalMapping);
            this.update = fieldMapping(b -> {
                minimalMapping(b);
                update.accept(b);
            });
            this.check = check;
        }

        private UpdateCheck(
            CheckedConsumer<XContentBuilder, IOException> init,
            CheckedConsumer<XContentBuilder, IOException> update,
            Consumer<FieldMapper> check
        ) throws IOException {
            this.init = fieldMapping(init);
            this.update = fieldMapping(update);
            this.check = check;
        }
    }

    private static class ConflictCheck {
        final XContentBuilder init;
        final XContentBuilder update;

        private ConflictCheck(XContentBuilder init, XContentBuilder update) {
            this.init = init;
            this.update = update;
        }
    }

    public class ParameterChecker {

        List<UpdateCheck> updateChecks = new ArrayList<>();
        Map<String, ConflictCheck> conflictChecks = new HashMap<>();

        /**
         * Register a check that a parameter can be updated, using the minimal mapping as a base
         *
         * @param update a field builder applied on top of the minimal mapping
         * @param check  a check that the updated parameter has been applied to the FieldMapper
         */
        public void registerUpdateCheck(CheckedConsumer<XContentBuilder, IOException> update, Consumer<FieldMapper> check)
            throws IOException {
            updateChecks.add(new UpdateCheck(update, check));
        }

        /**
         * Register a check that a parameter can be updated
         *
         * @param init   the initial mapping
         * @param update the updated mapping
         * @param check  a check that the updated parameter has been applied to the FieldMapper
         */
        public void registerUpdateCheck(
            CheckedConsumer<XContentBuilder, IOException> init,
            CheckedConsumer<XContentBuilder, IOException> update,
            Consumer<FieldMapper> check
        ) throws IOException {
            updateChecks.add(new UpdateCheck(init, update, check));
        }

        /**
         * Register a check that a parameter update will cause a conflict, using the minimal mapping as a base
         *
         * @param param  the parameter name, expected to appear in the error message
         * @param update a field builder applied on top of the minimal mapping
         */
        public void registerConflictCheck(String param, CheckedConsumer<XContentBuilder, IOException> update) throws IOException {
            conflictChecks.put(param, new ConflictCheck(fieldMapping(MapperTestCase.this::minimalMapping), fieldMapping(b -> {
                minimalMapping(b);
                update.accept(b);
            })));
        }

        /**
         * Register a check that a parameter update will cause a conflict
         *
         * @param param  the parameter name, expected to appear in the error message
         * @param init   the initial mapping
         * @param update the updated mapping
         */
        public void registerConflictCheck(String param, XContentBuilder init, XContentBuilder update) {
            conflictChecks.put(param, new ConflictCheck(init, update));
        }
    }

    protected abstract void registerParameters(ParameterChecker checker) throws IOException;

    public void testUpdates() throws IOException {
        ParameterChecker checker = new ParameterChecker();
        registerParameters(checker);
        for (UpdateCheck updateCheck : checker.updateChecks) {
            MapperService mapperService = createMapperService(updateCheck.init);
            merge(mapperService, updateCheck.update);
            FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
            updateCheck.check.accept(mapper);
            // do it again to ensure that we don't get conflicts the second time
            merge(mapperService, updateCheck.update);
            mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
            updateCheck.check.accept(mapper);

        }
        for (String param : checker.conflictChecks.keySet()) {
            MapperService mapperService = createMapperService(checker.conflictChecks.get(param).init);
            // merging the same change is fine
            merge(mapperService, checker.conflictChecks.get(param).init);
            // merging the conflicting update should throw an exception
            Exception e = expectThrows(
                IllegalArgumentException.class,
                "No conflict when updating parameter [" + param + "]",
                () -> merge(mapperService, checker.conflictChecks.get(param).update)
            );
            assertThat(
                e.getMessage(),
                anyOf(containsString("Cannot update parameter [" + param + "]"), containsString("different [" + param + "]"))
            );
        }
        assertParseMaximalWarnings();
    }

}
