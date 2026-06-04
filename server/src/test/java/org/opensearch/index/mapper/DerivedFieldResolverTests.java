/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.opensearch.OpenSearchException;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.core.index.Index;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.script.Script;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DerivedFieldResolverTests extends MapperServiceTestCase {
    public void testResolutionFromIndexMapping() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startObject("properties");
            {
                b.startObject("indexed_field");
                {
                    b.field("type", "text");
                }
                b.endObject();
            }
            b.endObject();
            b.startObject("derived");
            {
                b.startObject("derived_text");
                {
                    b.field("type", "keyword");
                    b.field("script", "");
                }
                b.endObject();
            }
            b.endObject();
        }));
        QueryShardContext queryShardContext = createQueryShardContext(mapperService);
        when(queryShardContext.index()).thenReturn(new Index("test_index", "uuid"));
        DefaultDerivedFieldResolver resolver = (DefaultDerivedFieldResolver) DerivedFieldResolverFactory.createResolver(
            queryShardContext,
            null,
            null,
            true
        );
        assertEquals("keyword", resolver.resolve("derived_text").getType());
        assertEqualDerivedField(new DerivedField("derived_text", "keyword", new Script("")), resolver.resolve("derived_text").derivedField);
    }

    public void testResolutionFromSearchRequest() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {}));
        QueryShardContext queryShardContext = createQueryShardContext(mapperService);
        when(queryShardContext.index()).thenReturn(new Index("test_index", "uuid"));
        DefaultDerivedFieldResolver resolver = (DefaultDerivedFieldResolver) DerivedFieldResolverFactory.createResolver(
            queryShardContext,
            createDerivedFieldsObject(),
            createDerivedFields(),
            true
        );
        assertEquals("text", resolver.resolve("derived_text").getType());
        assertEqualDerivedField(new DerivedField("derived_text", "text", new Script("")), resolver.resolve("derived_text").derivedField);
        assertEquals("object", resolver.resolve("derived_object").getType());
        assertEqualDerivedField(
            new DerivedField("derived_object", "object", new Script("")),
            resolver.resolve("derived_object").derivedField
        );
        assertEquals("keyword", resolver.resolve("derived_keyword").getType());
        assertEqualDerivedField(
            new DerivedField("derived_keyword", "keyword", new Script("")),
            resolver.resolve("derived_keyword").derivedField
        );
    }

    public void testEmpty() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {}));
        QueryShardContext queryShardContext = createQueryShardContext(mapperService);
        when(queryShardContext.index()).thenReturn(new Index("test_index", "uuid"));
        DefaultDerivedFieldResolver resolver = (DefaultDerivedFieldResolver) DerivedFieldResolverFactory.createResolver(
            queryShardContext,
            null,
            null,
            true
        );
        assertNull(resolver.resolve("derived_keyword"));
    }

    public void testResolutionPrecedence() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startObject("properties");
            {
                b.startObject("indexed_field");
                {
                    b.field("type", "text");
                }
                b.endObject();
            }
            b.endObject();
            b.startObject("derived");
            {
                b.startObject("derived_text");
                {
                    b.field("type", "keyword");
                    b.field("script", "");
                }
                b.endObject();
                b.startObject("derived_2");
                {
                    b.field("type", "keyword");
                    b.field("script", "");
                }
                b.endObject();
            }
            b.endObject();
        }));
        QueryShardContext queryShardContext = createQueryShardContext(mapperService);
        when(queryShardContext.index()).thenReturn(new Index("test_index", "uuid"));
        DefaultDerivedFieldResolver resolver = (DefaultDerivedFieldResolver) DerivedFieldResolverFactory.createResolver(
            queryShardContext,
            createDerivedFieldsObject(),
            createDerivedFields(),
            true
        );

        // precedence given to search definition; derived_text is present in both -
        // search definition uses type text, whereas index definition uses the type keyword

        assertEquals("text", resolver.resolve("derived_text").getType());
        assertEqualDerivedField(new DerivedField("derived_text", "text", new Script("")), resolver.resolve("derived_text").derivedField);

        assertEquals("keyword", resolver.resolve("derived_2").getType());
        assertEqualDerivedField(new DerivedField("derived_2", "keyword", new Script("")), resolver.resolve("derived_2").derivedField);
    }

    public void testNestedWithParentDefinedInIndexMapping() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startObject("properties");
            {
                b.startObject("indexed_field");
                {
                    b.field("type", "text");
                }
                b.endObject();
            }
            b.endObject();
            b.startObject("derived");
            {
                b.startObject("derived_obj");
                {
                    b.field("type", "object");
                    b.field("script", "");
                }
                b.endObject();
                b.startObject("derived_obj_2");
                {
                    b.field("type", "object");
                    b.field("script", "");
                    b.field("format", "yyyy-MM-dd");

                    b.startObject("properties");
                    {
                        b.field("sub_field1", "long");
                        b.field("sub_field2", "date");
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));
        QueryShardContext queryShardContext = createQueryShardContext(mapperService);
        when(queryShardContext.index()).thenReturn(new Index("test_index", "uuid"));

        try (Directory dir = newDirectory()) {
            IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));
            Document d = new Document();
            for (int i = 0; i < 10; i++) {
                iw.addDocument(d);
            }
            try (IndexReader reader = DirectoryReader.open(iw)) {
                iw.close();
                FieldTypeInference typeInference = new FieldTypeInference("test_index", queryShardContext.getMapperService(), reader);
                TestDerivedFieldResolver resolver = new TestDerivedFieldResolver(queryShardContext, null, null, typeInference);
                assertEquals("text", resolver.resolve("derived_obj.sub_field1").getType());
                assertEqualDerivedField(
                    new DerivedField("derived_obj.sub_field1", "text", new Script("")),
                    resolver.resolve("derived_obj.sub_field1").derivedField
                );
                assertEquals("text", resolver.resolve("derived_obj.sub_field1.sub_field2").getType());
                assertEqualDerivedField(
                    new DerivedField("derived_obj.sub_field1.sub_field2", "text", new Script("")),
                    resolver.resolve("derived_obj.sub_field1.sub_field2").derivedField
                );
                // when explicit type is set in properties
                DerivedField expectedDerivedField1 = new DerivedField("derived_obj_2.sub_field1", "long", new Script(""));
                expectedDerivedField1.setProperties(Map.of("sub_field1", "long", "sub_field2", "date"));
                expectedDerivedField1.setFormat("yyyy-MM-dd");
                assertEqualDerivedField(expectedDerivedField1, resolver.resolve("derived_obj_2.sub_field1").derivedField);
                DerivedField expectedDerivedField2 = new DerivedField("derived_obj_2.sub_field2", "date", new Script(""));
                expectedDerivedField2.setProperties(Map.of("sub_field1", "long", "sub_field2", "date"));
                expectedDerivedField2.setFormat("yyyy-MM-dd");
                assertEqualDerivedField(expectedDerivedField2, resolver.resolve("derived_obj_2.sub_field2").derivedField);
            }
        }
    }

    public void testNestedWithParentDefinedInSearchRequest() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {}));
        QueryShardContext queryShardContext = createQueryShardContext(mapperService);
        when(queryShardContext.index()).thenReturn(new Index("test_index", "uuid"));
        try (Directory dir = newDirectory()) {
            IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));
            Document d = new Document();
            for (int i = 0; i < 10; i++) {
                iw.addDocument(d);
            }
            try (IndexReader reader = DirectoryReader.open(iw)) {
                iw.close();
                FieldTypeInference typeInference = new FieldTypeInference("test_index", queryShardContext.getMapperService(), reader);
                TestDerivedFieldResolver resolver = new TestDerivedFieldResolver(
                    queryShardContext,
                    createDerivedFieldsObject(),
                    createDerivedFields(),
                    typeInference
                );
                assertEquals("text", resolver.resolve("derived_object.sub_field1").getType());
                assertEqualDerivedField(
                    new DerivedField("derived_object.sub_field1", "text", new Script("")),
                    resolver.resolve("derived_object.sub_field1").derivedField
                );
                assertEquals("text", resolver.resolve("derived_object.sub_field1.sub_field2").getType());
                assertEqualDerivedField(
                    new DerivedField("derived_object.sub_field1.sub_field2", "text", new Script("")),
                    resolver.resolve("derived_object.sub_field1.sub_field2").derivedField
                );
                assertEquals(2, resolver.cnt);

            }
        }
    }

    public void testNestedWithParentUndefined() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {}));
        QueryShardContext queryShardContext = createQueryShardContext(mapperService);
        when(queryShardContext.index()).thenReturn(new Index("test_index", "uuid"));
        try (Directory dir = newDirectory()) {
            IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));
            Document d = new Document();
            for (int i = 0; i < 10; i++) {
                iw.addDocument(d);
            }
            try (IndexReader reader = DirectoryReader.open(iw)) {
                iw.close();
                FieldTypeInference typeInference = new FieldTypeInference("test_index", queryShardContext.getMapperService(), reader);
                TestDerivedFieldResolver resolver = new TestDerivedFieldResolver(queryShardContext, null, null, typeInference);
                assertNull(resolver.resolve("derived_object.sub_field1"));
            }
        }
    }

    public void testInferredTypeNull() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {}));
        QueryShardContext queryShardContext = createQueryShardContext(mapperService);
        when(queryShardContext.index()).thenReturn(new Index("test_index", "uuid"));
        try (Directory dir = newDirectory()) {
            IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));
            Document d = new Document();
            for (int i = 0; i < 10; i++) {
                iw.addDocument(d);
            }
            try (IndexReader reader = DirectoryReader.open(iw)) {
                iw.close();
                FieldTypeInference typeInference = new FieldTypeInference("test_index", queryShardContext.getMapperService(), reader);
                TestDerivedFieldResolver resolver = new TestDerivedFieldResolver(
                    queryShardContext,
                    createDerivedFieldsObject(),
                    createDerivedFields(),
                    typeInference,
                    true
                );
                assertNull(resolver.resolve("derived_object.field"));
            }
        }
    }

    public void testInferThrowsIOException() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {}));
        QueryShardContext queryShardContext = createQueryShardContext(mapperService);
        when(queryShardContext.index()).thenReturn(new Index("test_index", "uuid"));
        FieldTypeInference typeInferenceMock = mock(FieldTypeInference.class);
        when(typeInferenceMock.infer(any())).thenThrow(new IOException("Simulated IOException"));
        TestDerivedFieldResolver resolver = new TestDerivedFieldResolver(
            queryShardContext,
            createDerivedFieldsObject(),
            createDerivedFields(),
            typeInferenceMock,
            true
        );
        assertNull(resolver.resolve("derived_object.field"));
    }

    public void testRegularFieldTypesAreNotResolved() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startObject("properties");
            {
                b.startObject("indexed_field");
                {
                    b.field("type", "text");
                }
                b.endObject();
                b.startObject("indexed_field_2.sub_field");
                {
                    b.field("type", "text");
                }
                b.endObject();
            }
            b.endObject();
            b.startObject("derived");
            {
                b.startObject("derived_text");
                {
                    b.field("type", "keyword");
                    b.field("script", "");
                }
                b.endObject();
            }
            b.endObject();
        }));
        QueryShardContext queryShardContext = createQueryShardContext(mapperService);
        when(queryShardContext.index()).thenReturn(new Index("test_index", "uuid"));
        DefaultDerivedFieldResolver resolver = (DefaultDerivedFieldResolver) DerivedFieldResolverFactory.createResolver(
            queryShardContext,
            null,
            null,
            true
        );
        assertNull(resolver.resolve("indexed_field"));
        assertNull(resolver.resolve("indexed_field_2.sub_field"));
    }

    public void testResolutionCaching() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {}));
        QueryShardContext queryShardContext = createQueryShardContext(mapperService);
        when(queryShardContext.index()).thenReturn(new Index("test_index", "uuid"));
        try (Directory dir = newDirectory()) {
            IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));
            Document d = new Document();
            for (int i = 0; i < 10; i++) {
                iw.addDocument(d);
            }
            try (IndexReader reader = DirectoryReader.open(iw)) {
                iw.close();
                FieldTypeInference typeInference = new FieldTypeInference("test_index", queryShardContext.getMapperService(), reader);
                TestDerivedFieldResolver resolver = new TestDerivedFieldResolver(
                    queryShardContext,
                    createDerivedFieldsObject(),
                    createDerivedFields(),
                    typeInference
                );
                assertEquals("text", resolver.resolve("derived_object.sub_field1").getType());
                assertEquals("text", resolver.resolve("derived_object.sub_field1").getType());
                assertEquals(1, resolver.cnt);
            }
        }
    }

    public void testResolutionDisabled() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startObject("properties");
            {
                b.startObject("indexed_field");
                {
                    b.field("type", "text");
                }
                b.endObject();
                b.startObject("indexed_field_2.sub_field");
                {
                    b.field("type", "text");
                }
                b.endObject();
            }
            b.endObject();
            b.startObject("derived");
            {
                b.startObject("derived_text");
                {
                    b.field("type", "keyword");
                    b.field("script", "");
                }
                b.endObject();
            }
            b.endObject();
        }));
        QueryShardContext queryShardContext = createQueryShardContext(mapperService);
        when(queryShardContext.index()).thenReturn(new Index("test_index", "uuid"));
        DerivedFieldResolver resolver = DerivedFieldResolverFactory.createResolver(queryShardContext, null, null, false);
        assertTrue(resolver instanceof NoOpDerivedFieldResolver);
        assertNull(resolver.resolve("derived_text"));
        assertEquals(0, resolver.resolvePattern("*").size());
        assertNull(resolver.resolve("indexed_field"));
        assertNull(resolver.resolve("indexed_field_2.sub_field"));

        assertThrows(
            OpenSearchException.class,
            () -> DerivedFieldResolverFactory.createResolver(queryShardContext, createDerivedFieldsObject(), createDerivedFields(), false)
        );

        when(queryShardContext.allowExpensiveQueries()).thenReturn(false);

        assertThrows(
            OpenSearchException.class,
            () -> DerivedFieldResolverFactory.createResolver(queryShardContext, createDerivedFieldsObject(), createDerivedFields(), true)
        );
    }

    public void testResolvePattern() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startObject("properties");
            {
                b.startObject("indexed_field");
                {
                    b.field("type", "text");
                }
                b.endObject();
            }
            b.endObject();
            b.startObject("derived");
            {
                b.startObject("derived_text");
                {
                    b.field("type", "keyword");
                    b.field("script", "");
                }
                b.endObject();
                b.startObject("derived_2");
                {
                    b.field("type", "keyword");
                    b.field("script", "");
                }
                b.endObject();
            }
            b.endObject();
        }));
        QueryShardContext queryShardContext = createQueryShardContext(mapperService);
        when(queryShardContext.index()).thenReturn(new Index("test_index", "uuid"));
        DefaultDerivedFieldResolver resolver = (DefaultDerivedFieldResolver) DerivedFieldResolverFactory.createResolver(
            queryShardContext,
            createDerivedFieldsObject(),
            createDerivedFields(),
            true
        );
        assertEquals(4, resolver.resolvePattern("derived_*").size());
        assertEquals(4, resolver.resolvePattern("*").size()); // should not include regular field indexed_field
    }

    private void assertEqualDerivedField(DerivedField expected, DerivedField actual) {
        assertEquals(expected, actual);
    }

    private Map<String, Object> createDerivedFieldsObject() {
        return new HashMap<>() {
            {
                put("derived_text", new HashMap<String, Object>() {
                    {
                        put("type", "text");
                        put("script", "");
                    }
                });
                put("derived_object", new HashMap<String, Object>() {
                    {
                        put("type", "object");
                        put("script", "");
                    }
                });
            }
        };
    }

    private static class TestDerivedFieldResolver extends DefaultDerivedFieldResolver {
        private final boolean error;
        private int cnt;

        public TestDerivedFieldResolver(
            QueryShardContext queryShardContext,
            Map<String, Object> derivedFieldsObject,
            List<DerivedField> derivedFields,
            FieldTypeInference typeInference
        ) {
            this(queryShardContext, derivedFieldsObject, derivedFields, typeInference, false);
        }

        public TestDerivedFieldResolver(
            QueryShardContext queryShardContext,
            Map<String, Object> derivedFieldsObject,
            List<DerivedField> derivedFields,
            FieldTypeInference typeInference,
            boolean error
        ) {
            super(queryShardContext, derivedFieldsObject, derivedFields, typeInference);
            this.error = error;
            this.cnt = 0;
        }

        @Override
        ValueFetcher getValueFetcher(String fieldName, Script script, boolean ignoreMalFormed) {
            cnt++;
            if (!error) {
                return lookup -> List.of("text field content");
            } else {
                return lookup -> null;
            }
        }
    }

    private List<DerivedField> createDerivedFields() {
        DerivedField derivedField = new DerivedField("derived_keyword", "keyword", new Script(""));
        return Collections.singletonList(derivedField);
    }

}
