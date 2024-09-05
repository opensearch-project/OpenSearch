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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.core.index.Index;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.when;

public class FieldTypeInferenceTests extends MapperServiceTestCase {

    private static final Map<String, List<Object>> documentMap;
    static {
        List<Object> listWithNull = new ArrayList<>();
        listWithNull.add(null);
        documentMap = new HashMap<>();
        documentMap.put("text_field", List.of("The quick brown fox jumps over the lazy dog."));
        documentMap.put("int_field", List.of(789));
        documentMap.put("float_field", List.of(123.45));
        documentMap.put("date_field_1", List.of("2024-05-12T15:45:00Z"));
        documentMap.put("date_field_2", List.of("2024-05-12"));
        documentMap.put("boolean_field", List.of(true));
        documentMap.put("null_field", listWithNull);
        documentMap.put("array_field_int", List.of(100, 200, 300, 400, 500));
        documentMap.put("array_field_text", List.of("100", "200"));
        documentMap.put("object_type", List.of(Map.of("foo", Map.of("bar", 10))));
    }

    public void testJsonSupportedTypes() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {}));
        QueryShardContext queryShardContext = createQueryShardContext(mapperService);
        when(queryShardContext.index()).thenReturn(new Index("test_index", "uuid"));
        int totalDocs = 10000;
        int docsPerLeafCount = 1000;
        try (Directory dir = newDirectory()) {
            IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));
            Document d = new Document();
            for (int i = 0; i < totalDocs; i++) {
                iw.addDocument(d);
                if ((i + 1) % docsPerLeafCount == 0) {
                    iw.commit();
                }
            }
            try (IndexReader reader = DirectoryReader.open(iw)) {
                iw.close();
                FieldTypeInference typeInference = new FieldTypeInference("test_index", queryShardContext.getMapperService(), reader);
                String[] fieldName = { "text_field" };
                Mapper mapper = typeInference.infer(lookup -> documentMap.get(fieldName[0]));
                assertEquals("text", mapper.typeName());

                fieldName[0] = "int_field";
                mapper = typeInference.infer(lookup -> documentMap.get(fieldName[0]));
                assertEquals("long", mapper.typeName());

                fieldName[0] = "float_field";
                mapper = typeInference.infer(lookup -> documentMap.get(fieldName[0]));
                assertEquals("float", mapper.typeName());

                fieldName[0] = "date_field_1";
                mapper = typeInference.infer(lookup -> documentMap.get(fieldName[0]));
                assertEquals("date", mapper.typeName());

                fieldName[0] = "date_field_2";
                mapper = typeInference.infer(lookup -> documentMap.get(fieldName[0]));
                assertEquals("date", mapper.typeName());

                fieldName[0] = "boolean_field";
                mapper = typeInference.infer(lookup -> documentMap.get(fieldName[0]));
                assertEquals("boolean", mapper.typeName());

                fieldName[0] = "array_field_int";
                mapper = typeInference.infer(lookup -> documentMap.get(fieldName[0]));
                assertEquals("long", mapper.typeName());

                fieldName[0] = "array_field_text";
                mapper = typeInference.infer(lookup -> documentMap.get(fieldName[0]));
                assertEquals("text", mapper.typeName());

                fieldName[0] = "object_type";
                mapper = typeInference.infer(lookup -> documentMap.get(fieldName[0]));
                assertEquals("object", mapper.typeName());

                fieldName[0] = "null_field";
                mapper = typeInference.infer(lookup -> documentMap.get(fieldName[0]));
                assertNull(mapper);

                // If field is missing ensure that sample docIDs generated for inference are ordered and are in bounds
                fieldName[0] = "missing_field";
                List<List<Integer>> docsEvaluated = new ArrayList<>();
                int[] totalDocsEvaluated = { 0 };
                typeInference.setSampleSize(50);
                mapper = typeInference.infer(new ValueFetcher() {
                    @Override
                    public List<Object> fetchValues(SourceLookup lookup) throws IOException {
                        docsEvaluated.get(docsEvaluated.size() - 1).add(lookup.docId());
                        totalDocsEvaluated[0]++;
                        return documentMap.get(fieldName[0]);
                    }

                    @Override
                    public void setNextReader(LeafReaderContext leafReaderContext) {
                        docsEvaluated.add(new ArrayList<>());
                    }
                });
                assertNull(mapper);
                assertEquals(typeInference.getSampleSize(), totalDocsEvaluated[0]);
                for (List<Integer> docsPerLeaf : docsEvaluated) {
                    for (int j = 0; j < docsPerLeaf.size() - 1; j++) {
                        assertTrue(docsPerLeaf.get(j) < docsPerLeaf.get(j + 1));
                    }
                    if (!docsPerLeaf.isEmpty()) {
                        assertTrue(docsPerLeaf.get(0) >= 0 && docsPerLeaf.get(docsPerLeaf.size() - 1) < docsPerLeafCount);
                    }
                }
            }
        }
    }

    public void testDeleteAllDocs() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {}));
        QueryShardContext queryShardContext = createQueryShardContext(mapperService);
        when(queryShardContext.index()).thenReturn(new Index("test_index", "uuid"));
        int totalDocs = 10000;
        int docsPerLeafCount = 1000;
        try (Directory dir = newDirectory()) {
            IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));
            Document d = new Document();
            for (int i = 0; i < totalDocs; i++) {
                iw.addDocument(d);
                if ((i + 1) % docsPerLeafCount == 0) {
                    iw.commit();
                }
            }
            iw.deleteAll();
            iw.commit();

            try (IndexReader reader = DirectoryReader.open(iw)) {
                iw.close();
                FieldTypeInference typeInference = new FieldTypeInference("test_index", queryShardContext.getMapperService(), reader);
                String[] fieldName = { "text_field" };
                Mapper mapper = typeInference.infer(lookup -> documentMap.get(fieldName[0]));
                assertNull(mapper);
            }
        }
    }

    public void testZeroDoc() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {}));
        QueryShardContext queryShardContext = createQueryShardContext(mapperService);
        when(queryShardContext.index()).thenReturn(new Index("test_index", "uuid"));
        try (Directory dir = newDirectory()) {
            IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));
            try (IndexReader reader = DirectoryReader.open(iw)) {
                iw.close();
                FieldTypeInference typeInference = new FieldTypeInference("test_index", queryShardContext.getMapperService(), reader);
                String[] fieldName = { "text_field" };
                Mapper mapper = typeInference.infer(lookup -> documentMap.get(fieldName[0]));
                assertNull(mapper);
            }
        }
    }

    public void testSampleGeneration() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {}));
        QueryShardContext queryShardContext = createQueryShardContext(mapperService);
        when(queryShardContext.index()).thenReturn(new Index("test_index", "uuid"));
        int totalDocs = 10000;
        int docsPerLeafCount = 1000;
        try (Directory dir = newDirectory()) {
            IndexWriter iw = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));
            Document d = new Document();
            for (int i = 0; i < totalDocs; i++) {
                iw.addDocument(d);
                if ((i + 1) % docsPerLeafCount == 0) {
                    iw.commit();
                }
            }
            try (IndexReader reader = DirectoryReader.open(iw)) {
                iw.close();
                FieldTypeInference typeInference = new FieldTypeInference("test_index", queryShardContext.getMapperService(), reader);
                typeInference.setSampleSize(1000 - 1);
                typeInference.infer(lookup -> documentMap.get("unknown_field"));
                assertThrows(IllegalArgumentException.class, () -> typeInference.setSampleSize(1000 + 1));
                typeInference.setSampleSize(1000);
                typeInference.infer(lookup -> documentMap.get("unknown_field"));
            }
        }
    }
}
