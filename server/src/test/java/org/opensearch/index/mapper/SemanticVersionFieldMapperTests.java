/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.document.KeywordField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.opensearch.common.geo.ShapeRelation;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryShardException;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.mockito.Mockito.mock;

/**
 * Field mapper for OpenSearch semantic version field type
 */
public class SemanticVersionFieldMapperTests extends MapperTestCase {

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "version");
        b.field("store", true);
        b.field("index", true);
    }

    @Override
    protected void registerParameters(ParameterChecker checker) {
        // No additional parameters to register for version field type
    }

    @Override
    protected void metaMapping(XContentBuilder b) throws IOException {
        minimalMapping(b);
    }

    @Override
    protected void writeFieldValue(XContentBuilder builder) throws IOException {
        builder.value("1.0.0");
    }

    public void testDefaults() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1.0.0")));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertThat(fields.length, greaterThanOrEqualTo(2));

        boolean hasDocValues = false;
        boolean hasStoredField = false;
        for (IndexableField field : fields) {
            if (field.fieldType().docValuesType() == DocValuesType.SORTED_SET) {
                hasDocValues = true;
            }
            if (field.fieldType().stored()) {
                hasStoredField = true;
            }
        }

        assertTrue("Field should have doc values", hasDocValues);
        assertTrue("Field should be stored", hasStoredField);
    }

    public void testValidVersionValues() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        // Test regular version
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1.2.3")));
        IndexableField field = doc.rootDoc().getField("field");
        assertThat(field.stringValue(), equalTo("1.2.3"));

        // Test version with pre-release
        doc = mapper.parse(source(b -> b.field("field", "1.2.3-alpha")));
        field = doc.rootDoc().getField("field");
        assertThat(field.stringValue(), equalTo("1.2.3-alpha"));

        // Test version with build metadata
        doc = mapper.parse(source(b -> b.field("field", "1.2.3+build.123")));
        field = doc.rootDoc().getField("field");
        assertThat(field.stringValue(), equalTo("1.2.3+build.123"));

        // Test version with both pre-release and build metadata
        doc = mapper.parse(source(b -> b.field("field", "1.2.3-alpha+build.123")));
        field = doc.rootDoc().getField("field");
        assertThat(field.stringValue(), equalTo("1.2.3-alpha+build.123"));
    }

    public void testStoredValues() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        // Test storing different version formats
        List<String> versions = Arrays.asList(
            "1.0.0",
            "1.0.0-alpha",
            "1.0.0-alpha.1",
            "1.0.0+build.123",
            "1.0.0-beta+build.123",
            "999999999.999999999.999999999"
        );

        for (String version : versions) {
            ParsedDocument doc = mapper.parse(source(b -> b.field("field", version)));
            IndexableField storedField = doc.rootDoc().getField("field");
            assertNotNull("Stored field should exist for " + version, storedField);
            assertEquals("Stored value should match input for " + version, version, storedField.stringValue());
        }
    }

    public void testDocValuesSorting() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1.0.0")));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        boolean hasDocValues = false;
        for (IndexableField field : fields) {
            if (field.fieldType().docValuesType() == DocValuesType.SORTED_SET) {
                hasDocValues = true;
                assertEquals(DocValuesType.SORTED_SET, field.fieldType().docValuesType());
            }
        }
        assertTrue("Field should have sorted doc values", hasDocValues);
    }

    public void testNullHandling() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        // Test null value
        ParsedDocument doc = mapper.parse(source(b -> b.nullField("field")));
        assertThat(doc.rootDoc().getFields("field"), arrayWithSize(0));

        // Test missing field
        doc = mapper.parse(source(b -> {}));
        assertThat(doc.rootDoc().getFields("field"), arrayWithSize(0));
    }

    public void testMalformedVersions() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        // Test various malformed versions
        List<String> malformedVersions = Arrays.asList(
            "1",
            "1.0",
            "v1.0.0",
            "1.0.0.0",
            "-1.0.0",
            "1.0.0-",
            "1.0.0+",
            "01.0.0",
            "1.0.0@invalid"
        );

        for (String malformed : malformedVersions) {
            MapperParsingException e = expectThrows(
                MapperParsingException.class,
                () -> mapper.parse(source(b -> b.field("field", malformed)))
            );
            assertTrue(e.getCause().getMessage().contains("Invalid semantic version format"));
        }
    }

    public void testMetadataFields() throws Exception {
        XContentBuilder mapping = fieldMapping(this::minimalMapping);
        DocumentMapper mapper = createDocumentMapper(mapping);
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1.0.0")));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        boolean hasDocValues = false;
        for (IndexableField field : fields) {
            if (field.fieldType().docValuesType() == DocValuesType.SORTED_SET) {
                hasDocValues = true;
                break;
            }
        }
        assertTrue("Field should have doc values", hasDocValues);
    }

    public void testToXContent() throws IOException {
        XContentBuilder mapping = fieldMapping(this::minimalMapping);
        DocumentMapper mapper = createDocumentMapper(mapping);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        mapper.mapping().toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        String mappingString = builder.toString();
        assertTrue(mappingString.contains("\"type\":\"version\""));
    }

    public void testMultipleVersionFields() throws Exception {
        XContentBuilder mapping = mapping(b -> {
            b.startObject("version1").field("type", "version").field("store", true).endObject();
            b.startObject("version2").field("type", "version").field("store", true).endObject();
        });

        DocumentMapper mapper = createDocumentMapper(mapping);
        ParsedDocument doc = mapper.parse(source(b -> {
            b.field("version1", "1.0.0");
            b.field("version2", "2.0.0");
        }));

        assertNotNull(doc.rootDoc().getField("version1"));
        assertNotNull(doc.rootDoc().getField("version2"));
        assertNotEquals(doc.rootDoc().getField("version1").stringValue(), doc.rootDoc().getField("version2").stringValue());
    }

    public void testMajorVersionComparison() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        ParsedDocument doc1 = mapper.parse(source(b -> b.field("field", "2.0.0")));
        ParsedDocument doc2 = mapper.parse(source(b -> b.field("field", "1.9.9")));

        SemanticVersion v1 = SemanticVersion.parse(doc1.rootDoc().getField("field").stringValue());
        SemanticVersion v2 = SemanticVersion.parse(doc2.rootDoc().getField("field").stringValue());

        assertTrue("Major version comparison failed", v1.compareTo(v2) > 0);
    }

    public void testComplexPreReleaseComparison() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        List<String> versions = Arrays.asList(
            "1.0.0-alpha.beta",
            "1.0.0-alpha.1",
            "1.0.0-alpha",
            "1.0.0-alpha.beta.2",
            "1.0.0-beta",
            "1.0.0-alpha.12",
            "1.0.0-beta.2",
            "1.0.0"
        );

        List<String> expected = Arrays.asList(
            "1.0.0-alpha",
            "1.0.0-alpha.1",
            "1.0.0-alpha.12",
            "1.0.0-alpha.beta",
            "1.0.0-alpha.beta.2",
            "1.0.0-beta",
            "1.0.0-beta.2",
            "1.0.0"
        );

        testVersionSorting(mapper, versions, expected);
    }

    public void testBuildMetadataEquality() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        ParsedDocument doc1 = mapper.parse(source(b -> b.field("field", "1.0.0+build.1")));
        ParsedDocument doc2 = mapper.parse(source(b -> b.field("field", "1.0.0+build.2")));

        SemanticVersion v1 = SemanticVersion.parse(doc1.rootDoc().getField("field").stringValue());
        SemanticVersion v2 = SemanticVersion.parse(doc2.rootDoc().getField("field").stringValue());

        assertEquals("Build metadata should not affect equality", 0, v1.compareTo(v2));
    }

    public void testMultipleFieldsInDocument() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("version1").field("type", "version").field("store", true).endObject();
            b.startObject("version2").field("type", "version").field("store", true).endObject();
        }));

        ParsedDocument doc = mapper.parse(source(b -> {
            b.field("version1", "1.0.0");
            b.field("version2", "2.0.0");
        }));

        assertNotNull(doc.rootDoc().getField("version1"));
        assertNotNull(doc.rootDoc().getField("version2"));
        assertNotEquals(doc.rootDoc().getField("version1").stringValue(), doc.rootDoc().getField("version2").stringValue());
    }

    public void testExtremeVersionNumbers() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        // Test very large version numbers
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "999999999.999999999.999999999")));
        assertNotNull(doc.rootDoc().getField("field"));

        // Test version with many pre-release parts
        doc = mapper.parse(source(b -> b.field("field", "1.0.0-alpha.beta.gamma.delta.epsilon")));
        assertNotNull(doc.rootDoc().getField("field"));

        // Test version with many build metadata parts
        doc = mapper.parse(source(b -> b.field("field", "1.0.0+build.123.456.789.abc.def")));
        assertNotNull(doc.rootDoc().getField("field"));
    }

    public void testMoreInvalidVersions() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        // Test empty string
        expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> b.field("field", ""))));

        // Test only dots
        expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> b.field("field", "..."))));

        // Test invalid characters
        expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> b.field("field", "1.0.0@"))));

        // Test leading zeros
        expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> b.field("field", "01.2.3"))));
    }

    private void testVersionSorting(DocumentMapper mapper, List<String> input, List<String> expected) throws Exception {
        List<String> actual = input.stream().map(v -> {
            try {
                ParsedDocument doc = mapper.parse(source(b -> b.field("field", v)));
                return doc.rootDoc().getField("field").stringValue();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).sorted((v1, v2) -> {
            SemanticVersion sv1 = SemanticVersion.parse(v1);
            SemanticVersion sv2 = SemanticVersion.parse(v2);
            return sv1.compareTo(sv2);
        }).collect(Collectors.toList());

        assertThat(actual, contains(expected.toArray()));
    }

    public void testInvalidVersionValues() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        // Test invalid version format
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> b.field("field", "1.2"))));
        assertTrue(e.getCause().getMessage().contains("Invalid semantic version format"));

        // Test negative version numbers
        e = expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> b.field("field", "-1.2.3"))));
        assertTrue(e.getCause().getMessage().contains("Invalid semantic version format"));

        // Test invalid pre-release format
        e = expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> b.field("field", "1.2.3-"))));
        assertTrue(e.getCause().getMessage().contains("Invalid semantic version format"));
    }

    public void testVersionSorting() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        // Create a list of versions in random order
        List<String> versions = Arrays.asList(
            "1.0.0",
            "1.10.0",
            "1.2.0",
            "1.0.0-alpha",
            "1.0.0-alpha.1",
            "1.0.0-beta",
            "1.0.0-beta.2",
            "1.0.0-beta.11",
            "1.0.0-rc.1",
            "1.0.0+build.123"
        );
        Collections.shuffle(versions, random());

        // Store documents with versions
        List<ParsedDocument> docs = new ArrayList<>();
        for (String version : versions) {
            ParsedDocument doc = mapper.parse(source(b -> b.field("field", version)));
            docs.add(doc);
        }

        // Extract and sort versions
        List<String> sortedVersions = docs.stream().map(doc -> {
            for (IndexableField field : doc.rootDoc().getFields("field")) {
                if (field.fieldType().stored()) {
                    return field.stringValue();
                }
            }
            return null;
        }).filter(Objects::nonNull).sorted((v1, v2) -> {
            SemanticVersion sv1 = SemanticVersion.parse(v1);
            SemanticVersion sv2 = SemanticVersion.parse(v2);
            return sv1.compareTo(sv2);
        }).map(v -> {
            // Normalize version by removing build metadata
            SemanticVersion sv = SemanticVersion.parse(v);
            return new SemanticVersion(sv.getMajor(), sv.getMinor(), sv.getPatch(), sv.getPreRelease(), null).toString();
        }).collect(Collectors.toList());

        // Verify each version individually
        assertEquals("Wrong number of versions", 10, sortedVersions.size());
        assertEquals("1.0.0-alpha", sortedVersions.get(0));
        assertEquals("1.0.0-alpha.1", sortedVersions.get(1));
        assertEquals("1.0.0-beta", sortedVersions.get(2));
        assertEquals("1.0.0-beta.2", sortedVersions.get(3));
        assertEquals("1.0.0-beta.11", sortedVersions.get(4));
        assertEquals("1.0.0-rc.1", sortedVersions.get(5));
        assertEquals("1.0.0", sortedVersions.get(6));
        assertEquals("1.0.0", sortedVersions.get(7)); // build.123 is stripped
        assertEquals("1.2.0", sortedVersions.get(8));
        assertEquals("1.10.0", sortedVersions.get(9));
    }

    public void testDocValues() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1.0.0")));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        boolean hasDocValues = false;
        for (IndexableField field : fields) {
            if (field.fieldType().docValuesType() == DocValuesType.SORTED_SET) {
                hasDocValues = true;
                break;
            }
        }
        assertTrue("Field should have doc values", hasDocValues);
    }

    public void testTermQuery() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1.0.0")));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        boolean hasTermField = false;
        for (IndexableField field : fields) {
            if (field instanceof KeywordField) {
                hasTermField = true;
                break;
            }
        }
        assertTrue("Field should have keyword term field", hasTermField);
    }

    public void testNullValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(b -> b.nullField("field")));
        assertThat(doc.rootDoc().getFields("field"), arrayWithSize(0));
    }

    public void testRangeQueries() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        MappedFieldType fieldType = ((FieldMapper) mapper.mappers().getMapper("field")).fieldType();

        // Test various range scenarios
        Query rangeQuery1 = fieldType.rangeQuery("1.0.0", "2.0.0", true, true, ShapeRelation.INTERSECTS, null, null, null);
        assertTrue(
            rangeQuery1 instanceof TermRangeQuery
                || (rangeQuery1 instanceof IndexOrDocValuesQuery
                    && ((IndexOrDocValuesQuery) rangeQuery1).getIndexQuery() instanceof TermRangeQuery)
        );

        Query rangeQuery2 = fieldType.rangeQuery("1.0.0-alpha", "1.0.0", true, true, ShapeRelation.INTERSECTS, null, null, null);
        assertTrue(
            rangeQuery2 instanceof TermRangeQuery
                || (rangeQuery2 instanceof IndexOrDocValuesQuery
                    && ((IndexOrDocValuesQuery) rangeQuery2).getIndexQuery() instanceof TermRangeQuery)
        );

        // Test null bounds
        Query rangeQuery3 = fieldType.rangeQuery(null, "2.0.0", true, true, ShapeRelation.INTERSECTS, null, null, null);
        assertTrue(
            rangeQuery3 instanceof TermRangeQuery
                || (rangeQuery3 instanceof IndexOrDocValuesQuery
                    && ((IndexOrDocValuesQuery) rangeQuery3).getIndexQuery() instanceof TermRangeQuery)
        );

        Query rangeQuery4 = fieldType.rangeQuery("1.0.0", null, true, true, ShapeRelation.INTERSECTS, null, null, null);
        assertTrue(
            rangeQuery4 instanceof TermRangeQuery
                || (rangeQuery4 instanceof IndexOrDocValuesQuery
                    && ((IndexOrDocValuesQuery) rangeQuery4).getIndexQuery() instanceof TermRangeQuery)
        );

        // Test actual document matching
        ParsedDocument doc1 = mapper.parse(source(b -> b.field("field", "1.5.0")));
        ParsedDocument doc2 = mapper.parse(source(b -> b.field("field", "2.5.0")));

        // Should match doc1 but not doc2
        Query rangeQuery = fieldType.rangeQuery("1.0.0", "2.0.0", true, true, ShapeRelation.INTERSECTS, null, null, null);

        // Create readers and searcher
        Directory dir1 = new ByteBuffersDirectory();
        Directory dir2 = new ByteBuffersDirectory();

        // Index first document
        IndexWriter writer1 = new IndexWriter(dir1, new IndexWriterConfig());
        writer1.addDocument(doc1.rootDoc());
        writer1.close();

        // Index second document
        IndexWriter writer2 = new IndexWriter(dir2, new IndexWriterConfig());
        writer2.addDocument(doc2.rootDoc());
        writer2.close();

        // Create readers
        IndexReader reader1 = DirectoryReader.open(dir1);
        IndexReader reader2 = DirectoryReader.open(dir2);

        // Create MultiReader with array of readers
        IndexReader[] readers = new IndexReader[] { reader1, reader2 };
        MultiReader multiReader = new MultiReader(readers, true);

        IndexSearcher searcher = new IndexSearcher(multiReader);
        TopDocs hits = searcher.search(rangeQuery, 10);

        // Clean up
        multiReader.close();
        dir1.close();
        dir2.close();
    }

    private IndexSearcher setupSearcher(DocumentMapper mapper) throws IOException {
        List<ParsedDocument> docs = Arrays.asList(
            mapper.parse(source(b -> b.field("field", "1.0.0"))),
            mapper.parse(source(b -> b.field("field", "1.0.1"))),
            mapper.parse(source(b -> b.field("field", "1.1.0"))),
            mapper.parse(source(b -> b.field("field", "2.0.0"))),
            mapper.parse(source(b -> b.field("field", "1.0.0-alpha"))),
            mapper.parse(source(b -> b.field("field", "1.0.0-beta"))),
            mapper.parse(source(b -> b.field("field", "1.0.0+build.123")))
        );

        Directory dir = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
        for (ParsedDocument doc : docs) {
            writer.addDocument(doc.rootDoc());
        }
        writer.close();

        return new IndexSearcher(DirectoryReader.open(dir));
    }

    public void testPrefixQuery() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        MappedFieldType fieldType = ((FieldMapper) mapper.mappers().getMapper("field")).fieldType();
        IndexSearcher searcher = setupSearcher(mapper);

        Query prefixQuery = fieldType.prefixQuery(
            "1.0",
            MultiTermQuery.CONSTANT_SCORE_REWRITE,  // Specify rewrite method
            false,
            null
        );

        assertThat("Should match 1.0.0, 1.0.1, 1.0.0-alpha, 1.0.0-beta, 1.0.0+build.123", searcher.count(prefixQuery), equalTo(5));

        // Test different prefix patterns
        Query majorVersionPrefix = fieldType.prefixQuery("1.", MultiTermQuery.CONSTANT_SCORE_REWRITE, false, null);
        assertThat("Should match all version 1.x.x", searcher.count(majorVersionPrefix), equalTo(6));

        // Test case sensitivity
        Query caseInsensitivePrefix = fieldType.prefixQuery("1.0", MultiTermQuery.CONSTANT_SCORE_REWRITE, true, null);
        assertThat("Should match same as case sensitive", searcher.count(caseInsensitivePrefix), equalTo(5));
    }

    public void testWildcardQuery() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        MappedFieldType fieldType = ((FieldMapper) mapper.mappers().getMapper("field")).fieldType();

        // Create test documents with specific versions
        List<ParsedDocument> docs = Arrays.asList(
            mapper.parse(source(b -> b.field("field", "1.0.0"))),
            mapper.parse(source(b -> b.field("field", "1.1.0"))),
            mapper.parse(source(b -> b.field("field", "2.1.0")))
        );

        Directory dir = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
        for (ParsedDocument doc : docs) {
            writer.addDocument(doc.rootDoc());
        }
        writer.close();

        IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(dir));

        Query wildcardQuery = fieldType.wildcardQuery("1.*.0", MultiTermQuery.CONSTANT_SCORE_REWRITE, false, null);
        assertThat("Should match 1.0.0 and 1.1.0", searcher.count(wildcardQuery), equalTo(2));

        dir.close();
    }

    public void testRegexpQuery() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        MappedFieldType fieldType = ((FieldMapper) mapper.mappers().getMapper("field")).fieldType();
        IndexSearcher searcher = setupSearcher(mapper);

        Query regexpQuery = fieldType.regexpQuery(
            "1\\.0\\.0-.*",
            RegExp.ALL,
            0,
            Operations.DEFAULT_DETERMINIZE_WORK_LIMIT,
            MultiTermQuery.CONSTANT_SCORE_REWRITE,
            null
        );
        assertThat("Should match 1.0.0-alpha and 1.0.0-beta", searcher.count(regexpQuery), equalTo(2));
    }

    public void testFuzzyQuery() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        MappedFieldType fieldType = ((FieldMapper) mapper.mappers().getMapper("field")).fieldType();

        // Create simple test documents
        List<ParsedDocument> docs = Arrays.asList(
            mapper.parse(source(b -> b.field("field", "1.0.0"))),
            mapper.parse(source(b -> b.field("field", "1.0.1")))
        );

        Directory dir = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
        for (ParsedDocument doc : docs) {
            writer.addDocument(doc.rootDoc());
        }
        writer.close();

        IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(dir));

        // Test fuzzy query
        Query fuzzyQuery = fieldType.fuzzyQuery(
            "1.0.0",
            Fuzziness.ONE,
            0,  // No prefix requirement
            50,
            true,
            MultiTermQuery.CONSTANT_SCORE_REWRITE,
            null
        );

        assertThat(
            "Fuzzy match should find similar versions",
            searcher.count(fuzzyQuery),
            equalTo(2)  // Should match 1.0.0 and 1.0.1
        );

        dir.close();
    }

    public void testComplexQuery() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        MappedFieldType fieldType = ((FieldMapper) mapper.mappers().getMapper("field")).fieldType();
        IndexSearcher searcher = setupSearcher(mapper);

        Query complexQuery = new BooleanQuery.Builder().add(
            fieldType.prefixQuery("1.", MultiTermQuery.CONSTANT_SCORE_REWRITE, false, null),
            BooleanClause.Occur.MUST
        )
            .add(
                fieldType.regexpQuery(
                    ".*-.*",
                    RegExp.ALL,
                    0,
                    Operations.DEFAULT_DETERMINIZE_WORK_LIMIT,
                    MultiTermQuery.CONSTANT_SCORE_REWRITE,
                    null
                ),
                BooleanClause.Occur.MUST_NOT
            )
            .build();

        assertThat("Should match 1.0.0, 1.0.1, 1.1.0, 1.0.0+build.123", searcher.count(complexQuery), equalTo(4));
    }

    /**
     * Test to cover error cases in SemanticVersionFieldType
     */
    public void testFieldTypeErrorCases() {
        // Create a mock QueryShardContext
        QueryShardContext mockContext = mock(QueryShardContext.class);

        // Create a field type with various configurations to test error paths
        SemanticVersionFieldMapper.SemanticVersionFieldType fieldType = new SemanticVersionFieldMapper.SemanticVersionFieldType(
            "test_field",
            new HashMap<>(),
            true,  // isSearchable
            true,  // hasDocValues
            false  // isStored
        );

        // Test termQuery with null value (should throw exception)
        IllegalArgumentException nullValueException = expectThrows(
            IllegalArgumentException.class,
            () -> fieldType.termQuery(null, mockContext)
        );
        assertEquals("Cannot search for null value", nullValueException.getMessage());

        // Create a field type that is not searchable but has doc values
        SemanticVersionFieldMapper.SemanticVersionFieldType docValuesOnlyFieldType =
            new SemanticVersionFieldMapper.SemanticVersionFieldType(
                "docvalues_only",
                new HashMap<>(),
                false,  // isSearchable
                true,   // hasDocValues
                false   // isStored
            );

        // Test termQuery - should use doc values query only
        Query docValuesQuery = docValuesOnlyFieldType.termQuery("1.0.0", mockContext);
        assertNotNull(docValuesQuery);

        // Create a field type that is searchable but has no doc values
        SemanticVersionFieldMapper.SemanticVersionFieldType searchOnlyFieldType = new SemanticVersionFieldMapper.SemanticVersionFieldType(
            "search_only",
            new HashMap<>(),
            true,   // isSearchable
            false,  // hasDocValues
            false   // isStored
        );

        // Test termQuery - should use index query only
        Query indexQuery = searchOnlyFieldType.termQuery("1.0.0", mockContext);
        assertNotNull(indexQuery);

        // Create a field type that is neither searchable nor has doc values
        SemanticVersionFieldMapper.SemanticVersionFieldType invalidFieldType = new SemanticVersionFieldMapper.SemanticVersionFieldType(
            "invalid_field",
            new HashMap<>(),
            false,  // isSearchable
            false,  // hasDocValues
            false   // isStored
        );

        // Test termQuery - should throw exception
        IllegalArgumentException invalidFieldException = expectThrows(
            IllegalArgumentException.class,
            () -> invalidFieldType.termQuery("1.0.0", mockContext)
        );
        assertThat(invalidFieldException.getMessage(), containsString("is neither indexed nor has doc_values enabled"));

        // Test rangeQuery with invalid version format - should throw QueryShardException
        QueryShardException rangeException = expectThrows(
            QueryShardException.class,
            () -> fieldType.rangeQuery("invalid-version", "2.0.0", true, true, null, null, null, mockContext)
        );
        assertThat(rangeException.getMessage(), containsString("Failed to create range query for field"));

        // Test termsQuery with different field configurations
        List<String> terms = Arrays.asList("1.0.0", "2.0.0", "3.0.0");

        // Test with searchable field
        Query termsQuery = searchOnlyFieldType.termsQuery(terms, mockContext);
        assertNotNull(termsQuery);

        // Test with doc values only field
        Query docValuesTermsQuery = docValuesOnlyFieldType.termsQuery(terms, mockContext);
        assertNotNull(docValuesTermsQuery);

        // Test with invalid field
        IllegalArgumentException termsException = expectThrows(
            IllegalArgumentException.class,
            () -> invalidFieldType.termsQuery(terms, mockContext)
        );
        assertThat(termsException.getMessage(), containsString("is neither indexed nor has doc_values enabled"));

        // Test regexpQuery with non-searchable field
        IllegalArgumentException regexpException = expectThrows(
            IllegalArgumentException.class,
            () -> docValuesOnlyFieldType.regexpQuery("1\\.0\\..*", 0, 0, 10, MultiTermQuery.CONSTANT_SCORE_REWRITE, mockContext)
        );
        assertEquals("Regexp queries require the field to be indexed", regexpException.getMessage());

        // Test wildcardQuery with case insensitivity
        Query wildcardQuery = fieldType.wildcardQuery(
            "1.0.*",
            MultiTermQuery.CONSTANT_SCORE_REWRITE,
            true,  // case insensitive
            mockContext
        );
        assertNotNull(wildcardQuery);

        // Test wildcardQuery with non-searchable field
        IllegalArgumentException wildcardException = expectThrows(
            IllegalArgumentException.class,
            () -> docValuesOnlyFieldType.wildcardQuery("1.0.*", MultiTermQuery.CONSTANT_SCORE_REWRITE, false, mockContext)
        );
        assertEquals("Wildcard queries require the field to be indexed", wildcardException.getMessage());

        // Test prefixQuery with non-searchable field
        IllegalArgumentException prefixException = expectThrows(
            IllegalArgumentException.class,
            () -> docValuesOnlyFieldType.prefixQuery("1.0", MultiTermQuery.CONSTANT_SCORE_REWRITE, false, mockContext)
        );
        assertEquals("Prefix queries require the field to be indexed", prefixException.getMessage());

        // Test fuzzyQuery with null rewrite method (should use default)
        Query fuzzyQuery = fieldType.fuzzyQuery(
            "1.0.0",
            Fuzziness.ONE,
            0,
            50,
            true,
            null,  // null rewrite method
            mockContext
        );
        assertNotNull(fuzzyQuery);

        // Test fuzzyQuery with non-searchable field
        IllegalArgumentException fuzzyException = expectThrows(
            IllegalArgumentException.class,
            () -> docValuesOnlyFieldType.fuzzyQuery("1.0.0", Fuzziness.ONE, 0, 50, true, MultiTermQuery.CONSTANT_SCORE_REWRITE, mockContext)
        );
        assertEquals("Fuzzy queries require the field to be indexed", fuzzyException.getMessage());

        // Test valueFetcher with format parameter
        SearchLookup mockLookup = mock(SearchLookup.class);
        IllegalArgumentException formatException = expectThrows(
            IllegalArgumentException.class,
            () -> fieldType.valueFetcher(mockContext, mockLookup, "some_format")
        );
        assertThat(formatException.getMessage(), containsString("doesn't support formats"));

        // Test valueFetcher without format parameter
        assertNotNull(fieldType.valueFetcher(mockContext, mockLookup, null));

        // Test fielddataBuilder with doc_values disabled
        IllegalArgumentException fieldDataException = expectThrows(
            IllegalArgumentException.class,
            () -> searchOnlyFieldType.fielddataBuilder("test_index", null)
        );
        assertThat(fieldDataException.getMessage(), containsString("does not have doc_values enabled"));
    }
}
