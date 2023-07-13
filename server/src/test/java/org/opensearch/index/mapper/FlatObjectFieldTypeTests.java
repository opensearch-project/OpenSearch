/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.analysis.AnalyzerScope;
import org.opensearch.index.analysis.NamedAnalyzer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class FlatObjectFieldTypeTests extends FieldTypeTestCase {
    private static MappedFieldType getFlatParentFieldType(String fieldName) {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id).build();
        Mapper.BuilderContext context = new Mapper.BuilderContext(settings, new ContentPath());
        MappedFieldType flatParentFieldType = new FlatObjectFieldMapper.Builder(fieldName).build(context).fieldType();
        return flatParentFieldType;
    }

    public void testFetchSourceValue() throws IOException {
        MappedFieldType mapper = getFlatParentFieldType("field");

        Map<String, Object> jsonPoint = new HashMap<>();
        jsonPoint.put("type", "flat_object");
        jsonPoint.put("coordinates", Arrays.asList(42.0, 27.1));
        Map<String, Object> otherJsonPoint = new HashMap<>();
        otherJsonPoint.put("type", "Point");
        otherJsonPoint.put("coordinates", Arrays.asList(30.0, 50.0));

        ArrayList<String> jsonPointList = new ArrayList<>();
        jsonPointList.add(jsonPoint.toString());

        ArrayList<String> otherJsonPointList = new ArrayList<>();
        otherJsonPointList.add(otherJsonPoint.toString());

        assertEquals(jsonPointList, fetchSourceValue(mapper, jsonPoint, null));
        assertEquals(otherJsonPointList, fetchSourceValue(mapper, otherJsonPoint, null));

    }

    public void testDirectSubfield() {
        {
            MappedFieldType flatParentFieldType = getFlatParentFieldType("field");

            // when searching for "foo" in "field", the directSubfield is field._value field
            String searchFieldName = ((FlatObjectFieldMapper.FlatObjectFieldType) flatParentFieldType).directSubfield();
            assertEquals("field._value", searchFieldName);

            MappedFieldType dynamicMappedFieldType = new FlatObjectFieldMapper.FlatObjectFieldType("bar", flatParentFieldType.name());
            // when searching for "foo" in "field.bar", the directSubfield is field._valueAndPath field
            String searchFieldNameDocPath = ((FlatObjectFieldMapper.FlatObjectFieldType) dynamicMappedFieldType).directSubfield();
            assertEquals("field._valueAndPath", searchFieldNameDocPath);
        }
        {
            NamedAnalyzer analyzer = new NamedAnalyzer("default", AnalyzerScope.INDEX, null);
            MappedFieldType ft = new FlatObjectFieldMapper.FlatObjectFieldType("field", analyzer);
            assertEquals("field._value", ((FlatObjectFieldMapper.FlatObjectFieldType) ft).directSubfield());
        }
    }

    public void testRewriteValue() {
        MappedFieldType flatParentFieldType = getFlatParentFieldType("field");

        // when searching for "foo" in "field", the rewrite value is "foo"
        String searchValues = ((FlatObjectFieldMapper.FlatObjectFieldType) flatParentFieldType).rewriteValue("foo");
        assertEquals("foo", searchValues);

        MappedFieldType dynamicMappedFieldType = new FlatObjectFieldMapper.FlatObjectFieldType("field.bar", flatParentFieldType.name());

        // when searching for "foo" in "field.bar", the rewrite value is "field.bar=foo"
        String searchFieldNameDocPath = ((FlatObjectFieldMapper.FlatObjectFieldType) dynamicMappedFieldType).directSubfield();
        String searchValuesDocPath = ((FlatObjectFieldMapper.FlatObjectFieldType) dynamicMappedFieldType).rewriteValue("foo");
        assertEquals("field.bar=foo", searchValuesDocPath);
    }

    public void testTermQuery() {

        MappedFieldType flatParentFieldType = getFlatParentFieldType("field");

        // when searching for "foo" in "field", the term query is directed to search "foo" in field._value field
        String searchFieldName = ((FlatObjectFieldMapper.FlatObjectFieldType) flatParentFieldType).directSubfield();
        String searchValues = ((FlatObjectFieldMapper.FlatObjectFieldType) flatParentFieldType).rewriteValue("foo");
        assertEquals("foo", searchValues);
        assertEquals(new TermQuery(new Term(searchFieldName, searchValues)), flatParentFieldType.termQuery(searchValues, null));

        MappedFieldType dynamicMappedFieldType = new FlatObjectFieldMapper.FlatObjectFieldType("field.bar", flatParentFieldType.name());

        // when searching for "foo" in "field.bar", the term query is directed to search in field._valueAndPath field
        String searchFieldNameDocPath = ((FlatObjectFieldMapper.FlatObjectFieldType) dynamicMappedFieldType).directSubfield();
        String searchValuesDocPath = ((FlatObjectFieldMapper.FlatObjectFieldType) dynamicMappedFieldType).rewriteValue("foo");
        assertEquals("field.bar=foo", searchValuesDocPath);
        assertEquals(new TermQuery(new Term(searchFieldNameDocPath, searchValuesDocPath)), dynamicMappedFieldType.termQuery("foo", null));

        MappedFieldType unsearchable = new FlatObjectFieldMapper.FlatObjectFieldType("field", false, true, Collections.emptyMap());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> unsearchable.termQuery("bar", null));
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());
    }

    public void testExistsQuery() {
        {
            MappedFieldType ft = getFlatParentFieldType("field");
            // when checking on the flat_object field name "field", check if exist in the field mapper names
            assertEquals(new TermQuery(new Term(FieldNamesFieldMapper.NAME, "field")), ft.existsQuery(null));

            // when checking if a subfield within the flat_object, for example, "field.bar", use term query in the flat_object field
            MappedFieldType dynamicMappedFieldType = new FlatObjectFieldMapper.FlatObjectFieldType("field.bar", ft.name());
            assertEquals(new TermQuery(new Term("field", "field.bar")), dynamicMappedFieldType.existsQuery(null));

        }
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = new FlatObjectFieldMapper.FlatObjectFieldType(
                "field",
                true,
                false,
                Collections.emptyMap()
            );
            assertEquals(new TermQuery(new Term(FieldNamesFieldMapper.NAME, "field")), ft.existsQuery(null));
        }
    }
}
