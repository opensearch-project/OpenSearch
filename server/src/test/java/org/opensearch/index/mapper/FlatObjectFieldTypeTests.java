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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class FlatObjectFieldTypeTests extends FieldTypeTestCase {

    public void testFetchSourceValue() throws IOException {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id).build();
        Mapper.BuilderContext context = new Mapper.BuilderContext(settings, new ContentPath());

        MappedFieldType mapper = new FlatObjectFieldMapper.Builder("field").build(context).fieldType();

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

    public void testTermQuery() {

        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id).build();
        Mapper.BuilderContext context = new Mapper.BuilderContext(settings, new ContentPath());
        MappedFieldType flatParentFieldType = new FlatObjectFieldMapper.Builder("field").build(context).fieldType();

        // when searching for "foo", the term query is directed to search in field._value field
        String searchFieldName = ((FlatObjectFieldMapper.FlatObjectFieldType) flatParentFieldType).directSubfield();
        String searchValues = ((FlatObjectFieldMapper.FlatObjectFieldType) flatParentFieldType).rewriteValue("foo");
        assertEquals("field._value", searchFieldName);
        assertEquals("foo", searchValues);
        assertEquals(new TermQuery(new Term(searchFieldName, searchValues)), flatParentFieldType.termQuery(searchValues, null));

        MappedFieldType dynamicMappedFieldType = new FlatObjectFieldMapper.FlatObjectFieldType("bar", flatParentFieldType.name());

        // when searching for "field.bar", the term query is directed to search in field._valueAndPath field
        String searchFieldNameDocPath = ((FlatObjectFieldMapper.FlatObjectFieldType) dynamicMappedFieldType).directSubfield();
        String searchValuesDocPath = ((FlatObjectFieldMapper.FlatObjectFieldType) dynamicMappedFieldType).rewriteValue("field.bar");
        assertEquals("field._valueAndPath", searchFieldNameDocPath);
        assertEquals("field.bar", searchValuesDocPath);
        assertEquals(
            new TermQuery(new Term(searchFieldNameDocPath, searchValuesDocPath)),
            dynamicMappedFieldType.termQuery(searchValuesDocPath, null)
        );

        MappedFieldType unsearchable = new FlatObjectFieldMapper.FlatObjectFieldType("field", false, true, Collections.emptyMap());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> unsearchable.termQuery("bar", null));
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());
    }

    public void testExistsQuery() {
        {
            Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id).build();
            Mapper.BuilderContext context = new Mapper.BuilderContext(settings, new ContentPath());
            MappedFieldType ft = new FlatObjectFieldMapper.Builder("field").build(context).fieldType();
            assertEquals(new TermQuery(new Term(null, "field")), ft.existsQuery(null));
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
