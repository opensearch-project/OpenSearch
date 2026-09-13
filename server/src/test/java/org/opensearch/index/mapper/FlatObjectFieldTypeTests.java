/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.opensearch.common.lucene.search.AutomatonQueries;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.index.analysis.AnalyzerScope;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.mapper.FlatObjectFieldMapper.FlatObjectFieldType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.index.mapper.FlatObjectFieldMapper.VALUE_AND_PATH_SUFFIX;
import static org.opensearch.index.mapper.FlatObjectFieldMapper.VALUE_SUFFIX;
import static org.apache.lucene.search.MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE;
import static org.apache.lucene.search.MultiTermQuery.CONSTANT_SCORE_REWRITE;
import static org.apache.lucene.search.MultiTermQuery.DOC_VALUES_REWRITE;

public class FlatObjectFieldTypeTests extends FieldTypeTestCase {

    private static MappedFieldType getFlatParentFieldType(
        String fieldName,
        String mappedFieldTypeName,
        boolean isSearchable,
        boolean hasDocValues
    ) {
        FlatObjectFieldMapper.Builder builder = new FlatObjectFieldMapper.Builder(fieldName);
        FlatObjectFieldMapper.FlatObjectFieldType flatObjectFieldType = new FlatObjectFieldMapper.FlatObjectFieldType(
            fieldName,
            mappedFieldTypeName,
            isSearchable,
            hasDocValues
        );
        FieldType fieldtype = new FieldType(FlatObjectFieldMapper.Defaults.FIELD_TYPE);
        FieldType vft = new FieldType(fieldtype);
        if (flatObjectFieldType.isSearchable() == false) {
            vft.setIndexOptions(IndexOptions.NONE);
        }
        return flatObjectFieldType;
    }

    public void testFetchSourceValue() throws IOException {
        MappedFieldType mapper = getFlatParentFieldType("field", null, true, true);

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

    public void testGetSearchField() {
        {
            FlatObjectFieldMapper.FlatObjectFieldType flatParentFieldType =
                (FlatObjectFieldMapper.FlatObjectFieldType) (getFlatParentFieldType("field", null, true, true));

            // when searching for "foo" in "field", the directSubfield is field._value field
            String searchFieldName = (flatParentFieldType).getSearchField();
            assertEquals("field._value", searchFieldName);

            MappedFieldType dynamicMappedFieldType = new FlatObjectFieldMapper.FlatObjectFieldType(
                flatParentFieldType.name() + ".bar",
                flatParentFieldType.name(),
                flatParentFieldType.getValueFieldType(),
                flatParentFieldType.getValueAndPathFieldType()
            );
            // when searching for "foo" in "field.bar", the directSubfield is field._valueAndPath field
            String searchFieldNameDocPath = ((FlatObjectFieldMapper.FlatObjectFieldType) dynamicMappedFieldType).getSearchField();
            assertEquals("field._valueAndPath", searchFieldNameDocPath);
        }
        {
            NamedAnalyzer analyzer = new NamedAnalyzer("default", AnalyzerScope.INDEX, null);
            MappedFieldType ft = new FlatObjectFieldMapper.FlatObjectFieldType("field", null, true, true);
            assertEquals("field._value", ((FlatObjectFieldMapper.FlatObjectFieldType) ft).getSearchField());
        }
    }

    public void testRewriteValue() {
        FlatObjectFieldMapper.FlatObjectFieldType flatParentFieldType = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
            "field",
            null,
            true,
            true
        );

        // when searching for "foo" in "field", the rewrite value is "foo"
        String searchValues = (flatParentFieldType).rewriteSearchValue("foo");
        assertEquals("foo", searchValues);

        MappedFieldType dynamicMappedFieldType = new FlatObjectFieldMapper.FlatObjectFieldType(
            "field.bar",
            flatParentFieldType.name(),
            flatParentFieldType.getValueFieldType(),
            flatParentFieldType.getValueAndPathFieldType()
        );

        // when searching for "foo" in "field.bar", the rewrite value is "field.bar=foo"
        String searchFieldNameDocPath = ((FlatObjectFieldMapper.FlatObjectFieldType) dynamicMappedFieldType).getSearchField();
        String searchValuesDocPath = ((FlatObjectFieldMapper.FlatObjectFieldType) dynamicMappedFieldType).rewriteSearchValue("foo");
        assertEquals("field.bar=foo", searchValuesDocPath);
    }

    public void testTermQueryCaseInsensitive() {

        // 1.test isSearchable=true, hasDocValues=true, mappedFieldTypeName=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType flatParentFieldType =
                (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType("field", null, true, true);

            MappedFieldType dynamicMappedFieldType = new FlatObjectFieldMapper.FlatObjectFieldType(
                "field.bar",
                flatParentFieldType.name(),
                flatParentFieldType.getValueFieldType(),
                flatParentFieldType.getValueAndPathFieldType()
            );
            assertEquals(
                AutomatonQueries.caseInsensitiveTermQuery(new Term("field._valueAndPath", "field.bar=fOo")),
                dynamicMappedFieldType.termQueryCaseInsensitive("fOo", null)
            );
        }

        // 2.test isSearchable=true, hasDocValues=false, mappedFieldTypeName=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field",
                null,
                true,
                false
            );
            assertEquals(
                AutomatonQueries.caseInsensitiveTermQuery(new Term("field._value", "fOo")),
                ft.termQueryCaseInsensitive("fOo", null)
            );
        }

        // test isSearchable=true, hasDocValues=false, mappedFieldTypeName!=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field.field1",
                "field",
                true,
                false
            );
            Query expected = new ConstantScoreQuery(
                new TermQuery(new Term("field" + VALUE_AND_PATH_SUFFIX, new BytesRef("field.field1=fOo")))
            );

            assertEquals(expected, ft.termQuery("fOo", MOCK_QSC_ENABLE_INDEX_DOC_VALUES));
        }

        // 3.test isSearchable=false, hasDocValues=true, mappedFieldTypeName=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field",
                null,
                false,
                true
            );
            Query expected = AutomatonQueries.createAutomatonQuery(
                new Term("field" + VALUE_SUFFIX, "field.fOo"),
                AutomatonQueries.toCaseInsensitiveString("field.fOo"),
                MultiTermQuery.DOC_VALUES_REWRITE
            );
            assertEquals(expected, ft.termQueryCaseInsensitive("fOo", MOCK_QSC_ENABLE_INDEX_DOC_VALUES));
        }

        // test isSearchable=false, hasDocValues=true, mappedFieldTypeName!=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field.field1",
                "field",
                false,
                true
            );
            Query expected = AutomatonQueries.createAutomatonQuery(
                new Term("field" + VALUE_AND_PATH_SUFFIX, "field.field.field1=fOo"),
                AutomatonQueries.toCaseInsensitiveString("field.field.field1=fOo"),
                MultiTermQuery.DOC_VALUES_REWRITE
            );

            assertEquals(expected, ft.termQueryCaseInsensitive("fOo", MOCK_QSC_ENABLE_INDEX_DOC_VALUES));
        }

        // 4.test isSearchable=false, hasDocValues=false, mappedFieldTypeName=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field",
                null,
                false,
                false
            );
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> ft.termQueryCaseInsensitive("foo", MOCK_QSC_ENABLE_INDEX_DOC_VALUES)
            );
            assertEquals(
                "Cannot search on field [field] since it is both not indexed, and does not have doc_values " + "enabled.",
                e.getMessage()
            );
        }

        // test isSearchable=false, hasDocValues=false, mappedFieldTypeName!=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field.field1",
                "field",
                false,
                false
            );
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> ft.termQuery("foo", MOCK_QSC_ENABLE_INDEX_DOC_VALUES)
            );
            assertEquals(
                "Cannot search on field [field.field1] since it is both not indexed, and does not have doc_values " + "enabled.",
                e.getMessage()
            );
        }

        MappedFieldType unsearchable = new FlatObjectFieldMapper.FlatObjectFieldType("field", null, false, false);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> unsearchable.termQuery("bar", MOCK_QSC_ENABLE_INDEX_DOC_VALUES)
        );
        assertEquals("Cannot search on field [field] since it is both not indexed, and does not have doc_values enabled.", e.getMessage());
    }

    public void testTermQuery() {

        // 1.test isSearchable=true, hasDocValues=true, mappedFieldTypeName=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType flatParentFieldType =
                (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType("field", null, true, true);

            // when searching for "foo" in "field", the term query is directed to search "foo" in field._value field
            String searchFieldName = flatParentFieldType.getSearchField();
            String searchValues = flatParentFieldType.rewriteSearchValue("foo");
            assertEquals("foo", searchValues);
            assertEquals(
                new ConstantScoreQuery(new TermQuery(new Term(searchFieldName, searchValues))),
                flatParentFieldType.termQuery(searchValues, null)
            );

            FlatObjectFieldType dynamicMappedFieldType = new FlatObjectFieldMapper.FlatObjectFieldType(
                "field.bar",
                flatParentFieldType.name(),
                flatParentFieldType.getValueFieldType(),
                flatParentFieldType.getValueAndPathFieldType()
            );

            // when searching for "foo" in "field.bar", the term query is directed to search in field._valueAndPath field
            String searchFieldNameDocPath = dynamicMappedFieldType.getSearchField();
            String searchValuesDocPath = dynamicMappedFieldType.rewriteSearchValue("foo");
            assertEquals("field.bar=foo", searchValuesDocPath);
            assertEquals(
                new ConstantScoreQuery(new TermQuery(new Term(searchFieldNameDocPath, searchValuesDocPath))),
                dynamicMappedFieldType.termQuery("foo", null)
            );
        }

        // 2.test isSearchable=true, hasDocValues=false, mappedFieldTypeName=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field",
                null,
                true,
                false
            );
            Query expected = new ConstantScoreQuery(new TermQuery(new Term("field" + VALUE_SUFFIX, new BytesRef("foo"))));
            assertEquals(expected, ft.termQuery("foo", MOCK_QSC_ENABLE_INDEX_DOC_VALUES));
        }

        // test isSearchable=true, hasDocValues=false, mappedFieldTypeName!=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field.field1",
                "field",
                true,
                false
            );
            Query expected = new ConstantScoreQuery(
                new TermQuery(new Term("field" + VALUE_AND_PATH_SUFFIX, new BytesRef("field.field1=foo")))
            );

            assertEquals(expected, ft.termQuery("foo", MOCK_QSC_ENABLE_INDEX_DOC_VALUES));
        }

        // 3.test isSearchable=false, hasDocValues=true, mappedFieldTypeName=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field",
                null,
                false,
                true
            );
            Query expected = SortedSetDocValuesField.newSlowRangeQuery(
                "field" + VALUE_SUFFIX,
                new BytesRef("field.foo"),
                new BytesRef("field.foo"),
                true,
                true
            );
            assertEquals(expected, ft.termQuery("foo", MOCK_QSC_ENABLE_INDEX_DOC_VALUES));

        }

        // test isSearchable=false, hasDocValues=true, mappedFieldTypeName!=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field.field1",
                "field",
                false,
                true
            );
            Query expected = SortedSetDocValuesField.newSlowRangeQuery(
                "field" + VALUE_AND_PATH_SUFFIX,
                new BytesRef("field.field.field1=foo"),
                new BytesRef("field.field.field1=foo"),
                true,
                true
            );
            assertEquals(expected, ft.termQuery("foo", MOCK_QSC_ENABLE_INDEX_DOC_VALUES));
        }

        // 4.test isSearchable=false, hasDocValues=false, mappedFieldTypeName=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field",
                null,
                false,
                false
            );
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> ft.termQuery("foo", MOCK_QSC_ENABLE_INDEX_DOC_VALUES)
            );
            assertEquals(
                "Cannot search on field [field] since it is both not indexed, and does not have doc_values " + "enabled.",
                e.getMessage()
            );
        }

        // test isSearchable=false, hasDocValues=false, mappedFieldTypeName!=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field.field1",
                "field",
                false,
                false
            );
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> ft.termQuery("foo", MOCK_QSC_ENABLE_INDEX_DOC_VALUES)
            );
            assertEquals(
                "Cannot search on field [field.field1] since it is both not indexed, and does not have doc_values " + "enabled.",
                e.getMessage()
            );
        }

        MappedFieldType unsearchable = new FlatObjectFieldMapper.FlatObjectFieldType("field", null, false, false);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> unsearchable.termQuery("bar", MOCK_QSC_ENABLE_INDEX_DOC_VALUES)
        );
        assertEquals("Cannot search on field [field] since it is both not indexed, and does not have doc_values enabled.", e.getMessage());
    }

    public void testExistsQuery() {
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field",
                null,
                true,
                true
            );
            // when checking on the flat_object field name "field", check if exist in the field mapper names
            assertEquals(new FieldExistsQuery("field"), ft.existsQuery(null));

            // when checking if a subfield within the flat_object, for example, "field.bar", use term query in the flat_object field
            MappedFieldType dynamicMappedFieldType = new FlatObjectFieldMapper.FlatObjectFieldType(
                "field.bar",
                ft.name(),
                ft.getValueFieldType(),
                ft.getValueAndPathFieldType()
            );
            Automaton termAutomaton = PrefixQuery.toAutomaton(new BytesRef("field.bar="));
            Automaton dvAutomaton = PrefixQuery.toAutomaton(new BytesRef("field.field.bar="));
            Query indexQuery = new AutomatonQuery(new Term("field" + VALUE_AND_PATH_SUFFIX), termAutomaton, true);
            Query dvQuery = new AutomatonQuery(new Term("field" + VALUE_AND_PATH_SUFFIX), dvAutomaton, true, DOC_VALUES_REWRITE);
            Query expected = new IndexOrDocValuesQuery(indexQuery, dvQuery);
            assertEquals(expected, dynamicMappedFieldType.existsQuery(MOCK_QSC_ENABLE_INDEX_DOC_VALUES));
        }

        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = new FlatObjectFieldMapper.FlatObjectFieldType("field", null, true, false);
            assertEquals(new TermQuery(new Term(FieldNamesFieldMapper.NAME, "field")), ft.existsQuery(MOCK_QSC_ENABLE_INDEX_DOC_VALUES));
        }
    }

    public void testTermsQuery() {
        List<String> values = Arrays.asList("foo", "bar");
        Collections.shuffle(values, random());
        // 1.test isSearchable=true, hasDocValues=true, mappedFieldTypeName=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field",
                null,
                true,
                true
            );
            List<BytesRef> indexTerms = new ArrayList<>();
            indexTerms.add(new BytesRef("foo"));
            indexTerms.add(new BytesRef("bar"));
            List<BytesRef> docValueterms = new ArrayList<>();
            docValueterms.add(new BytesRef("field.foo"));
            docValueterms.add(new BytesRef("field.bar"));
            Query expected = new IndexOrDocValuesQuery(
                new TermInSetQuery("field" + VALUE_SUFFIX, indexTerms),
                new TermInSetQuery(DOC_VALUES_REWRITE, "field" + VALUE_SUFFIX, docValueterms)
            );

            assertEquals(expected, ft.termsQuery(values, MOCK_QSC_ENABLE_INDEX_DOC_VALUES));
        }

        // test isSearchable=true, hasDocValues=true, mappedFieldTypeName!=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field.field1",
                "field",
                true,
                true
            );

            List<BytesRef> indexTerms = new ArrayList<>();
            indexTerms.add(new BytesRef("field.field1=foo"));
            indexTerms.add(new BytesRef("field.field1=bar"));
            List<BytesRef> docValueterms = new ArrayList<>();
            docValueterms.add(new BytesRef("field.field.field1=foo"));
            docValueterms.add(new BytesRef("field.field.field1=bar"));
            Query expected = new IndexOrDocValuesQuery(
                new TermInSetQuery("field" + VALUE_AND_PATH_SUFFIX, indexTerms),
                new TermInSetQuery(DOC_VALUES_REWRITE, "field" + VALUE_AND_PATH_SUFFIX, docValueterms)
            );

            assertEquals(expected, ft.termsQuery(values, MOCK_QSC_ENABLE_INDEX_DOC_VALUES));
        }

        // 2.test isSearchable=true, hasDocValues=false, mappedFieldTypeName=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field",
                null,
                true,
                false
            );
            List<BytesRef> indexTerms = new ArrayList<>();
            indexTerms.add(new BytesRef("foo"));
            indexTerms.add(new BytesRef("bar"));
            Query expected = new TermInSetQuery("field" + VALUE_SUFFIX, indexTerms);

            assertEquals(expected, ft.termsQuery(values, MOCK_QSC_ENABLE_INDEX_DOC_VALUES));
        }

        // test isSearchable=true, hasDocValues=false, mappedFieldTypeName!=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field.field1",
                "field",
                true,
                false
            );
            List<BytesRef> indexTerms = new ArrayList<>();
            indexTerms.add(new BytesRef("field.field1=foo"));
            indexTerms.add(new BytesRef("field.field1=bar"));
            Query expected = new TermInSetQuery("field" + VALUE_AND_PATH_SUFFIX, indexTerms);

            assertEquals(expected, ft.termsQuery(values, null));
        }

        // 3.test isSearchable=false, hasDocValues=true, mappedFieldTypeName=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field",
                null,
                false,
                true
            );

            List<BytesRef> indexTerms = new ArrayList<>();
            indexTerms.add(new BytesRef("foo"));
            indexTerms.add(new BytesRef("bar"));
            List<BytesRef> docValueterms = new ArrayList<>();
            docValueterms.add(new BytesRef("field.foo"));
            docValueterms.add(new BytesRef("field.bar"));
            Query expected = new TermInSetQuery(DOC_VALUES_REWRITE, "field" + VALUE_SUFFIX, docValueterms);

            assertEquals(expected, ft.termsQuery(values, MOCK_QSC_ENABLE_INDEX_DOC_VALUES));
        }

        // test isSearchable=false, hasDocValues=true, mappedFieldTypeName!=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field.field1",
                "field",
                false,
                true
            );

            List<BytesRef> docValueTerms = new ArrayList<>();
            docValueTerms.add(new BytesRef("field.field.field1=foo"));
            docValueTerms.add(new BytesRef("field.field.field1=bar"));
            Query expected = new TermInSetQuery(DOC_VALUES_REWRITE, "field" + VALUE_AND_PATH_SUFFIX, docValueTerms);

            assertEquals(expected, ft.termsQuery(values, MOCK_QSC_ENABLE_INDEX_DOC_VALUES));
        }

        // 4.test isSearchable=false, hasDocValues=false, mappedFieldTypeName=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field",
                null,
                false,
                false
            );
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> ft.termsQuery(values, MOCK_QSC_ENABLE_INDEX_DOC_VALUES)
            );
            assertEquals(
                "Cannot search on field [field] since it is both not indexed, and does not have doc_values " + "enabled.",
                e.getMessage()
            );
        }

        // test isSearchable=false, hasDocValues=false, mappedFieldTypeName!=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field.field1",
                "field",
                false,
                false
            );
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> ft.termsQuery(values, MOCK_QSC_ENABLE_INDEX_DOC_VALUES)
            );
            assertEquals(
                "Cannot search on field [field.field1] since it is both not indexed, and does not have doc_values " + "enabled.",
                e.getMessage()
            );
        }
    }

    public void testPrefixQuery() {
        // 1.test isSearchable=true, hasDocValues=true, mappedFieldTypeName=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field",
                null,
                true,
                true
            );
            Query expected = new IndexOrDocValuesQuery(
                new PrefixQuery(new Term("field" + VALUE_SUFFIX, "foo"), CONSTANT_SCORE_REWRITE),
                new PrefixQuery(new Term("field" + VALUE_SUFFIX, "field.foo"), DOC_VALUES_REWRITE)
            );
            assertEquals(expected, ft.prefixQuery("foo", CONSTANT_SCORE_REWRITE, false, MOCK_QSC_ENABLE_INDEX_DOC_VALUES));
        }

        // test isSearchable=true, hasDocValues=true, mappedFieldTypeName!=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field.field1",
                "field",
                true,
                true
            );
            Query expected = new IndexOrDocValuesQuery(
                new PrefixQuery(new Term("field" + VALUE_AND_PATH_SUFFIX, "field.field1=foo"), CONSTANT_SCORE_REWRITE),
                new PrefixQuery(new Term("field" + VALUE_AND_PATH_SUFFIX, "field.field.field1=foo"), DOC_VALUES_REWRITE)
            );
            assertEquals(expected, ft.prefixQuery("foo", CONSTANT_SCORE_REWRITE, false, MOCK_QSC_ENABLE_INDEX_DOC_VALUES));
        }

        // 2.test isSearchable=true, hasDocValues=false, mappedFieldTypeName=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field",
                null,
                true,
                false
            );
            Query expected = new PrefixQuery(new Term("field" + VALUE_SUFFIX, "foo"), CONSTANT_SCORE_BLENDED_REWRITE);
            assertEquals(expected, ft.prefixQuery("foo", CONSTANT_SCORE_BLENDED_REWRITE, false, MOCK_QSC_ENABLE_INDEX_DOC_VALUES));
        }

        // test isSearchable=true, hasDocValues=false, mappedFieldTypeName!=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field.field1",
                "field",
                true,
                false
            );
            Query expected = new PrefixQuery(new Term("field" + VALUE_AND_PATH_SUFFIX, "field.field1=foo"), CONSTANT_SCORE_REWRITE);
            assertEquals(expected, ft.prefixQuery("foo", CONSTANT_SCORE_REWRITE, false, MOCK_QSC_ENABLE_INDEX_DOC_VALUES));
        }

        // 3.test isSearchable=false, hasDocValues=true, mappedFieldTypeName=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field",
                null,
                false,
                true
            );
            Query expected = new PrefixQuery(new Term("field" + VALUE_SUFFIX, "field.foo"), DOC_VALUES_REWRITE);
            assertEquals(expected, ft.prefixQuery("foo", CONSTANT_SCORE_BLENDED_REWRITE, false, MOCK_QSC_ENABLE_INDEX_DOC_VALUES));
        }

        // test isSearchable=false, hasDocValues=true, mappedFieldTypeName!=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field.field1",
                "field",
                false,
                true
            );
            Query expected = new PrefixQuery(new Term("field" + VALUE_AND_PATH_SUFFIX, "field.field.field1=foo"), DOC_VALUES_REWRITE);
            assertEquals(expected, ft.prefixQuery("foo", CONSTANT_SCORE_REWRITE, false, MOCK_QSC_ENABLE_INDEX_DOC_VALUES));
        }

        // 4.test isSearchable=false, hasDocValues=false, mappedFieldTypeName=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field",
                null,
                false,
                false
            );
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> ft.prefixQuery("foo", CONSTANT_SCORE_REWRITE, false, MOCK_QSC_ENABLE_INDEX_DOC_VALUES)
            );
            assertEquals(
                "Cannot search on field [field] since it is both not indexed, and does not have doc_values " + "enabled.",
                e.getMessage()
            );
        }

        // test isSearchable=false, hasDocValues=false, mappedFieldTypeName!=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field.field1",
                "field",
                false,
                false
            );
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> ft.prefixQuery("foo", CONSTANT_SCORE_REWRITE, false, MOCK_QSC_ENABLE_INDEX_DOC_VALUES)
            );
            assertEquals(
                "Cannot search on field [field.field1] since it is both not indexed, and does not have doc_values " + "enabled.",
                e.getMessage()
            );
        }
    }

    public void testRegexpQuery() {
        // 1.test isSearchable=true, hasDocValues=true, mappedFieldTypeName=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field",
                null,
                true,
                true
            );
            Query expected = new IndexOrDocValuesQuery(
                new RegexpQuery(
                    new Term("field" + VALUE_SUFFIX, new BytesRef("foo")),
                    0,
                    0,
                    RegexpQuery.DEFAULT_PROVIDER,
                    10,
                    CONSTANT_SCORE_BLENDED_REWRITE
                ),
                new RegexpQuery(
                    new Term("field" + VALUE_SUFFIX, new BytesRef("field.foo")),
                    0,
                    0,
                    RegexpQuery.DEFAULT_PROVIDER,
                    10,
                    DOC_VALUES_REWRITE
                )
            );
            assertEquals(expected, ft.regexpQuery("foo", 0, 0, 10, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES));
        }

        // test isSearchable=true, hasDocValues=true, mappedFieldTypeName!=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field.field1",
                "field",
                true,
                true
            );
            Query expected = new IndexOrDocValuesQuery(
                new RegexpQuery(
                    new Term("field" + VALUE_AND_PATH_SUFFIX, new BytesRef("field.field1=foo")),
                    0,
                    0,
                    RegexpQuery.DEFAULT_PROVIDER,
                    10,
                    CONSTANT_SCORE_BLENDED_REWRITE
                ),
                new RegexpQuery(
                    new Term("field" + VALUE_AND_PATH_SUFFIX, new BytesRef("field.field.field1=foo")),
                    0,
                    0,
                    RegexpQuery.DEFAULT_PROVIDER,
                    10,
                    DOC_VALUES_REWRITE
                )
            );
            assertEquals(expected, ft.regexpQuery("foo", 0, 0, 10, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES));
        }

        // 2.test isSearchable=true, hasDocValues=false, mappedFieldTypeName=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field",
                null,
                true,
                false
            );
            Query expected = new RegexpQuery(
                new Term("field" + VALUE_SUFFIX, new BytesRef("foo")),
                0,
                0,
                RegexpQuery.DEFAULT_PROVIDER,
                10,
                CONSTANT_SCORE_BLENDED_REWRITE
            );
            assertEquals(expected, ft.regexpQuery("foo", 0, 0, 10, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES));
        }

        // test isSearchable=true, hasDocValues=false, mappedFieldTypeName!=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field.field1",
                "field",
                true,
                false
            );
            Query expected = new RegexpQuery(
                new Term("field" + VALUE_AND_PATH_SUFFIX, new BytesRef("field.field1=foo")),
                0,
                0,
                RegexpQuery.DEFAULT_PROVIDER,
                10,
                CONSTANT_SCORE_BLENDED_REWRITE
            );
            assertEquals(expected, ft.regexpQuery("foo", 0, 0, 10, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES));
        }

        // 3.test isSearchable=false, hasDocValues=true, mappedFieldTypeName=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field",
                null,
                false,
                true
            );
            Query expected = new RegexpQuery(
                new Term("field" + VALUE_SUFFIX, new BytesRef("field.foo")),
                0,
                0,
                RegexpQuery.DEFAULT_PROVIDER,
                10,
                DOC_VALUES_REWRITE
            );
            assertEquals(expected, ft.regexpQuery("foo", 0, 0, 10, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES));
        }

        // test isSearchable=false, hasDocValues=true, mappedFieldTypeName!=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field.field1",
                "field",
                false,
                true
            );
            Query expected = new RegexpQuery(
                new Term("field" + VALUE_AND_PATH_SUFFIX, new BytesRef("field.field.field1=foo")),
                0,
                0,
                RegexpQuery.DEFAULT_PROVIDER,
                10,
                DOC_VALUES_REWRITE
            );
            assertEquals(expected, ft.regexpQuery("foo", 0, 0, 10, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES));
        }

        // 4.test isSearchable=false, hasDocValues=false, mappedFieldTypeName=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field",
                null,
                false,
                false
            );
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> ft.regexpQuery("foo", 0, 0, 10, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES)
            );
            assertEquals(
                "Cannot search on field [field] since it is both not indexed, and does not have doc_values " + "enabled.",
                e.getMessage()
            );
        }

        // test isSearchable=false, hasDocValues=false, mappedFieldTypeName!=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field.field1",
                "field",
                false,
                false
            );
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> ft.regexpQuery("foo", 0, 0, 10, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES)
            );
            assertEquals(
                "Cannot search on field [field.field1] since it is both not indexed, and does not have doc_values " + "enabled.",
                e.getMessage()
            );
        }
    }

    public void testFuzzyQuery() {
        // 1.test isSearchable=true, hasDocValues=true, mappedFieldTypeName=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field",
                null,
                true,
                true
            );
            Query expected = new IndexOrDocValuesQuery(
                new FuzzyQuery(new Term("field" + VALUE_SUFFIX, "foo"), 2, 1, 50, true),
                new FuzzyQuery(new Term("field" + VALUE_SUFFIX, "field.foo"), 2, 1, 50, true, MultiTermQuery.DOC_VALUES_REWRITE)
            );
            assertEquals(expected, ft.fuzzyQuery("foo", Fuzziness.fromEdits(2), 1, 50, true, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES));
        }

        // test isSearchable=true, hasDocValues=true, mappedFieldTypeName!=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field.field1",
                "field",
                true,
                true
            );
            Query expected = new IndexOrDocValuesQuery(
                new FuzzyQuery(new Term("field" + VALUE_AND_PATH_SUFFIX, "field.field1=foo"), 2, 1, 50, true),
                new FuzzyQuery(
                    new Term("field" + VALUE_AND_PATH_SUFFIX, "field.field.field1=foo"),
                    2,
                    1,
                    50,
                    true,
                    MultiTermQuery.DOC_VALUES_REWRITE
                )
            );
            assertEquals(expected, ft.fuzzyQuery("foo", Fuzziness.fromEdits(2), 1, 50, true, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES));
        }

        // 2.test isSearchable=true, hasDocValues=false, mappedFieldTypeName=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field",
                null,
                true,
                false
            );
            Query expected = new FuzzyQuery(new Term("field" + VALUE_SUFFIX, "foo"), 2, 1, 50, true);
            assertEquals(expected, ft.fuzzyQuery("foo", Fuzziness.fromEdits(2), 1, 50, true, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES));
        }

        // test isSearchable=true, hasDocValues=false, mappedFieldTypeName!=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field.field1",
                "field",
                true,
                false
            );
            Query expected = new FuzzyQuery(new Term("field" + VALUE_AND_PATH_SUFFIX, "field.field1=foo"), 2, 1, 50, true);
            assertEquals(expected, ft.fuzzyQuery("foo", Fuzziness.fromEdits(2), 1, 50, true, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES));
        }

        // 3.test isSearchable=false, hasDocValues=true, mappedFieldTypeName=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field",
                null,
                false,
                true
            );
            Query expected = new FuzzyQuery(
                new Term("field" + VALUE_SUFFIX, "field.foo"),
                2,
                1,
                50,
                true,
                MultiTermQuery.DOC_VALUES_REWRITE
            );
            assertEquals(expected, ft.fuzzyQuery("foo", Fuzziness.fromEdits(2), 1, 50, true, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES));
        }

        // test isSearchable=false, hasDocValues=true, mappedFieldTypeName!=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field.field1",
                "field",
                false,
                true
            );
            Query expected = new FuzzyQuery(
                new Term("field" + VALUE_AND_PATH_SUFFIX, "field.field.field1=foo"),
                2,
                1,
                50,
                true,
                MultiTermQuery.DOC_VALUES_REWRITE
            );
            assertEquals(expected, ft.fuzzyQuery("foo", Fuzziness.fromEdits(2), 1, 50, true, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES));
        }

        // 4.test isSearchable=false, hasDocValues=false, mappedFieldTypeName=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field",
                null,
                false,
                false
            );
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> ft.fuzzyQuery("foo", Fuzziness.fromEdits(2), 1, 50, true, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES)
            );
            assertEquals(
                "Cannot search on field [field] since it is both not indexed, and does not have doc_values " + "enabled.",
                e.getMessage()
            );
        }

        // test isSearchable=false, hasDocValues=false, mappedFieldTypeName!=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field.field1",
                "field",
                false,
                false
            );
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> ft.fuzzyQuery("foo", Fuzziness.fromEdits(2), 1, 50, true, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES)
            );
            assertEquals(
                "Cannot search on field [field.field1] since it is both not indexed, and does not have doc_values " + "enabled.",
                e.getMessage()
            );
        }
    }

    public void testRangeQuery() {
        {
            for (boolean searchable : new boolean[] { true, false }) {
                for (boolean hasDocValue : new boolean[] { true, false }) {
                    FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                        "field",
                        null,
                        searchable,
                        hasDocValue
                    );

                    if (searchable == false && hasDocValue == false) {
                        IllegalArgumentException e = expectThrows(
                            IllegalArgumentException.class,
                            () -> ft.rangeQuery(new BytesRef("2"), new BytesRef("10"), true, true, MOCK_QSC_ENABLE_INDEX_DOC_VALUES)
                        );
                        assertEquals(
                            "Cannot search on field [field] since it is both not indexed, and does not have doc_values " + "enabled.",
                            e.getMessage()
                        );
                        continue;
                    }

                    Query indexQuery = new TermRangeQuery("field" + VALUE_SUFFIX, new BytesRef("2"), new BytesRef("10"), true, true);
                    Query dvQuery = new TermRangeQuery(
                        "field" + VALUE_SUFFIX,
                        new BytesRef("field.2"),
                        new BytesRef("field.10"),
                        true,
                        true,
                        DOC_VALUES_REWRITE
                    );
                    Query expected = searchable == false
                        ? dvQuery
                        : (hasDocValue ? new IndexOrDocValuesQuery(indexQuery, dvQuery) : indexQuery);
                    assertEquals(
                        expected,
                        ft.rangeQuery(new BytesRef("2"), new BytesRef("10"), true, true, MOCK_QSC_ENABLE_INDEX_DOC_VALUES)
                    );
                }
            }
        }

        {
            for (boolean searchable : new boolean[] { true, false }) {
                for (boolean hasDocValue : new boolean[] { true, false }) {
                    FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                        "field.field1",
                        "field",
                        searchable,
                        hasDocValue
                    );

                    if (searchable == false && hasDocValue == false) {
                        IllegalArgumentException e = expectThrows(
                            IllegalArgumentException.class,
                            () -> ft.rangeQuery(new BytesRef("2"), new BytesRef("10"), true, true, MOCK_QSC_ENABLE_INDEX_DOC_VALUES)
                        );
                        assertEquals(
                            "Cannot search on field [field.field1] since it is both not indexed, and does not have doc_values "
                                + "enabled.",
                            e.getMessage()
                        );
                        continue;
                    }

                    Query indexQuery = new TermRangeQuery(
                        "field" + VALUE_AND_PATH_SUFFIX,
                        new BytesRef("field.field1=2"),
                        new BytesRef("field.field1=10"),
                        true,
                        true
                    );
                    Query dvQuery = new TermRangeQuery(
                        "field" + VALUE_AND_PATH_SUFFIX,
                        new BytesRef("field.field.field1=2"),
                        new BytesRef("field.field.field1=10"),
                        true,
                        true,
                        DOC_VALUES_REWRITE
                    );
                    Query expected = searchable == false
                        ? dvQuery
                        : (hasDocValue ? new IndexOrDocValuesQuery(indexQuery, dvQuery) : indexQuery);
                    assertEquals(
                        expected,
                        ft.rangeQuery(new BytesRef("2"), new BytesRef("10"), true, true, MOCK_QSC_ENABLE_INDEX_DOC_VALUES)
                    );
                }
            }
        }

        {
            for (boolean searchable : new boolean[] { true, false }) {
                for (boolean hasDocValue : new boolean[] { true, false }) {
                    FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                        "field",
                        null,
                        searchable,
                        hasDocValue
                    );

                    if (searchable == false && hasDocValue == false) {
                        IllegalArgumentException e = expectThrows(
                            IllegalArgumentException.class,
                            () -> ft.rangeQuery(new BytesRef("2"), new BytesRef("10"), true, true, MOCK_QSC_ENABLE_INDEX_DOC_VALUES)
                        );
                        assertEquals(
                            "Cannot search on field [field] since it is both not indexed, and does not have doc_values " + "enabled.",
                            e.getMessage()
                        );
                        continue;
                    }
                    boolean nullLowerTerm = randomBoolean();
                    boolean nullUpperTerm = nullLowerTerm == false || randomBoolean();

                    Automaton a1 = PrefixQuery.toAutomaton(new BytesRef("field."));
                    Automaton a2 = TermRangeQuery.toAutomaton(
                        nullLowerTerm ? null : new BytesRef("field.2"),
                        nullUpperTerm ? null : new BytesRef("field.10"),
                        true,
                        true
                    );
                    Automaton dvAutomaton = Operations.intersection(a1, a2);
                    Query indexQuery = new TermRangeQuery(
                        "field" + VALUE_SUFFIX,
                        nullLowerTerm ? null : new BytesRef("2"),
                        nullUpperTerm ? null : new BytesRef("10"),
                        true,
                        true
                    );
                    Query dvQuery = new AutomatonQuery(new Term("field" + VALUE_SUFFIX), dvAutomaton, true, DOC_VALUES_REWRITE);
                    Query expected = searchable == false
                        ? dvQuery
                        : (hasDocValue ? new IndexOrDocValuesQuery(indexQuery, dvQuery) : indexQuery);
                    assertEquals(
                        expected,
                        ft.rangeQuery(
                            nullLowerTerm ? null : new BytesRef("2"),
                            nullUpperTerm ? null : new BytesRef("10"),
                            true,
                            true,
                            MOCK_QSC_ENABLE_INDEX_DOC_VALUES
                        )
                    );
                }
            }
        }

        {
            for (boolean searchable : new boolean[] { true, false }) {
                for (boolean hasDocValue : new boolean[] { true, false }) {
                    FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                        "field.field1",
                        "field",
                        searchable,
                        hasDocValue
                    );
                    if (searchable == false && hasDocValue == false) {
                        IllegalArgumentException e = expectThrows(
                            IllegalArgumentException.class,
                            () -> ft.rangeQuery(new BytesRef("2"), new BytesRef("10"), true, true, MOCK_QSC_ENABLE_INDEX_DOC_VALUES)
                        );
                        assertEquals(
                            "Cannot search on field [field.field1] since it is both not indexed, and does not have doc_values "
                                + "enabled.",
                            e.getMessage()
                        );
                        continue;
                    }
                    boolean nullLowerTerm = randomBoolean();
                    boolean nullUpperTerm = nullLowerTerm == false || randomBoolean();

                    Automaton a1 = PrefixQuery.toAutomaton(new BytesRef("field.field1="));
                    Automaton a2 = TermRangeQuery.toAutomaton(
                        nullLowerTerm ? null : new BytesRef("field.field1=2"),
                        nullUpperTerm ? null : new BytesRef("field.field1=10"),
                        true,
                        true
                    );
                    Automaton termAutomaton = Operations.intersection(a1, a2);

                    Automaton dvA1 = PrefixQuery.toAutomaton(new BytesRef("field.field.field1="));
                    Automaton dvA2 = TermRangeQuery.toAutomaton(
                        nullLowerTerm ? null : new BytesRef("field.field.field1=2"),
                        nullUpperTerm ? null : new BytesRef("field.field.field1=10"),
                        true,
                        true
                    );
                    Automaton dvAutomaton = Operations.intersection(dvA1, dvA2);
                    Query indexQuery = new AutomatonQuery(new Term("field" + VALUE_AND_PATH_SUFFIX), termAutomaton, true);
                    Query dvQuery = new AutomatonQuery(new Term("field" + VALUE_AND_PATH_SUFFIX), dvAutomaton, true, DOC_VALUES_REWRITE);
                    Query expected = searchable == false
                        ? dvQuery
                        : (hasDocValue ? new IndexOrDocValuesQuery(indexQuery, dvQuery) : indexQuery);
                    assertEquals(
                        expected,
                        ft.rangeQuery(
                            nullLowerTerm ? null : new BytesRef("2"),
                            nullUpperTerm ? null : new BytesRef("10"),
                            true,
                            true,
                            MOCK_QSC_ENABLE_INDEX_DOC_VALUES
                        )
                    );
                }
            }
        }
    }

    public void testWildcardQuery() {
        // 1.test isSearchable=true, hasDocValues=true, mappedFieldTypeName=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field",
                null,
                true,
                true
            );
            Query expected = new IndexOrDocValuesQuery(
                new WildcardQuery(
                    new Term("field" + VALUE_SUFFIX, "foo*"),
                    Operations.DEFAULT_DETERMINIZE_WORK_LIMIT,
                    MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE
                ),
                new WildcardQuery(
                    new Term("field" + VALUE_SUFFIX, "field.foo*"),
                    Operations.DEFAULT_DETERMINIZE_WORK_LIMIT,
                    MultiTermQuery.DOC_VALUES_REWRITE
                )
            );
            assertEquals(expected, ft.wildcardQuery("foo*", null, false, MOCK_QSC_ENABLE_INDEX_DOC_VALUES));
        }

        // test isSearchable=true, hasDocValues=true, mappedFieldTypeName!=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field.field1",
                "field",
                true,
                true
            );
            Query expected = new IndexOrDocValuesQuery(
                new WildcardQuery(
                    new Term("field" + VALUE_AND_PATH_SUFFIX, "field.field1=foo*"),
                    Operations.DEFAULT_DETERMINIZE_WORK_LIMIT,
                    MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE
                ),
                new WildcardQuery(
                    new Term("field" + VALUE_AND_PATH_SUFFIX, "field.field.field1=foo*"),
                    Operations.DEFAULT_DETERMINIZE_WORK_LIMIT,
                    MultiTermQuery.DOC_VALUES_REWRITE
                )
            );
            assertEquals(expected, ft.wildcardQuery("foo*", null, false, MOCK_QSC_ENABLE_INDEX_DOC_VALUES));
        }

        // 2.test isSearchable=true, hasDocValues=false, mappedFieldTypeName=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field",
                null,
                true,
                false
            );
            Query expected = new WildcardQuery(
                new Term("field" + VALUE_SUFFIX, "foo*"),
                Operations.DEFAULT_DETERMINIZE_WORK_LIMIT,
                MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE
            );
            assertEquals(expected, ft.wildcardQuery("foo*", null, false, MOCK_QSC_ENABLE_INDEX_DOC_VALUES));
        }

        // test isSearchable=true, hasDocValues=false, mappedFieldTypeName!=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field.field1",
                "field",
                true,
                false
            );
            Query expected = new WildcardQuery(
                new Term("field" + VALUE_AND_PATH_SUFFIX, "field.field1=foo*"),
                Operations.DEFAULT_DETERMINIZE_WORK_LIMIT,
                MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE
            );
            assertEquals(expected, ft.wildcardQuery("foo*", null, false, MOCK_QSC_ENABLE_INDEX_DOC_VALUES));
        }

        // 3.test isSearchable=false, hasDocValues=true, mappedFieldTypeName=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field",
                null,
                false,
                true
            );
            Query expected = new WildcardQuery(
                new Term("field" + VALUE_SUFFIX, "field.foo*"),
                Operations.DEFAULT_DETERMINIZE_WORK_LIMIT,
                MultiTermQuery.DOC_VALUES_REWRITE
            );
            assertEquals(expected, ft.wildcardQuery("foo*", null, false, MOCK_QSC_ENABLE_INDEX_DOC_VALUES));
        }

        // test isSearchable=false, hasDocValues=true, mappedFieldTypeName!=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field.field1",
                "field",
                false,
                true
            );
            Query expected = new WildcardQuery(
                new Term("field" + VALUE_AND_PATH_SUFFIX, "field.field.field1=foo*"),
                Operations.DEFAULT_DETERMINIZE_WORK_LIMIT,
                MultiTermQuery.DOC_VALUES_REWRITE
            );
            assertEquals(expected, ft.wildcardQuery("foo*", null, false, MOCK_QSC_ENABLE_INDEX_DOC_VALUES));
        }

        // 4.test isSearchable=false, hasDocValues=false, mappedFieldTypeName=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field",
                null,
                false,
                false
            );
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> ft.wildcardQuery("foo*", null, false, MOCK_QSC_ENABLE_INDEX_DOC_VALUES)
            );
            assertEquals(
                "Cannot search on field [field] since it is both not indexed, and does not have doc_values " + "enabled.",
                e.getMessage()
            );
        }

        // test isSearchable=false, hasDocValues=false, mappedFieldTypeName!=null
        {
            FlatObjectFieldMapper.FlatObjectFieldType ft = (FlatObjectFieldMapper.FlatObjectFieldType) getFlatParentFieldType(
                "field.field1",
                "field",
                false,
                false
            );
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> ft.wildcardQuery("foo*", null, false, MOCK_QSC_ENABLE_INDEX_DOC_VALUES)
            );
            assertEquals(
                "Cannot search on field [field.field1] since it is both not indexed, and does not have doc_values " + "enabled.",
                e.getMessage()
            );
        }
    }
}
