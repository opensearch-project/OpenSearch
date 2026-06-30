/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search;

import org.opensearch.index.query.InnerHitBuilder;
import org.opensearch.protobufs.FieldAndFormat;
import org.opensearch.protobufs.InlineScript;
import org.opensearch.protobufs.InnerHits;
import org.opensearch.protobufs.ScriptField;
import org.opensearch.protobufs.ScriptLanguage;
import org.opensearch.protobufs.SourceConfig;
import org.opensearch.protobufs.SourceFilter;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.grpc.proto.request.search.query.QueryBuilderProtoConverterRegistryImpl;
import org.opensearch.transport.grpc.spi.QueryBuilderProtoConverterRegistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class InnerHitsBuilderProtoUtilsTests extends OpenSearchTestCase {

    private QueryBuilderProtoConverterRegistry registry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        registry = new QueryBuilderProtoConverterRegistryImpl();
    }

    public void testFromProtoWithBasicFields() throws IOException {
        // Create a protobuf InnerHits with basic fields
        InnerHits innerHits = InnerHits.newBuilder()
            .setName("test_inner_hits")
            .setIgnoreUnmapped(true)
            .setFrom(10)
            .setSize(20)
            .setExplain(true)
            .setVersion(true)
            .setSeqNoPrimaryTerm(true)
            .setTrackScores(true)
            .build();

        // Call the method under test
        InnerHitBuilder innerHitBuilder = InnerHitsBuilderProtoUtils.fromProto(innerHits, registry);

        // Verify the result
        assertNotNull("InnerHitBuilder should not be null", innerHitBuilder);
        assertEquals("Name should match", "test_inner_hits", innerHitBuilder.getName());
        assertTrue("IgnoreUnmapped should be true", innerHitBuilder.isIgnoreUnmapped());
        assertEquals("From should match", 10, innerHitBuilder.getFrom());
        assertEquals("Size should match", 20, innerHitBuilder.getSize());
        assertTrue("Explain should be true", innerHitBuilder.isExplain());
        assertTrue("Version should be true", innerHitBuilder.isVersion());
        assertTrue("SeqNoAndPrimaryTerm should be true", innerHitBuilder.isSeqNoAndPrimaryTerm());
        assertTrue("TrackScores should be true", innerHitBuilder.isTrackScores());
    }

    public void testFromProtoWithStoredFields() throws IOException {
        // Create a protobuf InnerHits with stored fields
        InnerHits innerHits = InnerHits.newBuilder()
            .setName("test_inner_hits")
            .addStoredFields("field1")
            .addStoredFields("field2")
            .addStoredFields("field3")
            .build();

        // Call the method under test
        InnerHitBuilder innerHitBuilder = InnerHitsBuilderProtoUtils.fromProto(innerHits, registry);

        // Verify the result
        assertNotNull("InnerHitBuilder should not be null", innerHitBuilder);
        assertNotNull("StoredFieldNames should not be null", innerHitBuilder.getStoredFieldsContext());
        assertEquals("StoredFieldNames size should match", 3, innerHitBuilder.getStoredFieldsContext().fieldNames().size());
        assertTrue("StoredFieldNames should contain field1", innerHitBuilder.getStoredFieldsContext().fieldNames().contains("field1"));
        assertTrue("StoredFieldNames should contain field2", innerHitBuilder.getStoredFieldsContext().fieldNames().contains("field2"));
        assertTrue("StoredFieldNames should contain field3", innerHitBuilder.getStoredFieldsContext().fieldNames().contains("field3"));
    }

    public void testFromProtoWithDocValueFields() throws IOException {
        // Create a protobuf InnerHits with doc value fields
        InnerHits innerHits = InnerHits.newBuilder()
            .setName("test_inner_hits")
            .addDocvalueFields(FieldAndFormat.newBuilder().setField("field1").setFormat("format1").build())
            .addDocvalueFields(FieldAndFormat.newBuilder().setField("field2").setFormat("format2").build())
            .build();

        // Call the method under test
        InnerHitBuilder innerHitBuilder = InnerHitsBuilderProtoUtils.fromProto(innerHits, registry);

        // Verify the result
        assertNotNull("InnerHitBuilder should not be null", innerHitBuilder);
        assertNotNull("DocValueFields should not be null", innerHitBuilder.getDocValueFields());
        assertEquals("DocValueFields size should match", 2, innerHitBuilder.getDocValueFields().size());

        // Check field names and formats
        boolean foundField1 = false;
        boolean foundField2 = false;
        for (org.opensearch.search.fetch.subphase.FieldAndFormat fieldAndFormat : innerHitBuilder.getDocValueFields()) {
            if (fieldAndFormat.field.equals("field1")) {
                assertEquals("Format should match for field1", "format1", fieldAndFormat.format);
                foundField1 = true;
            } else if (fieldAndFormat.field.equals("field2")) {
                assertEquals("Format should match for field2", "format2", fieldAndFormat.format);
                foundField2 = true;
            }
        }
        assertTrue("Should find field1", foundField1);
        assertTrue("Should find field2", foundField2);
    }

    public void testFromProtoWithFetchFields() throws IOException {
        // Create a protobuf InnerHits with fetch fields
        InnerHits innerHits = InnerHits.newBuilder()
            .setName("test_inner_hits")
            .addFields(org.opensearch.protobufs.FieldAndFormat.newBuilder().setField("field1").build())
            .addFields(org.opensearch.protobufs.FieldAndFormat.newBuilder().setField("field2").build())
            .build();

        // Call the method under test
        InnerHitBuilder innerHitBuilder = InnerHitsBuilderProtoUtils.fromProto(innerHits, registry);

        // Verify the result
        assertNotNull("InnerHitBuilder should not be null", innerHitBuilder);
        assertNotNull("FetchFields should not be null", innerHitBuilder.getFetchFields());
        assertEquals("FetchFields size should match", 2, innerHitBuilder.getFetchFields().size());

        // Check field names (formats will be null for string-based fields)
        boolean foundField1 = false;
        boolean foundField2 = false;
        for (org.opensearch.search.fetch.subphase.FieldAndFormat fieldAndFormat : innerHitBuilder.getFetchFields()) {
            if (fieldAndFormat.field.equals("field1")) {
                assertNull("Format should be null for field1", fieldAndFormat.format);
                foundField1 = true;
            } else if (fieldAndFormat.field.equals("field2")) {
                assertNull("Format should be null for field2", fieldAndFormat.format);
                foundField2 = true;
            }
        }
        assertTrue("Should find field1", foundField1);
        assertTrue("Should find field2", foundField2);
    }

    public void testFromProtoWithScriptFields() throws IOException {
        // Create a protobuf InnerHits with script fields
        InnerHits.Builder innerHitsBuilder = InnerHits.newBuilder().setName("test_inner_hits");

        // Create script field 1
        InlineScript inlineScript1 = InlineScript.newBuilder()
            .setSource("doc['field1'].value * 2")
            .setLang(
                ScriptLanguage.newBuilder()
                    .setBuiltin(org.opensearch.protobufs.BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_PAINLESS)
                    .build()
            )
            .build();
        org.opensearch.protobufs.Script script1 = org.opensearch.protobufs.Script.newBuilder().setInline(inlineScript1).build();
        ScriptField scriptField1 = ScriptField.newBuilder().setScript(script1).setIgnoreFailure(true).build();
        innerHitsBuilder.putScriptFields("script_field1", scriptField1);

        // Create script field 2
        InlineScript inlineScript2 = InlineScript.newBuilder()
            .setSource("doc['field2'].value + '_suffix'")
            .setLang(
                ScriptLanguage.newBuilder()
                    .setBuiltin(org.opensearch.protobufs.BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_PAINLESS)
                    .build()
            )
            .build();
        org.opensearch.protobufs.Script script2 = org.opensearch.protobufs.Script.newBuilder().setInline(inlineScript2).build();
        ScriptField scriptField2 = ScriptField.newBuilder().setScript(script2).build();
        innerHitsBuilder.putScriptFields("script_field2", scriptField2);

        InnerHits innerHits = innerHitsBuilder.build();

        // Call the method under test
        InnerHitBuilder innerHitBuilder = InnerHitsBuilderProtoUtils.fromProto(innerHits, registry);

        // Verify the result
        assertNotNull("InnerHitBuilder should not be null", innerHitBuilder);
        Set<SearchSourceBuilder.ScriptField> scriptFields = innerHitBuilder.getScriptFields();
        assertNotNull("ScriptFields should not be null", scriptFields);
        assertEquals("ScriptFields size should match", 2, scriptFields.size());

        // Check script fields
        boolean foundScriptField1 = false;
        boolean foundScriptField2 = false;
        for (SearchSourceBuilder.ScriptField scriptField : scriptFields) {
            if (scriptField.fieldName().equals("script_field1")) {
                assertTrue("IgnoreFailure should be true for script_field1", scriptField.ignoreFailure());
                foundScriptField1 = true;
            } else if (scriptField.fieldName().equals("script_field2")) {
                assertFalse("IgnoreFailure should be false for script_field2", scriptField.ignoreFailure());
                foundScriptField2 = true;
            }
        }
        assertTrue("Should find script_field1", foundScriptField1);
        assertTrue("Should find script_field2", foundScriptField2);
    }

    public void testFromProtoWithSource() throws IOException {
        // Create a protobuf InnerHits with source context
        SourceConfig sourceContext = SourceConfig.newBuilder()
            .setFilter(SourceFilter.newBuilder().addIncludes("include1").addIncludes("include2").addExcludes("exclude1").build())
            .build();

        InnerHits innerHits = InnerHits.newBuilder().setName("test_inner_hits").setXSource(sourceContext).build();

        // Call the method under test
        InnerHitBuilder innerHitBuilder = InnerHitsBuilderProtoUtils.fromProto(innerHits, registry);

        // Verify the result
        assertNotNull("InnerHitBuilder should not be null", innerHitBuilder);
        org.opensearch.search.fetch.subphase.FetchSourceContext fetchSourceContext = innerHitBuilder.getFetchSourceContext();
        assertNotNull("FetchSourceContext should not be null", fetchSourceContext);
        assertTrue("FetchSource should be true", fetchSourceContext.fetchSource());
        assertArrayEquals("Includes should match", new String[] { "include1", "include2" }, fetchSourceContext.includes());
        assertArrayEquals("Excludes should match", new String[] { "exclude1" }, fetchSourceContext.excludes());
    }

    public void testFromProtoWithMultipleInnerHits() throws IOException {
        // Create multiple protobuf InnerHits
        InnerHits innerHits1 = InnerHits.newBuilder().setName("inner_hits1").setSize(10).build();

        InnerHits innerHits2 = InnerHits.newBuilder().setName("inner_hits2").setSize(20).build();

        List<InnerHits> innerHitsList = Arrays.asList(innerHits1, innerHits2);

        List<InnerHitBuilder> innerHitBuilders = new ArrayList<>();
        for (InnerHits innerHits : innerHitsList) {
            innerHitBuilders.add(InnerHitsBuilderProtoUtils.fromProto(innerHits, registry));
        }

        // Verify the result
        assertNotNull("InnerHitBuilder list should not be null", innerHitBuilders);
        assertEquals("Should have 2 InnerHitBuilders", 2, innerHitBuilders.size());

        // Check first InnerHitBuilder
        InnerHitBuilder innerHitBuilder1 = innerHitBuilders.get(0);
        assertEquals("First name should match", "inner_hits1", innerHitBuilder1.getName());
        assertEquals("First size should match", 10, innerHitBuilder1.getSize());

        // Check second InnerHitBuilder
        InnerHitBuilder innerHitBuilder2 = innerHitBuilders.get(1);
        assertEquals("Second name should match", "inner_hits2", innerHitBuilder2.getName());
        assertEquals("Second size should match", 20, innerHitBuilder2.getSize());
    }

    public void testFromProtoWithEmptyList() throws IOException {
        List<InnerHits> emptyList = Arrays.asList();
        List<InnerHitBuilder> innerHitBuilders = new ArrayList<>();
        for (InnerHits innerHits : emptyList) {
            innerHitBuilders.add(InnerHitsBuilderProtoUtils.fromProto(innerHits, registry));
        }

        // Verify the result
        assertNotNull("InnerHitBuilder list should not be null", innerHitBuilders);
        assertEquals("Should have 0 InnerHitBuilders", 0, innerHitBuilders.size());
    }

    public void testFromProtoWithNullInnerHits() {
        // Test null input validation for single InnerHits
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> InnerHitsBuilderProtoUtils.fromProto((InnerHits) null, registry)
        );

        assertEquals("InnerHits cannot be null", exception.getMessage());
    }

    public void testFromProtoWithSort() throws IOException {
        InnerHits innerHits = InnerHits.newBuilder()
            .setName("test_inner_hits")
            .addSort(org.opensearch.protobufs.SortCombinations.newBuilder().build())
            .build();

        InnerHitBuilder innerHitBuilder = InnerHitsBuilderProtoUtils.fromProto(innerHits, registry);

        assertNotNull("InnerHitBuilder should not be null", innerHitBuilder);
        assertNotNull("Sorts should not be null", innerHitBuilder.getSorts());
        assertEquals("Name should match", "test_inner_hits", innerHitBuilder.getName());
    }

    public void testFromProtoWithHighlight() throws IOException {
        org.opensearch.protobufs.Highlight highlightProto = org.opensearch.protobufs.Highlight.newBuilder().build();

        InnerHits innerHits = InnerHits.newBuilder().setName("test_inner_hits").setHighlight(highlightProto).build();

        InnerHitBuilder innerHitBuilder = InnerHitsBuilderProtoUtils.fromProto(innerHits, registry);

        assertNotNull("InnerHitBuilder should not be null", innerHitBuilder);
        assertNotNull("HighlightBuilder should not be null", innerHitBuilder.getHighlightBuilder());
        assertEquals("Name should match", "test_inner_hits", innerHitBuilder.getName());
    }

    public void testFromProtoWithCollapse() throws IOException {
        // Create a protobuf InnerHits with collapse
        org.opensearch.protobufs.FieldCollapse collapseProto = org.opensearch.protobufs.FieldCollapse.newBuilder()
            .setField("category")
            .build();

        InnerHits innerHits = InnerHits.newBuilder().setName("test_inner_hits").setCollapse(collapseProto).build();

        // This should work and create the InnerHitBuilder with collapse
        InnerHitBuilder innerHitBuilder = InnerHitsBuilderProtoUtils.fromProto(innerHits, registry);

        // Verify the result
        assertNotNull("InnerHitBuilder should not be null", innerHitBuilder);
        assertNotNull("InnerCollapseBuilder should not be null", innerHitBuilder.getInnerCollapseBuilder());
    }
}
