/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.transport.grpc.proto.request.search;

import org.opensearch.index.query.InnerHitBuilder;
import org.opensearch.protobufs.FieldAndFormat;
import org.opensearch.protobufs.InlineScript;
import org.opensearch.protobufs.InnerHits;
import org.opensearch.protobufs.ScriptField;
import org.opensearch.protobufs.ScriptLanguage;
import org.opensearch.protobufs.ScriptLanguage.BuiltinScriptLanguage;
import org.opensearch.protobufs.SourceConfig;
import org.opensearch.protobufs.SourceFilter;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class InnerHitsBuilderProtoUtilsTests extends OpenSearchTestCase {

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
        InnerHitBuilder innerHitBuilder = InnerHitsBuilderProtoUtils.fromProto(Collections.singletonList(innerHits));

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
        InnerHitBuilder innerHitBuilder = InnerHitsBuilderProtoUtils.fromProto(Collections.singletonList(innerHits));

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
        InnerHitBuilder innerHitBuilder = InnerHitsBuilderProtoUtils.fromProto(Collections.singletonList(innerHits));

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
            .addFields(FieldAndFormat.newBuilder().setField("field1").setFormat("format1").build())
            .addFields(FieldAndFormat.newBuilder().setField("field2").setFormat("format2").build())
            .build();

        // Call the method under test
        InnerHitBuilder innerHitBuilder = InnerHitsBuilderProtoUtils.fromProto(Collections.singletonList(innerHits));

        // Verify the result
        assertNotNull("InnerHitBuilder should not be null", innerHitBuilder);
        assertNotNull("FetchFields should not be null", innerHitBuilder.getFetchFields());
        assertEquals("FetchFields size should match", 2, innerHitBuilder.getFetchFields().size());

        // Check field names and formats
        boolean foundField1 = false;
        boolean foundField2 = false;
        for (org.opensearch.search.fetch.subphase.FieldAndFormat fieldAndFormat : innerHitBuilder.getFetchFields()) {
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

    public void testFromProtoWithScriptFields() throws IOException {
        // Create a protobuf InnerHits with script fields
        InnerHits.Builder innerHitsBuilder = InnerHits.newBuilder().setName("test_inner_hits");

        // Create script field 1
        InlineScript inlineScript1 = InlineScript.newBuilder()
            .setSource("doc['field1'].value * 2")
            .setLang(ScriptLanguage.newBuilder().setBuiltinScriptLanguage(BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_PAINLESS).build())
            .build();
        org.opensearch.protobufs.Script script1 = org.opensearch.protobufs.Script.newBuilder().setInlineScript(inlineScript1).build();
        ScriptField scriptField1 = ScriptField.newBuilder().setScript(script1).setIgnoreFailure(true).build();
        innerHitsBuilder.putScriptFields("script_field1", scriptField1);

        // Create script field 2
        InlineScript inlineScript2 = InlineScript.newBuilder()
            .setSource("doc['field2'].value + '_suffix'")
            .setLang(ScriptLanguage.newBuilder().setBuiltinScriptLanguage(BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_PAINLESS).build())
            .build();
        org.opensearch.protobufs.Script script2 = org.opensearch.protobufs.Script.newBuilder().setInlineScript(inlineScript2).build();
        ScriptField scriptField2 = ScriptField.newBuilder().setScript(script2).build();
        innerHitsBuilder.putScriptFields("script_field2", scriptField2);

        InnerHits innerHits = innerHitsBuilder.build();

        // Call the method under test
        InnerHitBuilder innerHitBuilder = InnerHitsBuilderProtoUtils.fromProto(Collections.singletonList(innerHits));

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

        InnerHits innerHits = InnerHits.newBuilder().setName("test_inner_hits").setSource(sourceContext).build();

        // Call the method under test
        InnerHitBuilder innerHitBuilder = InnerHitsBuilderProtoUtils.fromProto(Collections.singletonList(innerHits));

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

        // Call the method under test
        InnerHitBuilder innerHitBuilder = InnerHitsBuilderProtoUtils.fromProto(innerHitsList);

        // Verify the result
        assertNotNull("InnerHitBuilder should not be null", innerHitBuilder);
        // The last inner hits in the list should override previous ones
        assertEquals("Name should match the last inner hits", "inner_hits2", innerHitBuilder.getName());
        assertEquals("Size should match the last inner hits", 20, innerHitBuilder.getSize());
    }

    public void testFromProtoWithEmptyList() throws IOException {
        // Call the method under test with an empty list
        InnerHitBuilder innerHitBuilder = InnerHitsBuilderProtoUtils.fromProto(Collections.emptyList());

        // Verify the result
        assertNotNull("InnerHitBuilder should not be null", innerHitBuilder);
        // Should have default values
        assertNull("Name should be null", innerHitBuilder.getName());
        assertEquals("From should be default", 0, innerHitBuilder.getFrom());
        assertEquals("Size should be default", 3, innerHitBuilder.getSize());
    }
}
