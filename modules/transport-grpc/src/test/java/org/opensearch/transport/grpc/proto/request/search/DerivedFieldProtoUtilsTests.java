/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search;

import org.opensearch.index.mapper.DerivedField;
import org.opensearch.protobufs.BuiltinScriptLanguage;
import org.opensearch.protobufs.InlineScript;
import org.opensearch.protobufs.ObjectMap;
import org.opensearch.protobufs.Script;
import org.opensearch.protobufs.ScriptLanguage;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

public class DerivedFieldProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoWithRequiredFieldsOnly() {
        // Create a minimal DerivedField proto with only required fields
        org.opensearch.protobufs.DerivedField proto = org.opensearch.protobufs.DerivedField.newBuilder()
            .setType("keyword")
            .setScript(Script.newBuilder().setInline(InlineScript.newBuilder().setSource("emit(doc['test'])").build()).build())
            .build();

        // Convert to OpenSearch DerivedField
        DerivedField derivedField = DerivedFieldProtoUtils.fromProto("test_field", proto);

        // Verify required fields
        assertEquals("test_field", derivedField.getName());
        assertEquals("keyword", derivedField.getType());
        assertEquals("emit(doc['test'])", derivedField.getScript().getIdOrCode());

        // Verify optional fields are null/default
        assertNull(derivedField.getProperties());
        assertNull(derivedField.getPrefilterField());
        assertNull(derivedField.getFormat());
        assertFalse(derivedField.getIgnoreMalformed());
    }

    public void testFromProtoWithAllOptionalFields() {
        // Create properties map
        ObjectMap properties = ObjectMap.newBuilder()
            .putFields("sub_field1", ObjectMap.Value.newBuilder().setString("text").build())
            .putFields("sub_field2", ObjectMap.Value.newBuilder().setString("keyword").build())
            .build();

        // Create a DerivedField proto with all optional fields
        org.opensearch.protobufs.DerivedField proto = org.opensearch.protobufs.DerivedField.newBuilder()
            .setType("object")
            .setScript(Script.newBuilder().setInline(InlineScript.newBuilder().setSource("emit(doc['message'])").build()).build())
            .setProperties(properties)
            .setPrefilterField("source_field")
            .setFormat("dd-MM-yyyy")
            .setIgnoreMalformed(true)
            .build();

        // Convert to OpenSearch DerivedField
        DerivedField derivedField = DerivedFieldProtoUtils.fromProto("complex_field", proto);

        // Verify required fields
        assertEquals("complex_field", derivedField.getName());
        assertEquals("object", derivedField.getType());
        assertEquals("emit(doc['message'])", derivedField.getScript().getIdOrCode());

        // Verify optional fields are set
        assertNotNull(derivedField.getProperties());
        Map<String, Object> props = derivedField.getProperties();
        assertEquals("text", props.get("sub_field1"));
        assertEquals("keyword", props.get("sub_field2"));

        assertEquals("source_field", derivedField.getPrefilterField());
        assertEquals("dd-MM-yyyy", derivedField.getFormat());
        assertTrue(derivedField.getIgnoreMalformed());
    }

    public void testFromProtoWithPropertiesOnly() {
        // Create properties map
        ObjectMap properties = ObjectMap.newBuilder()
            .putFields("nested_field", ObjectMap.Value.newBuilder().setString("long").build())
            .build();

        org.opensearch.protobufs.DerivedField proto = org.opensearch.protobufs.DerivedField.newBuilder()
            .setType("object")
            .setScript(Script.newBuilder().setInline(InlineScript.newBuilder().setSource("emit(doc['data'])").build()).build())
            .setProperties(properties)
            .build();

        DerivedField derivedField = DerivedFieldProtoUtils.fromProto("field_with_props", proto);

        // Verify properties are set
        assertNotNull(derivedField.getProperties());
        assertEquals("long", derivedField.getProperties().get("nested_field"));

        // Verify other optional fields are null/default
        assertNull(derivedField.getPrefilterField());
        assertNull(derivedField.getFormat());
        assertFalse(derivedField.getIgnoreMalformed());
    }

    public void testFromProtoWithPrefilterFieldOnly() {
        org.opensearch.protobufs.DerivedField proto = org.opensearch.protobufs.DerivedField.newBuilder()
            .setType("keyword")
            .setScript(Script.newBuilder().setInline(InlineScript.newBuilder().setSource("emit(doc['text'])").build()).build())
            .setPrefilterField("indexed_field")
            .build();

        DerivedField derivedField = DerivedFieldProtoUtils.fromProto("filtered_field", proto);

        // Verify prefilter field is set
        assertEquals("indexed_field", derivedField.getPrefilterField());

        // Verify other optional fields are null/default
        assertNull(derivedField.getProperties());
        assertNull(derivedField.getFormat());
        assertFalse(derivedField.getIgnoreMalformed());
    }

    public void testFromProtoWithFormatOnly() {
        org.opensearch.protobufs.DerivedField proto = org.opensearch.protobufs.DerivedField.newBuilder()
            .setType("date")
            .setScript(Script.newBuilder().setInline(InlineScript.newBuilder().setSource("emit(doc['timestamp'])").build()).build())
            .setFormat("yyyy-MM-dd")
            .build();

        DerivedField derivedField = DerivedFieldProtoUtils.fromProto("date_field", proto);

        // Verify format is set
        assertEquals("yyyy-MM-dd", derivedField.getFormat());

        // Verify other optional fields are null/default
        assertNull(derivedField.getProperties());
        assertNull(derivedField.getPrefilterField());
        assertFalse(derivedField.getIgnoreMalformed());
    }

    public void testFromProtoWithIgnoreMalformedOnly() {
        org.opensearch.protobufs.DerivedField proto = org.opensearch.protobufs.DerivedField.newBuilder()
            .setType("long")
            .setScript(Script.newBuilder().setInline(InlineScript.newBuilder().setSource("emit(doc['count'])").build()).build())
            .setIgnoreMalformed(true)
            .build();

        DerivedField derivedField = DerivedFieldProtoUtils.fromProto("numeric_field", proto);

        // Verify ignoreMalformed is set
        assertTrue(derivedField.getIgnoreMalformed());

        // Verify other optional fields are null/default
        assertNull(derivedField.getProperties());
        assertNull(derivedField.getPrefilterField());
        assertNull(derivedField.getFormat());
    }

    public void testFromProtoWithIgnoreMalformedFalse() {
        org.opensearch.protobufs.DerivedField proto = org.opensearch.protobufs.DerivedField.newBuilder()
            .setType("long")
            .setScript(Script.newBuilder().setInline(InlineScript.newBuilder().setSource("emit(doc['count'])").build()).build())
            .setIgnoreMalformed(false)
            .build();

        DerivedField derivedField = DerivedFieldProtoUtils.fromProto("strict_field", proto);

        // Verify ignoreMalformed is false (explicitly set)
        assertFalse(derivedField.getIgnoreMalformed());
    }

    public void testFromProtoWithScriptLanguage() {
        org.opensearch.protobufs.DerivedField proto = org.opensearch.protobufs.DerivedField.newBuilder()
            .setType("keyword")
            .setScript(
                Script.newBuilder()
                    .setInline(
                        InlineScript.newBuilder()
                            .setSource("emit(doc['field'])")
                            .setLang(ScriptLanguage.newBuilder().setBuiltin(BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_PAINLESS).build())
                            .build()
                    )
                    .build()
            )
            .build();

        DerivedField derivedField = DerivedFieldProtoUtils.fromProto("scripted_field", proto);

        // Verify script with language
        assertEquals("emit(doc['field'])", derivedField.getScript().getIdOrCode());
        assertEquals("painless", derivedField.getScript().getLang());
    }

    public void testFromProtoWithComplexProperties() {
        // Create nested properties
        ObjectMap properties = ObjectMap.newBuilder()
            .putFields("field1", ObjectMap.Value.newBuilder().setString("text").build())
            .putFields("field2", ObjectMap.Value.newBuilder().setString("keyword").build())
            .putFields("field3", ObjectMap.Value.newBuilder().setString("long").build())
            .build();

        org.opensearch.protobufs.DerivedField proto = org.opensearch.protobufs.DerivedField.newBuilder()
            .setType("object")
            .setScript(Script.newBuilder().setInline(InlineScript.newBuilder().setSource("emit(doc['complex'])").build()).build())
            .setProperties(properties)
            .build();

        DerivedField derivedField = DerivedFieldProtoUtils.fromProto("complex_object", proto);

        // Verify all properties
        Map<String, Object> props = derivedField.getProperties();
        assertEquals(3, props.size());
        assertEquals("text", props.get("field1"));
        assertEquals("keyword", props.get("field2"));
        assertEquals("long", props.get("field3"));
    }

    public void testFromProtoMatchesRESTSidePattern() {
        // This test verifies that the pattern matches DerivedFieldMapper.Builder.build()
        // which uses: simple constructor + conditional setters

        ObjectMap properties = ObjectMap.newBuilder().putFields("sub", ObjectMap.Value.newBuilder().setString("text").build()).build();

        org.opensearch.protobufs.DerivedField proto = org.opensearch.protobufs.DerivedField.newBuilder()
            .setType("object")
            .setScript(Script.newBuilder().setInline(InlineScript.newBuilder().setSource("emit(doc['test'])").build()).build())
            .setProperties(properties)
            .setPrefilterField("prefilter")
            .setFormat("format")
            .setIgnoreMalformed(true)
            .build();

        DerivedField derivedField = DerivedFieldProtoUtils.fromProto("rest_parity_field", proto);

        // Verify the object was created with simple constructor pattern
        // (name, type, script) + conditional setters for optional fields
        assertEquals("rest_parity_field", derivedField.getName());
        assertEquals("object", derivedField.getType());
        assertNotNull(derivedField.getScript());

        // Verify optional fields were set via setters
        assertNotNull(derivedField.getProperties());
        assertEquals("prefilter", derivedField.getPrefilterField());
        assertEquals("format", derivedField.getFormat());
        assertTrue(derivedField.getIgnoreMalformed());
    }
}
