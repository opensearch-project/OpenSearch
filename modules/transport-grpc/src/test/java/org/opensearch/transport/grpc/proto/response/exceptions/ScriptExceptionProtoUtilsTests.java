/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.exceptions;

import org.opensearch.protobufs.ObjectMap;
import org.opensearch.script.ScriptException;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ScriptExceptionProtoUtilsTests extends OpenSearchTestCase {

    public void testMetadataToProtoWithoutPosition() {
        // Create a ScriptException without position information
        String script = "doc['field'].value > 100";
        String lang = "painless";
        List<String> stack = new ArrayList<>();
        stack.add("line 1: error");
        stack.add("line 2: another error");

        ScriptException exception = new ScriptException("Test script error", new RuntimeException("Test cause"), stack, script, lang);

        // Convert to Protocol Buffer
        Map<String, ObjectMap.Value> metadata = ScriptExceptionProtoUtils.metadataToProto(exception);

        // Verify the conversion
        assertTrue("Should have script_stack field", metadata.containsKey("script_stack"));
        assertTrue("Should have script field", metadata.containsKey("script"));
        assertTrue("Should have lang field", metadata.containsKey("lang"));
        assertFalse("Should have position field", metadata.containsKey("position"));

        // Verify field values
        ObjectMap.Value scriptValue = metadata.get("script");
        ObjectMap.Value langValue = metadata.get("lang");

        assertEquals("script should match", script, scriptValue.getString());
        assertEquals("lang should match", lang, langValue.getString());

        // Verify script stack
        ObjectMap.Value stackValue = metadata.get("script_stack");
        assertEquals("script_stack should have 2 items", 2, stackValue.getListValue().getValueCount());
        assertEquals("First stack item should match", stack.get(0), stackValue.getListValue().getValue(0).getString());
        assertEquals("Second stack item should match", stack.get(1), stackValue.getListValue().getValue(1).getString());
    }

    public void testMetadataToProtoWithPosition() {
        // Create a ScriptException with position information
        String script = "doc['field'].value > 100";
        String lang = "painless";
        List<String> stack = new ArrayList<>();
        stack.add("line 1: error");

        ScriptException.Position pos = new ScriptException.Position(10, 5, 15);
        ScriptException exception = new ScriptException("Test script error", new RuntimeException("Test cause"), stack, script, lang, pos);

        // Convert to Protocol Buffer
        Map<String, ObjectMap.Value> metadata = ScriptExceptionProtoUtils.metadataToProto(exception);

        // Verify the conversion
        assertTrue("Should have position field", metadata.containsKey("position"));

        // Verify position field
        ObjectMap.Value positionValue = metadata.get("position");
        Map<String, ObjectMap.Value> positionMap = positionValue.getObjectMap().getFieldsMap();

        assertTrue("Position should have offset field", positionMap.containsKey("offset"));
        assertTrue("Position should have start field", positionMap.containsKey("start"));
        assertTrue("Position should have end field", positionMap.containsKey("end"));

        assertEquals("offset should match", 10, positionMap.get("offset").getInt32());
        assertEquals("start should match", 5, positionMap.get("start").getInt32());
        assertEquals("end should match", 15, positionMap.get("end").getInt32());
    }

    public void testToProtoBuilderMethod() {
        // Test the toProto method that takes a builder and position
        ScriptException.Position pos = new ScriptException.Position(10, 5, 15);

        // Create a builder and add position
        Map<String, ObjectMap.Value> map = new HashMap<>();
        Map<String, ObjectMap.Value> result = ScriptExceptionProtoUtils.toProto(map, pos);

        // Verify the result
        assertTrue("Should have position field", result.containsKey("position"));

        // Verify position field
        ObjectMap.Value positionValue = result.get("position");
        Map<String, ObjectMap.Value> positionMap = positionValue.getObjectMap().getFieldsMap();

        assertEquals("offset should match", 10, positionMap.get("offset").getInt32());
        assertEquals("start should match", 5, positionMap.get("start").getInt32());
        assertEquals("end should match", 15, positionMap.get("end").getInt32());
    }
}
