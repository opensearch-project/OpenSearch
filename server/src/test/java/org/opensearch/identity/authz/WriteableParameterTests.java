/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.identity.authz;

import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamInput;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WriteableParameterTests extends OpenSearchTestCase {

    public void testBooleanParameter() throws IOException {
        WriteableParameter wp = new WriteableParameter(new CheckableParameter<>("booleanParam", true, Boolean.class));
        assertEquals(WriteableParameter.ParameterType.Boolean, wp.getType());
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            wp.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                WriteableParameter wpIn = new WriteableParameter(in);

                assertEquals(WriteableParameter.ParameterType.Boolean, wpIn.getType());
                CheckableParameter checkableParameter = wpIn.getParameter();
                assertEquals("booleanParam", checkableParameter.getKey());
                assertTrue((Boolean) checkableParameter.getValue());
            }
        }
    }

    public void testIntegerParameter() throws IOException {
        WriteableParameter wp = new WriteableParameter(new CheckableParameter<>("integerParam", 1, Integer.class));
        assertEquals(WriteableParameter.ParameterType.Integer, wp.getType());
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            wp.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                WriteableParameter wpIn = new WriteableParameter(in);

                assertEquals(WriteableParameter.ParameterType.Integer, wpIn.getType());
                CheckableParameter checkableParameter = wpIn.getParameter();
                assertEquals("integerParam", checkableParameter.getKey());
                assertEquals(Integer.valueOf(1), (Integer) checkableParameter.getValue());
            }
        }
    }

    public void testLongParameter() throws IOException {
        WriteableParameter wp = new WriteableParameter(new CheckableParameter<>("longParam", 1l, Long.class));
        assertEquals(WriteableParameter.ParameterType.Long, wp.getType());
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            wp.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                WriteableParameter wpIn = new WriteableParameter(in);

                assertEquals(WriteableParameter.ParameterType.Long, wpIn.getType());
                CheckableParameter checkableParameter = wpIn.getParameter();
                assertEquals("longParam", checkableParameter.getKey());
                assertTrue(1l == (Long) checkableParameter.getValue());
            }
        }
    }

    public void testFloatParameter() throws IOException {
        WriteableParameter wp = new WriteableParameter(new CheckableParameter<>("floatParam", (float) 1.0, Float.class));
        assertEquals(WriteableParameter.ParameterType.Float, wp.getType());
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            wp.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                WriteableParameter wpIn = new WriteableParameter(in);

                assertEquals(WriteableParameter.ParameterType.Float, wpIn.getType());
                CheckableParameter checkableParameter = wpIn.getParameter();
                assertEquals("floatParam", checkableParameter.getKey());
                assertTrue((float) 1.0 == (Float) checkableParameter.getValue());
            }
        }
    }

    public void testDoubleParameter() throws IOException {
        WriteableParameter wp = new WriteableParameter(new CheckableParameter<>("doubleParam", 1.0, Double.class));
        assertEquals(WriteableParameter.ParameterType.Double, wp.getType());
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            wp.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                WriteableParameter wpIn = new WriteableParameter(in);

                assertEquals(WriteableParameter.ParameterType.Double, wpIn.getType());
                CheckableParameter checkableParameter = wpIn.getParameter();
                assertEquals("doubleParam", checkableParameter.getKey());
                assertTrue(1.0 == (Double) checkableParameter.getValue());
            }
        }
    }

    public void testStringParameter() throws IOException {
        WriteableParameter wp = new WriteableParameter(new CheckableParameter<>("stringParam", "test", String.class));
        assertEquals(WriteableParameter.ParameterType.String, wp.getType());
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            wp.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                WriteableParameter wpIn = new WriteableParameter(in);

                assertEquals(WriteableParameter.ParameterType.String, wpIn.getType());
                CheckableParameter checkableParameter = wpIn.getParameter();
                assertEquals("stringParam", checkableParameter.getKey());
                assertEquals("test", (String) checkableParameter.getValue());
            }
        }
    }

    public void testTimeValueParameter() throws IOException {
        WriteableParameter wp = new WriteableParameter(new CheckableParameter<>("timeValueParam", TimeValue.ZERO, TimeValue.class));
        assertEquals(WriteableParameter.ParameterType.TimeValue, wp.getType());
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            wp.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                WriteableParameter wpIn = new WriteableParameter(in);

                assertEquals(WriteableParameter.ParameterType.TimeValue, wpIn.getType());
                CheckableParameter checkableParameter = wpIn.getParameter();
                assertEquals("timeValueParam", checkableParameter.getKey());
                assertEquals(TimeValue.ZERO, (TimeValue) checkableParameter.getValue());
            }
        }
    }

    public void testExceptionHandling() throws SecurityException, IllegalArgumentException {
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> {
                WriteableParameter wp = new WriteableParameter(
                    new CheckableParameter<List>("unsupportedParam", new ArrayList<>(), List.class)
                );
            }
        );
        assertTrue(iae.getMessage().contains("No enum constant"));
    }
}
