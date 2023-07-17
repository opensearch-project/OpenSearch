/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation.core.index;

import org.apache.lucene.document.FieldType;
import org.junit.Assert;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.common.Randomness;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;

/**
 * Unit tests for VectorField
 */
public class VectorFieldTests extends OpenSearchTestCase {

    private final Random random = Randomness.get();

    /**
     * test VectorField ctor
     */
    public void testVectorField_ctor() {
        VectorField field = new VectorField("test-field", new float[] { 1.0f, 1.0f }, new FieldType());
        Assert.assertEquals("test-field", field.name());
    }

    /**
     * test float vector to array serializer
     * @throws IOException IOException
     */
    public void testVectorAsArraySerializer() throws IOException {
        final float[] vector = getArrayOfRandomFloats(20);

        final BytesStreamOutput objectStream = new BytesStreamOutput();
        objectStream.writeFloatArray(vector);
        final byte[] serializedVector = objectStream.bytes().toBytesRef().bytes;

        final byte[] actualSerializedVector = VectorField.floatToByteArray(vector);

        Assert.assertNotNull(actualSerializedVector);
        Assert.assertArrayEquals(serializedVector, actualSerializedVector);

        final float[] actualDeserializedVector = byteToFloatArray(actualSerializedVector);
        Assert.assertNotNull(actualDeserializedVector);
        Assert.assertArrayEquals(vector, actualDeserializedVector, 0.1f);
    }

    /**
     * test byte array to float vector failures
     */
    public void testByteToFloatArrayFailures() {
        final byte[] serializedVector = "test-dummy".getBytes(StandardCharsets.UTF_8);
        expectThrows(OpenSearchException.class, () -> { byteToFloatArray(serializedVector); });
    }

    private float[] getArrayOfRandomFloats(int length) {
        float[] vector = new float[length];
        for (int i = 0; i < 20; ++i) {
            vector[i] = random.nextFloat();
        }
        return vector;
    }

    private static float[] byteToFloatArray(byte[] byteStream) {
        try (BytesStreamInput objectStream = new BytesStreamInput(byteStream)) {
            return objectStream.readFloatArray();
        } catch (IOException ex) {
            throw ExceptionsHelper.convertToOpenSearchException(ex);
        }
    }
}
