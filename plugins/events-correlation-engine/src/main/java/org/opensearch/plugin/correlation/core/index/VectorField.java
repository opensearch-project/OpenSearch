/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation.core.index;

import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.io.stream.BytesStreamOutput;

import java.io.IOException;

/**
 * Generic Vector Field defining a correlation vector name, float array.
 *
 * @opensearch.internal
 */
public class VectorField extends Field {

    /**
     * Parameterized ctor for VectorField
     * @param name name of the field
     * @param value float array value for the field
     * @param type type of the field
     */
    public VectorField(String name, float[] value, IndexableFieldType type) {
        super(name, new BytesRef(), type);
        try {
            final byte[] floatToByte = floatToByteArray(value);
            this.setBytesValue(floatToByte);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * converts float array based vector to byte array.
     * @param input float array
     * @return byte array
     */
    protected static byte[] floatToByteArray(float[] input) throws IOException {
        BytesStreamOutput objectStream = new BytesStreamOutput();
        objectStream.writeFloatArray(input);
        return objectStream.bytes().toBytesRef().bytes;
    }
}
