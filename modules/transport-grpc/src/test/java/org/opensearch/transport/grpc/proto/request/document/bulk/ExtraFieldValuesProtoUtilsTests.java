/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.document.bulk;

import com.google.protobuf.ByteString;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.mapper.extrasource.BytesValue;
import org.opensearch.index.mapper.extrasource.ExtraFieldValue;
import org.opensearch.index.mapper.extrasource.ExtraFieldValues;
import org.opensearch.index.mapper.extrasource.FloatArrayValue;
import org.opensearch.protobufs.BinaryFieldValue;
import org.opensearch.protobufs.BulkRequestBody;
import org.opensearch.protobufs.FloatBinaryLE;
import org.opensearch.protobufs.FloatList;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ExtraFieldValuesProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoReturnsEmptyForNoExtraFieldValues() {
        assertSame(ExtraFieldValues.EMPTY, ExtraFieldValuesProtoUtils.fromProto(BulkRequestBody.newBuilder().build()));
    }

    public void testFromProtoConvertsSupportedTypes() {
        byte[] rawBytes = new byte[] { 0, 1, 2, 127, -128, -1 };
        BulkRequestBody body = BulkRequestBody.newBuilder()
            .putExtraFieldValues("raw_bytes", binaryBytesValue(rawBytes))
            .putExtraFieldValues("vector_values", binaryFloatValues(1.5f, 2.5f))
            .putExtraFieldValues("vector_packed", binaryPackedFloatValue(packFloatLE(3.5f, 4.5f), 2))
            .build();

        ExtraFieldValues extraFieldValues = ExtraFieldValuesProtoUtils.fromProto(body);

        assertEquals(3, extraFieldValues.values().size());
        assertBytesValue(extraFieldValues.get("raw_bytes"), rawBytes);
        assertFloatArrayValue(extraFieldValues.get("vector_values"), false, 1.5f, 2.5f);
        assertFloatArrayValue(extraFieldValues.get("vector_packed"), true, 3.5f, 4.5f);
    }

    public void testFromProtoRejectsInvalidEntryWithFieldPath() {
        BulkRequestBody body = BulkRequestBody.newBuilder()
            .putExtraFieldValues("bad_vector", binaryPackedFloatValue(new byte[] { 1, 2, 3 }))
            .build();

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ExtraFieldValuesProtoUtils.fromProto(body));

        assertTrue(e.getMessage(), e.getMessage().contains("Invalid extra_field_values entry [bad_vector]"));
        assertTrue(e.getMessage(), e.getMessage().contains("packed_le byte length"));
        assertNotNull(e.getCause());
    }

    private static BinaryFieldValue binaryBytesValue(byte... values) {
        return BinaryFieldValue.newBuilder()
            .setBytesValue(org.opensearch.protobufs.BytesValue.newBuilder().setBytes(ByteString.copyFrom(values)).build())
            .build();
    }

    private static BinaryFieldValue binaryFloatValues(float... values) {
        FloatList.Builder floatList = FloatList.newBuilder();
        for (float value : values) {
            floatList.addValues(value);
        }
        return BinaryFieldValue.newBuilder()
            .setFloatArrayValue(org.opensearch.protobufs.FloatArrayValue.newBuilder().setValues(floatList.build()).build())
            .build();
    }

    private static BinaryFieldValue binaryPackedFloatValue(byte[] bytes) {
        return BinaryFieldValue.newBuilder()
            .setFloatArrayValue(
                org.opensearch.protobufs.FloatArrayValue.newBuilder()
                    .setBinaryLe(FloatBinaryLE.newBuilder().setBytesLe(ByteString.copyFrom(bytes)).build())
                    .build()
            )
            .build();
    }

    private static BinaryFieldValue binaryPackedFloatValue(byte[] bytes, int dimension) {
        return BinaryFieldValue.newBuilder()
            .setFloatArrayValue(
                org.opensearch.protobufs.FloatArrayValue.newBuilder()
                    .setBinaryLe(FloatBinaryLE.newBuilder().setBytesLe(ByteString.copyFrom(bytes)).setDimension(dimension).build())
                    .build()
            )
            .build();
    }

    private static void assertBytesValue(ExtraFieldValue value, byte... expected) {
        assertTrue(value instanceof BytesValue);
        assertArrayEquals(expected, BytesReference.toBytes(((BytesValue) value).bytes()));
    }

    private static void assertFloatArrayValue(ExtraFieldValue value, boolean expectedPackedLE, float... expected) {
        assertTrue(value instanceof FloatArrayValue);
        FloatArrayValue floatArrayValue = (FloatArrayValue) value;
        assertEquals(expectedPackedLE, floatArrayValue.isPackedLE());
        assertEquals(expected.length, floatArrayValue.dimension());
        assertArrayEquals(expected, floatArrayValue.asFloatArray(), 0.0f);
    }

    private static byte[] packFloatLE(float... values) {
        ByteBuffer buffer = ByteBuffer.allocate(values.length * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (float value : values) {
            buffer.putFloat(value);
        }
        return buffer.array();
    }
}
