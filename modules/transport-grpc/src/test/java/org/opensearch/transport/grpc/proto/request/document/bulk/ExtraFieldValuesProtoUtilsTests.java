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
import org.opensearch.index.mapper.extrasource.DoubleArrayValue;
import org.opensearch.index.mapper.extrasource.ExtraFieldValue;
import org.opensearch.index.mapper.extrasource.ExtraFieldValues;
import org.opensearch.index.mapper.extrasource.FloatArrayValue;
import org.opensearch.index.mapper.extrasource.IntArrayValue;
import org.opensearch.index.mapper.extrasource.LongArrayValue;
import org.opensearch.protobufs.BinaryFieldValue;
import org.opensearch.protobufs.BulkRequestBody;
import org.opensearch.protobufs.DoubleBinaryLE;
import org.opensearch.protobufs.DoubleList;
import org.opensearch.protobufs.FloatBinaryLE;
import org.opensearch.protobufs.FloatList;
import org.opensearch.protobufs.IntBinaryLE;
import org.opensearch.protobufs.IntList;
import org.opensearch.protobufs.LongBinaryLE;
import org.opensearch.protobufs.LongList;
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
            .putExtraFieldValues("double_values", binaryDoubleValues(1.25d, -2.5d))
            .putExtraFieldValues("double_packed", binaryPackedDoubleValue(packDoubleLE(Double.MIN_VALUE, Double.MAX_VALUE), 2))
            .putExtraFieldValues("int_values", binaryIntValues(Integer.MIN_VALUE, 0, Integer.MAX_VALUE))
            .putExtraFieldValues("int_packed", binaryPackedIntValue(packIntLE(-10, 20), 2))
            .putExtraFieldValues("long_values", binaryLongValues(Long.MIN_VALUE, 0L, Long.MAX_VALUE))
            .putExtraFieldValues("long_packed", binaryPackedLongValue(packLongLE(-100L, 200L), 2))
            .build();

        ExtraFieldValues extraFieldValues = ExtraFieldValuesProtoUtils.fromProto(body);

        assertEquals(9, extraFieldValues.values().size());
        assertBytesValue(extraFieldValues.get("raw_bytes"), rawBytes);
        assertFloatArrayValue(extraFieldValues.get("vector_values"), false, 1.5f, 2.5f);
        assertFloatArrayValue(extraFieldValues.get("vector_packed"), true, 3.5f, 4.5f);
        assertDoubleArrayValue(extraFieldValues.get("double_values"), false, 1.25d, -2.5d);
        assertDoubleArrayValue(extraFieldValues.get("double_packed"), true, Double.MIN_VALUE, Double.MAX_VALUE);
        assertIntArrayValue(extraFieldValues.get("int_values"), false, Integer.MIN_VALUE, 0, Integer.MAX_VALUE);
        assertIntArrayValue(extraFieldValues.get("int_packed"), true, -10, 20);
        assertLongArrayValue(extraFieldValues.get("long_values"), false, Long.MIN_VALUE, 0L, Long.MAX_VALUE);
        assertLongArrayValue(extraFieldValues.get("long_packed"), true, -100L, 200L);
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

    private static BinaryFieldValue binaryDoubleValues(double... values) {
        DoubleList.Builder doubleList = DoubleList.newBuilder();
        for (double value : values) {
            doubleList.addValues(value);
        }
        return BinaryFieldValue.newBuilder()
            .setDoubleArrayValue(org.opensearch.protobufs.DoubleArrayValue.newBuilder().setValues(doubleList.build()).build())
            .build();
    }

    private static BinaryFieldValue binaryPackedDoubleValue(byte[] bytes, int dimension) {
        return BinaryFieldValue.newBuilder()
            .setDoubleArrayValue(
                org.opensearch.protobufs.DoubleArrayValue.newBuilder()
                    .setBinaryLe(DoubleBinaryLE.newBuilder().setBytesLe(ByteString.copyFrom(bytes)).setDimension(dimension).build())
                    .build()
            )
            .build();
    }

    private static BinaryFieldValue binaryIntValues(int... values) {
        IntList.Builder intList = IntList.newBuilder();
        for (int value : values) {
            intList.addValues(value);
        }
        return BinaryFieldValue.newBuilder()
            .setIntArrayValue(org.opensearch.protobufs.IntArrayValue.newBuilder().setValues(intList.build()).build())
            .build();
    }

    private static BinaryFieldValue binaryPackedIntValue(byte[] bytes, int dimension) {
        return BinaryFieldValue.newBuilder()
            .setIntArrayValue(
                org.opensearch.protobufs.IntArrayValue.newBuilder()
                    .setBinaryLe(IntBinaryLE.newBuilder().setBytesLe(ByteString.copyFrom(bytes)).setDimension(dimension).build())
                    .build()
            )
            .build();
    }

    private static BinaryFieldValue binaryLongValues(long... values) {
        LongList.Builder longList = LongList.newBuilder();
        for (long value : values) {
            longList.addValues(value);
        }
        return BinaryFieldValue.newBuilder()
            .setLongArrayValue(org.opensearch.protobufs.LongArrayValue.newBuilder().setValues(longList.build()).build())
            .build();
    }

    private static BinaryFieldValue binaryPackedLongValue(byte[] bytes, int dimension) {
        return BinaryFieldValue.newBuilder()
            .setLongArrayValue(
                org.opensearch.protobufs.LongArrayValue.newBuilder()
                    .setBinaryLe(LongBinaryLE.newBuilder().setBytesLe(ByteString.copyFrom(bytes)).setDimension(dimension).build())
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

    private static void assertDoubleArrayValue(ExtraFieldValue value, boolean expectedPackedLE, double... expected) {
        assertTrue(value instanceof DoubleArrayValue);
        DoubleArrayValue doubleArrayValue = (DoubleArrayValue) value;
        assertEquals(expectedPackedLE, doubleArrayValue.isPackedLE());
        assertEquals(expected.length, doubleArrayValue.dimension());
        assertArrayEquals(expected, doubleArrayValue.asDoubleArray(), 0.0d);
    }

    private static void assertIntArrayValue(ExtraFieldValue value, boolean expectedPackedLE, int... expected) {
        assertTrue(value instanceof IntArrayValue);
        IntArrayValue intArrayValue = (IntArrayValue) value;
        assertEquals(expectedPackedLE, intArrayValue.isPackedLE());
        assertEquals(expected.length, intArrayValue.dimension());
        assertArrayEquals(expected, intArrayValue.asIntArray());
    }

    private static void assertLongArrayValue(ExtraFieldValue value, boolean expectedPackedLE, long... expected) {
        assertTrue(value instanceof LongArrayValue);
        LongArrayValue longArrayValue = (LongArrayValue) value;
        assertEquals(expectedPackedLE, longArrayValue.isPackedLE());
        assertEquals(expected.length, longArrayValue.dimension());
        assertArrayEquals(expected, longArrayValue.asLongArray());
    }

    private static byte[] packFloatLE(float... values) {
        ByteBuffer buffer = ByteBuffer.allocate(values.length * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (float value : values) {
            buffer.putFloat(value);
        }
        return buffer.array();
    }

    private static byte[] packDoubleLE(double... values) {
        ByteBuffer buffer = ByteBuffer.allocate(values.length * Double.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (double value : values) {
            buffer.putDouble(value);
        }
        return buffer.array();
    }

    private static byte[] packIntLE(int... values) {
        ByteBuffer buffer = ByteBuffer.allocate(values.length * Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (int value : values) {
            buffer.putInt(value);
        }
        return buffer.array();
    }

    private static byte[] packLongLE(long... values) {
        ByteBuffer buffer = ByteBuffer.allocate(values.length * Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (long value : values) {
            buffer.putLong(value);
        }
        return buffer.array();
    }
}
