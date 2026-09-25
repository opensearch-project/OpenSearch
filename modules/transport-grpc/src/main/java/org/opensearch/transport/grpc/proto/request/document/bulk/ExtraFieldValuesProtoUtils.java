/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.document.bulk;

import com.google.protobuf.ByteString;
import org.opensearch.index.mapper.extrasource.BytesValue;
import org.opensearch.index.mapper.extrasource.DoubleArrayValue;
import org.opensearch.index.mapper.extrasource.ExtraFieldValue;
import org.opensearch.index.mapper.extrasource.ExtraFieldValues;
import org.opensearch.index.mapper.extrasource.FloatArrayValue;
import org.opensearch.index.mapper.extrasource.IntArrayValue;
import org.opensearch.index.mapper.extrasource.LongArrayValue;
import org.opensearch.protobufs.BinaryFieldValue;
import org.opensearch.protobufs.BulkRequestBody;
import org.opensearch.protobufs.DoubleList;
import org.opensearch.protobufs.FloatList;
import org.opensearch.protobufs.IntList;
import org.opensearch.protobufs.LongList;

import java.util.HashMap;
import java.util.Map;

/**
 * Converts protobuf extra field values into OpenSearch extra source values.
 */
final class ExtraFieldValuesProtoUtils {

    private ExtraFieldValuesProtoUtils() {}

    static ExtraFieldValues fromProto(BulkRequestBody body) {
        Map<String, BinaryFieldValue> m = body.getExtraFieldValuesMap();
        if (m.isEmpty()) {
            return ExtraFieldValues.EMPTY;
        }

        Map<String, ExtraFieldValue> out = new HashMap<>(Math.max(16, m.size() * 2));
        for (Map.Entry<String, BinaryFieldValue> e : m.entrySet()) {
            try {
                out.put(e.getKey(), toExtraFieldValue(e.getValue()));
            } catch (IllegalArgumentException ex) {
                throw new IllegalArgumentException("Invalid extra_field_values entry [" + e.getKey() + "]: " + ex.getMessage(), ex);
            }
        }
        return new ExtraFieldValues(out);
    }

    private static ExtraFieldValue toExtraFieldValue(BinaryFieldValue protoVal) {
        switch (protoVal.getBinaryFieldValueCase()) {
            case BYTES_VALUE: {
                return new BytesValue(BulkRequestParserProtoUtils.byteStringToBytesReference(protoVal.getBytesValue().getBytes()));
            }
            case FLOAT_ARRAY_VALUE: {
                return toInternalFloatArrayValue(protoVal.getFloatArrayValue());
            }
            case DOUBLE_ARRAY_VALUE: {
                return toInternalDoubleArrayValue(protoVal.getDoubleArrayValue());
            }
            case INT_ARRAY_VALUE: {
                return toInternalIntArrayValue(protoVal.getIntArrayValue());
            }
            case LONG_ARRAY_VALUE: {
                return toInternalLongArrayValue(protoVal.getLongArrayValue());
            }
            case BINARYFIELDVALUE_NOT_SET:
            default:
                throw new IllegalArgumentException("Unsupported/empty BinaryFieldValue: " + protoVal.getBinaryFieldValueCase());
        }
    }

    private static FloatArrayValue toInternalFloatArrayValue(org.opensearch.protobufs.FloatArrayValue fav) {
        switch (fav.getEncodingCase()) {
            case BINARY_LE: {
                final ByteString bs = fav.getBinaryLe().getBytesLe();
                int dim = resolvePackedDimension(bs, fav.getBinaryLe().getDimension(), Float.BYTES, "float");
                return FloatArrayValue.fromPackedBytes(BulkRequestParserProtoUtils.byteStringToBytesReference(bs), dim);
            }
            case VALUES: {
                final FloatList fl = fav.getValues();
                final int count = fl.getValuesCount();
                final float[] arr = new float[count];
                // Important: Avoid boxing, protobuf uses primitive float list internally
                for (int i = 0; i < count; i++) {
                    arr[i] = fl.getValues(i);
                }
                return FloatArrayValue.fromFloatArray(arr);
            }
            case ENCODING_NOT_SET:
            default:
                throw new IllegalArgumentException("FloatArrayValue.repr is not set");
        }
    }

    private static DoubleArrayValue toInternalDoubleArrayValue(org.opensearch.protobufs.DoubleArrayValue dav) {
        switch (dav.getEncodingCase()) {
            case BINARY_LE: {
                final ByteString bs = dav.getBinaryLe().getBytesLe();
                int dim = resolvePackedDimension(bs, dav.getBinaryLe().getDimension(), Double.BYTES, "double");
                return DoubleArrayValue.fromPackedBytes(BulkRequestParserProtoUtils.byteStringToBytesReference(bs), dim);
            }
            case VALUES: {
                final DoubleList dl = dav.getValues();
                final int count = dl.getValuesCount();
                final double[] arr = new double[count];
                // Important: Avoid boxing, protobuf uses primitive double list internally
                for (int i = 0; i < count; i++) {
                    arr[i] = dl.getValues(i);
                }
                return DoubleArrayValue.fromDoubleArray(arr);
            }
            case ENCODING_NOT_SET:
            default:
                throw new IllegalArgumentException("DoubleArrayValue.repr is not set");
        }
    }

    private static IntArrayValue toInternalIntArrayValue(org.opensearch.protobufs.IntArrayValue iav) {
        switch (iav.getEncodingCase()) {
            case BINARY_LE: {
                final ByteString bs = iav.getBinaryLe().getBytesLe();
                int dim = resolvePackedDimension(bs, iav.getBinaryLe().getDimension(), Integer.BYTES, "int");
                return IntArrayValue.fromPackedBytes(BulkRequestParserProtoUtils.byteStringToBytesReference(bs), dim);
            }
            case VALUES: {
                final IntList il = iav.getValues();
                final int count = il.getValuesCount();
                final int[] arr = new int[count];
                // Important: Avoid boxing, protobuf uses primitive int list internally
                for (int i = 0; i < count; i++) {
                    arr[i] = il.getValues(i);
                }
                return IntArrayValue.fromIntArray(arr);
            }
            case ENCODING_NOT_SET:
            default:
                throw new IllegalArgumentException("IntArrayValue.repr is not set");
        }
    }

    private static LongArrayValue toInternalLongArrayValue(org.opensearch.protobufs.LongArrayValue lav) {
        switch (lav.getEncodingCase()) {
            case BINARY_LE: {
                final ByteString bs = lav.getBinaryLe().getBytesLe();
                int dim = resolvePackedDimension(bs, lav.getBinaryLe().getDimension(), Long.BYTES, "long");
                return LongArrayValue.fromPackedBytes(BulkRequestParserProtoUtils.byteStringToBytesReference(bs), dim);
            }
            case VALUES: {
                final LongList ll = lav.getValues();
                final int count = ll.getValuesCount();
                final long[] arr = new long[count];
                // Important: Avoid boxing, protobuf uses primitive long list internally
                for (int i = 0; i < count; i++) {
                    arr[i] = ll.getValues(i);
                }
                return LongArrayValue.fromLongArray(arr);
            }
            case ENCODING_NOT_SET:
            default:
                throw new IllegalArgumentException("LongArrayValue.repr is not set");
        }
    }

    private static int resolvePackedDimension(ByteString bytes, int dimension, int bytesPerElement, String valueType) {
        if (dimension < 0) {
            throw new IllegalArgumentException(valueType + " dimension must be >= 0 but was " + dimension);
        }

        int byteLength = bytes.size();
        if (dimension == 0) {
            if (byteLength % bytesPerElement != 0) {
                throw new IllegalArgumentException(
                    valueType + " packed_le byte length must be multiple of " + bytesPerElement + " but was " + byteLength
                );
            }
            return byteLength / bytesPerElement;
        }

        final int expectedByteLength;
        try {
            expectedByteLength = Math.multiplyExact(dimension, bytesPerElement);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException(valueType + " dimension too large: " + dimension, e);
        }
        if (byteLength != expectedByteLength) {
            throw new IllegalArgumentException(
                "Bad packed " + valueType + " length=" + byteLength + " expected=" + expectedByteLength + " (dim=" + dimension + ")"
            );
        }
        return dimension;
    }
}
