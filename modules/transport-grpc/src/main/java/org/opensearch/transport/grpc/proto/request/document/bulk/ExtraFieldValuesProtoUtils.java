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
import org.opensearch.index.mapper.extrasource.ExtraFieldValue;
import org.opensearch.index.mapper.extrasource.ExtraFieldValues;
import org.opensearch.index.mapper.extrasource.FloatArrayValue;
import org.opensearch.protobufs.BinaryFieldValue;
import org.opensearch.protobufs.BulkRequestBody;
import org.opensearch.protobufs.FloatList;

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
