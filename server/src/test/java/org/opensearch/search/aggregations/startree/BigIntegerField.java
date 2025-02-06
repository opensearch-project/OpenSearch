/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/
package org.opensearch.search.aggregations.startree;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StoredValue;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

import java.math.BigInteger;

public final class BigIntegerField extends Field {

    private static final FieldType FIELD_TYPE = new FieldType();
    private static final FieldType FIELD_TYPE_STORED;
    public static final int BYTES = 16;

    static {
        FIELD_TYPE.setDimensions(1, 16);
        FIELD_TYPE.setDocValuesType(DocValuesType.SORTED_NUMERIC);
        FIELD_TYPE.freeze();

        FIELD_TYPE_STORED = new FieldType(FIELD_TYPE);
        FIELD_TYPE_STORED.setStored(true);
        FIELD_TYPE_STORED.freeze();
    }

    private final StoredValue storedValue;

    /**
     * Creates a new BigIntegerField, indexing the provided point, storing it as a DocValue, and optionally
     * storing it as a stored field.
     *
     * @param name field name
     * @param value the BigInteger value
     * @param stored whether to store the field
     * @throws IllegalArgumentException if the field name or value is null.
     */
    public BigIntegerField(String name, BigInteger value, Field.Store stored) {
        super(name, stored == Field.Store.YES ? FIELD_TYPE_STORED : FIELD_TYPE);
        fieldsData = value;
        if (stored == Field.Store.YES) {
            storedValue = new StoredValue(value.longValue());
        } else {
            storedValue = null;
        }
    }

    @Override
    public BytesRef binaryValue() {
        return pack((BigInteger) fieldsData);
    }

    @Override
    public StoredValue storedValue() {
        return storedValue;
    }

    @Override
    public void setLongValue(long value) {
        super.setLongValue(value);
        if (storedValue != null) {
            storedValue.setLongValue(value);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " <" + name + ':' + fieldsData + '>';
    }

    private static BytesRef pack(BigInteger... point) {
        if (point == null) {
            throw new IllegalArgumentException("point must not be null");
        }
        if (point.length == 0) {
            throw new IllegalArgumentException("point must not be 0 dimensions");
        }
        byte[] packed = new byte[point.length * BYTES];

        for (int dim = 0; dim < point.length; dim++) {
            encodeDimension(point[dim], packed, dim * BYTES);
        }

        return new BytesRef(packed);
    }

    /** Encode single BigInteger dimension */
    public static void encodeDimension(BigInteger value, byte[] dest, int offset) {
        NumericUtils.bigIntToSortableBytes(value, BYTES, dest, offset);
    }

}
