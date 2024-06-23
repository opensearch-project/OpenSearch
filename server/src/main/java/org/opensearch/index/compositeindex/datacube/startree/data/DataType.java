/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.datacube.startree.data;

/**
 * Data type of doc values
 * @opensearch.internal
 */
public enum DataType {
    INT(Integer.BYTES, true),
    LONG(Long.BYTES, true),
    FLOAT(Float.BYTES, true),
    DOUBLE(Double.BYTES, true);

    private final int size;
    private final boolean numeric;

    DataType(int size, boolean numeric) {
        this.size = size;
        this.numeric = numeric;
    }

    /**
     * Returns the number of bytes needed to store the data type.
     */
    public int size() {
        if (size >= 0) {
            return size;
        }
        throw new IllegalStateException("Cannot get number of bytes for: " + this);
    }

    /**
     * Returns {@code true} if the data type is numeric (INT, LONG, FLOAT, DOUBLE, BIG_DECIMAL),
     * {@code false} otherwise.
     */
    public boolean isNumeric() {
        return numeric;
    }

    /**
     * Converts the given string value to the data type. Returns byte[] for BYTES.
     */
    public Object convert(String value) {
        try {
            switch (this) {
                case INT:
                    return Integer.valueOf(value);
                case LONG:
                    return Long.valueOf(value);
                case FLOAT:
                    return Float.valueOf(value);
                case DOUBLE:
                    return Double.valueOf(value);
                default:
                    throw new IllegalStateException();
            }
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }
}
