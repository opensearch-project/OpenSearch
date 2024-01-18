/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opensearch.index.codec.freshstartree.aggregator;

/** Data type of doc values */
public enum DataType {
    INT(Integer.BYTES, true), LONG(Long.BYTES, true), FLOAT(Float.BYTES, true), DOUBLE(Double.BYTES, true);

    private final int _size;
    private final boolean _numeric;

    DataType(int size, boolean numeric) {
        _size = size;
        _numeric = numeric;
    }

    /** Returns the number of bytes needed to store the data type. */
    public int size() {
        if (_size >= 0) {
            return _size;
        }
        throw new IllegalStateException("Cannot get number of bytes for: " + this);
    }

    /**
     * Returns {@code true} if the data type is numeric (INT, LONG, FLOAT, DOUBLE, BIG_DECIMAL),
     * {@code false} otherwise.
     */
    public boolean isNumeric() {
        return _numeric;
    }

    /** Converts the given string value to the data type. Returns byte[] for BYTES. */
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
