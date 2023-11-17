/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.sort;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.NamedWriteable;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.DocValueFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * A {@link Comparable}, {@link DocValueFormat} aware wrapper around a sort value.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public abstract class SortValue implements NamedWriteable, Comparable<SortValue> {
    /**
     * Get a {@linkplain SortValue} for a double.
     */
    public static SortValue from(double d) {
        return new DoubleSortValue(d);
    }

    /**
     * Get a {@linkplain SortValue} for a long.
     */
    public static SortValue from(long l) {
        return new LongSortValue(l);
    }

    /**
     * Get a {@linkplain SortValue} for a unsigned long.
     */
    public static SortValue fromUnsigned(long l) {
        return new UnsignedLongSortValue(l);
    }

    /**
     * Get the list of {@linkplain NamedWriteable}s that this class needs.
     */
    public static List<NamedWriteableRegistry.Entry> namedWriteables() {
        return Arrays.asList(
            new NamedWriteableRegistry.Entry(SortValue.class, DoubleSortValue.NAME, DoubleSortValue::new),
            new NamedWriteableRegistry.Entry(SortValue.class, LongSortValue.NAME, LongSortValue::new),
            new NamedWriteableRegistry.Entry(SortValue.class, UnsignedLongSortValue.NAME, UnsignedLongSortValue::new)
        );
    }

    private SortValue() {
        // All subclasses of this class are defined in this file.
    }

    @Override
    public final int compareTo(SortValue other) {
        /*
         * It might make sense to try and compare doubles to longs
         * *carefully* to get a real sort. but it might not. For now
         * we sort all doubles before all longs.
         */
        int typeCompare = getWriteableName().compareTo(other.getWriteableName());
        if (typeCompare != 0) {
            return typeCompare;
        }
        return compareToSameType(other);
    }

    /**
     * Write the key as xcontent.
     */
    public final XContentBuilder toXContent(XContentBuilder builder, DocValueFormat format) throws IOException {
        if (format == DocValueFormat.RAW) {
            return rawToXContent(builder);
        }
        return builder.value(format(format));
    }

    /**
     * The java object representing the sort value.
     */
    public abstract Object getKey();

    /**
     * Format this value using the provided format.
     */
    public abstract String format(DocValueFormat format);

    /**
     * Write the key as xcontent using the most native type possible.
     */
    protected abstract XContentBuilder rawToXContent(XContentBuilder builder) throws IOException;

    /**
     * Compare this sort value to another sort value of the same type.
     */
    protected abstract int compareToSameType(SortValue obj);

    // Force implementations to override equals for consistency with compareToSameType
    @Override
    public abstract boolean equals(Object obj);

    // Force implementations to override hashCode for consistency with equals
    @Override
    public abstract int hashCode();

    // Force implementations to override toString so debugging isn't a nightmare.
    @Override
    public abstract String toString();

    /**
     * Return this {@linkplain SortValue} as a boxed {@linkplain Number}.
     */
    public abstract Number numberValue();

    private static class DoubleSortValue extends SortValue {
        public static final String NAME = "double";

        private final double key;

        private DoubleSortValue(double key) {
            this.key = key;
        }

        private DoubleSortValue(StreamInput in) throws IOException {
            this.key = in.readDouble();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(key);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public Object getKey() {
            return key;
        }

        @Override
        public String format(DocValueFormat format) {
            return format.format(key).toString();
        }

        @Override
        protected XContentBuilder rawToXContent(XContentBuilder builder) throws IOException {
            return builder.value(key);
        }

        @Override
        protected int compareToSameType(SortValue obj) {
            DoubleSortValue other = (DoubleSortValue) obj;
            return Double.compare(key, other.key);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || false == getClass().equals(obj.getClass())) {
                return false;
            }
            DoubleSortValue other = (DoubleSortValue) obj;
            return key == other.key;
        }

        @Override
        public int hashCode() {
            return Double.hashCode(key);
        }

        @Override
        public String toString() {
            return Double.toString(key);
        }

        @Override
        public Number numberValue() {
            return key;
        }
    }

    private static class LongSortValue extends SortValue {
        public static final String NAME = "long";

        private final long key;

        LongSortValue(long key) {
            this.key = key;
        }

        LongSortValue(StreamInput in) throws IOException {
            key = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(key);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public Object getKey() {
            return key;
        }

        @Override
        public String format(DocValueFormat format) {
            return format.format(key).toString();
        }

        @Override
        protected XContentBuilder rawToXContent(XContentBuilder builder) throws IOException {
            return builder.value(key);
        }

        @Override
        protected int compareToSameType(SortValue obj) {
            LongSortValue other = (LongSortValue) obj;
            return Long.compare(key, other.key);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || false == getClass().equals(obj.getClass())) {
                return false;
            }
            LongSortValue other = (LongSortValue) obj;
            return key == other.key;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(key);
        }

        @Override
        public String toString() {
            return Long.toString(key);
        }

        @Override
        public Number numberValue() {
            return key;
        }
    }

    private static class UnsignedLongSortValue extends SortValue {
        public static final String NAME = "unsigned_long";

        private final long key;

        UnsignedLongSortValue(long key) {
            this.key = key;
        }

        UnsignedLongSortValue(StreamInput in) throws IOException {
            key = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(key);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public Object getKey() {
            return key;
        }

        @Override
        public String format(DocValueFormat format) {
            return format.format(key).toString();
        }

        @Override
        protected XContentBuilder rawToXContent(XContentBuilder builder) throws IOException {
            return builder.value(key);
        }

        @Override
        protected int compareToSameType(SortValue obj) {
            UnsignedLongSortValue other = (UnsignedLongSortValue) obj;
            return Long.compareUnsigned(key, other.key);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || false == getClass().equals(obj.getClass())) {
                return false;
            }
            UnsignedLongSortValue other = (UnsignedLongSortValue) obj;
            return key == other.key;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(key);
        }

        @Override
        public String toString() {
            return Long.toUnsignedString(key);
        }

        @Override
        public Number numberValue() {
            return key;
        }
    }
}
