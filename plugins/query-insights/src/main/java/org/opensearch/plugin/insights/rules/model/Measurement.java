/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/**
 * Represents a measurement with a name and a corresponding value.
 *
 * @param <T> the type of the value
 */
public class Measurement<T extends Number & Comparable<T>> {
    String name;
    T value;

    /**
     * Create a Measurement from a StreamInput
     * @param in the StreamInput to read from
     * @throws IOException IOException
     */
    @SuppressWarnings("unchecked")
    public Measurement(StreamInput in) throws IOException {
        this.name = in.readString();
        this.value = (T) in.readGenericValue();
    }

    /**
     * Write Measurement to a StreamOutput
     * @param out the StreamOutput to write
     * @param measurement the Measurement to write
     * @throws IOException IOException
     */
    public static void writeTo(StreamOutput out, Measurement<?> measurement) throws IOException {
        out.writeString(measurement.getName());
        out.writeGenericValue(measurement.getValue());
    }

    /**
     * Constructs a new Measurement with input name and value.
     *
     * @param name  the name of the measurement
     * @param value the value of the measurement
     */
    public Measurement(String name, T value) {
        this.name = name;
        this.value = value;
    }

    /**
     * Returns the name of the measurement.
     *
     * @return the name of the measurement
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the value of the measurement.
     *
     * @return the value of the measurement
     */
    public T getValue() {
        return value;
    }

    /**
     * Compare two measurements on the value
     * @param other the other measure to compare
     * @return 0 if the value that the two measurements holds are the same
     *         -1 if the value of the measurement is smaller than the other one
     *         1 if the value of the measurement is greater than the other one
     */
    @SuppressWarnings("unchecked")
    public int compareTo(Measurement<? extends Number> other) {
        Number otherValue = other.getValue();
        if (value.getClass().equals(otherValue.getClass())) {
            return value.compareTo((T) otherValue);
        } else {
            throw new UnsupportedOperationException(
                String.format(
                    Locale.ROOT,
                    "comparison between different types are not supported : %s, %s",
                    value.getClass(),
                    otherValue.getClass()
                )
            );
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Measurement<?> other = (Measurement<?>) o;
        return Objects.equals(name, other.name) && Objects.equals(value, other.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value);
    }
}
