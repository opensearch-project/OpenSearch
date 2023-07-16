/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.diagnostics.metrics;

/**
 * Represents a measurement with a name and a corresponding value.
 *
 * @param <T> the type of the value
 */
public class Measurement<T> {
    String name;
    T value;

    /**
     * Constructs a new Measurement with the specified name and value.
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
}
