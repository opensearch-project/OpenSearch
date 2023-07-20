/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.diagnostics.metrics;

import java.util.Map;

/**
 * Represents a metric point with a list of measurements, attributes, and their observation time.
 */
public class DiagnosticMetric {
    private final Map<String, Measurement<Number>> measurements;
    private final long observationTime;
    private final Map<String, Object> attributes;

    /**
     * Constructs a new Metric with the specified measurements, attributes, and observation time.
     *
     * @param measurements   the measurements associated with the metric
     * @param attributes     the attributes associated with the metric
     * @param observationTime the observation time of the metric in milliseconds
     * @throws IllegalArgumentException if any of the input parameters are null
     */
    public DiagnosticMetric(Map<String, Measurement<Number>> measurements, Map<String, Object> attributes,
                            long observationTime) {
        if (measurements == null || measurements.isEmpty()) {
            throw new IllegalArgumentException("Measurements cannot be null");
        }

        this.measurements = measurements;
        this.attributes = attributes;
        this.observationTime = observationTime;
    }

    /**
     * Returns the observation time of the metric.
     *
     * @return the observation time in milliseconds
     */
    public long getObservationTime() {
        return observationTime;
    }

    /**
     * Returns the measurement associated with the specified name.
     *
     * @param name the name of the measurement
     * @return the measurement object, or null if not found
     */
    public Measurement<Number> getMeasurement(String name) {
        return measurements.get(name);
    }

    /**
     * Returns an unmodifiable map of all the measurements associated with the metric.
     *
     * @return an unmodifiable map of measurement names to measurement objects
     */
    public Map<String, Measurement<Number>> getMeasurements() {
        return measurements;
    }

    /**
     * Returns an unmodifiable map of the attributes associated with the metric.
     *
     * @return an unmodifiable map of attribute keys to attribute values
     */
    public Map<String, Object> getAttributes() {
        return attributes;
    }

}

