/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.telemetry.tracing;

import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * TelemetryValidators for running validate on all applicable span Validator classes.
 */
public class TelemetryValidators {
    private Collection<Class<? extends SpanDataValidator>> validators;
    /**
     * Base constructor.
     * @param validators list of validators applicable
     */
    public TelemetryValidators(Collection<Class<? extends SpanDataValidator>> validators){
        this.validators = validators;
    }

    /**
     * calls validate of all validators and throws exception in case of error.
     * @param spans List of spans emitted
     * @param requests Request can be indexing/search call
     */
    public void validate(List<MockSpanData> spans, int requests) {
        for (Class<? extends SpanDataValidator> v : this.validators) {
            try {
                SpanDataValidator validator = v.newInstance();
                if (!validator.validate(spans, requests)) {
                    AssertionError error = new AssertionError(String.format(" SpanData validation failed for " +
                        "validator %s %s", v.getName(), printMockSpanData(spans)));
                    throw error;
                }
            } catch (InstantiationException | IllegalAccessException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }

    private String printMockSpanData(List<MockSpanData> spans){
        StringBuilder str = new StringBuilder();
        for (MockSpanData s : spans){
            str.append(s.toString());
            str.append("\n");
        }
        return str.toString();
    }
}
