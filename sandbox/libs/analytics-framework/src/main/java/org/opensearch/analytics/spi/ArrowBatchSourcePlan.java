/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to this file be licensed under
 * the Apache-2.0 license or a compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.opensearch.analytics.spi.ArrowBatchSourceFactory.InputColumn;

import java.util.List;
import java.util.Objects;

/**
 * Engine plan that consumes one named {@link ArrowBatchSourceFactory} input.
 *
 * @opensearch.internal
 */
public record ArrowBatchSourcePlan(String inputId, byte[] planBytes, List<InputColumn> inputColumns) {

    public ArrowBatchSourcePlan {
        inputId = Objects.requireNonNull(inputId, "inputId");
        if (inputId.isBlank()) {
            throw new IllegalArgumentException("inputId must not be blank");
        }
        planBytes = Objects.requireNonNull(planBytes, "planBytes").clone();
        if (planBytes.length == 0) {
            throw new IllegalArgumentException("planBytes must not be empty");
        }
        inputColumns = List.copyOf(Objects.requireNonNull(inputColumns, "inputColumns"));
    }

    @Override
    public byte[] planBytes() {
        return planBytes.clone();
    }
}
