/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.autotagging;

/**
 * Interface for validating a feature value against pre-defined values (such as
 * values from the index, cluster state, etc.) for a specific feature type.
 * @opensearch.experimental
 */
public interface FeatureValueValidator {
    /**
     * Validates the given feature value.
     * @param featureValue the value to validate
     */
    void validate(String featureValue);
}
