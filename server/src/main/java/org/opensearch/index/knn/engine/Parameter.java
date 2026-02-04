/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.knn.engine;

import org.opensearch.common.ValidationException;

/**
 * Parameter that can be set for a method component
 *
 * @param <T> Type parameter takes
 */
public interface Parameter<T> {
    public String getName();
    
    public T getDefaultValue();
    /**
     * Check if the value passed in is valid
     *
     * @param value to be checked
     * @param knnMethodConfigContext context for the validation
     * @return ValidationException produced by validation errors; null if no validations errors.
     */
    public ValidationException validate(Object value, KNNMethodConfigContext knnMethodConfigContext);
}
