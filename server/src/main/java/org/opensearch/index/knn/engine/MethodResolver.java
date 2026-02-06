/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.knn.engine;

/**
 * Interface for resolving the {@link ResolvedMethodContext} for an engine and configuration
 */
public interface MethodResolver {

    /**
     * Creates a new {@link ResolvedMethodContext} filling parameters based on other configuration details. A validation
     * exception will be thrown if the {@link KNNMethodConfigContext} is not compatible with the
     * parameters provided by the user.
     *
     * @param knnMethodContext User provided information regarding the method context. A new context should be
     *                         constructed. This variable will not be modified.
     * @param knnMethodConfigContext Configuration details that can be used for resolving the defaults. Should not be null
     * @param shouldRequireTraining Should the provided context require training
     * @param spaceType Space type for the method. Cannot be null or undefined
     * @return {@link ResolvedMethodContext} with dynamic defaults configured. This will include both the resolved
     *                                       compression as well as the completely resolve {@link KNNMethodContext}.
     *                                       This is guanteed to be a copy of the user provided context.
     * @throws org.opensearch.common.ValidationException on invalid configuration and userprovided context.
     */
    ResolvedMethodContext resolveMethod(
        KNNMethodContext knnMethodContext,
        KNNMethodConfigContext knnMethodConfigContext,
        boolean shouldRequireTraining,
        final SpaceType spaceType
    );
}
