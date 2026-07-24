/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.List;
import java.util.Map;

/**
 * Implemented by core field mappers that accept plugin-contributed mapping parameters
 * (see {@link PluginMappingParameter}).
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface PluginMappingParametersAware {

    /** Returns the resolved values of the plugin-contributed parameters on this mapper, keyed by parameter name. */
    Map<String, Object> pluginMappingParameterValues();

    /** Returns the plugin-contributed parameter definitions this mapper was built with. */
    List<PluginMappingParameter> pluginMappingParameterSpecs();
}
