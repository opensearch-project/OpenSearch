/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.format;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.mapper.ParametrizedFieldMapper;

import java.util.List;
import java.util.Map;

/**
 * Represents a data format.
 */
@ExperimentalApi
public interface DataFormat {

    /**
     *
     * @return name identifier for the data format.
     */
    String name();

    /**
     * Index level settings supported by this data format.
     */
    default Settings dataFormatSettings() {
        return Settings.EMPTY;
    }

    /**
     * Node level data format specific settings exposed by this data format.
     */
    default Settings nodeLevelDataFormatSettings() {
        return Settings.EMPTY;
    }

    /**
     * Mapping parameters which can be supported through the data format.
     * @return map containing data type name, and the supported params.
     */
    default Map<String, List<ParametrizedFieldMapper.Parameter<?>>> parameters() {
        return Map.of();
    }
}
