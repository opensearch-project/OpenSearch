/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.composite;

import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.exec.format.DataFormat;
import org.opensearch.index.mapper.ParametrizedFieldMapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A representation of together supported data-formats for a given OpenSearch index.
 */
public class CompositeDataFormat implements DataFormat {

    private List<DataFormat> dataFormats;

    public CompositeDataFormat(List<DataFormat> dataFormats) {
        this.dataFormats = dataFormats;
    }

    public Iterable<DataFormat> dataFormats() {
        return dataFormats;
    }

    @Override
    public String name() {
        return "__composite__";
    }

    @Override
    public Settings dataFormatSettings() {
        Settings.Builder builder = Settings.builder();
        for (DataFormat dataFormat : dataFormats) {
            if (dataFormat.dataFormatSettings() != null) {
                builder.put(dataFormat.dataFormatSettings());
            }
        }
        return builder.build();
    }

    @Override
    public Settings nodeLevelDataFormatSettings() {
        Settings.Builder builder = Settings.builder();
        for (DataFormat dataFormat : dataFormats) {
            if (dataFormat.nodeLevelDataFormatSettings() != null) {
                builder.put(dataFormat.nodeLevelDataFormatSettings());
            }
        }
        return builder.build();
    }

    @Override
    public Map<String, List<ParametrizedFieldMapper.Parameter<?>>> parameters() {
        final Map<String, List<ParametrizedFieldMapper.Parameter<?>>> parameters = new HashMap<>();
        for (DataFormat dataFormat : dataFormats) {
            if (dataFormat.parameters() != null) {
                for (Map.Entry<String, List<ParametrizedFieldMapper.Parameter<?>>> entry : dataFormat.parameters().entrySet()) {
                    parameters.compute(entry.getKey(), (k, v) -> {
                        if (v == null) {
                            return new ArrayList<>(entry.getValue());
                        }
                        v.addAll(entry.getValue());
                        return v;
                    });
                }
            }
        }
        return parameters;
    }
}
