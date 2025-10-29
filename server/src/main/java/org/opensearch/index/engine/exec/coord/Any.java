/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.exec.DataFormat;

import java.util.List;

public class Any implements DataFormat {

    private List<DataFormat> dataFormats;

    private DataFormat primaryDataFormat;

    public Any(List<DataFormat> dataFormats, DataFormat primaryDataFormat) {
        this.dataFormats = dataFormats;
        this.primaryDataFormat = primaryDataFormat;
    }

    public DataFormat getPrimaryDataFormat() {
        return primaryDataFormat;
    }

    @Override
    public Setting<Settings> dataFormatSettings() {
        return null;
    }

    @Override
    public Setting<Settings> clusterLeveldataFormatSettings() {
        return null;
    }

    @Override
    public String name() {
        return "all";
    }

    public List<DataFormat> getDataFormats() {
        return dataFormats;
    }

    @Override
    public void configureStore() {
        for (DataFormat dataFormat : dataFormats) {
            dataFormat.configureStore();
        }
    }
}
