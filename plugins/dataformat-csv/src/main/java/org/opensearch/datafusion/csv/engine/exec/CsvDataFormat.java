/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.csv.engine.exec;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.exec.DataFormat;

public class CsvDataFormat implements DataFormat {
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
        return "csv";
    }

    @Override
    public void configureStore() {

    }
}
