/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.exec.DataFormat;

public class LuceneDataFormat implements DataFormat {

    private final String LUCENE_DATA_FORMAT = "LuceneDataFormat";
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
        return "Lucene";
    }

    @Override
    public void configureStore() {

    }

    @Override
    public String toString() {
        return LUCENE_DATA_FORMAT;
    }
}
