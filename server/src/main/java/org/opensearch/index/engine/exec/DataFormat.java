/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.exec.lucene.LuceneDataFormat;
import org.opensearch.index.engine.exec.text.TextDF;

@ExperimentalApi
public interface DataFormat {
    Setting<Settings> dataFormatSettings();

    Setting<Settings> clusterLeveldataFormatSettings();

    String name();

    void configureStore();


    DataFormat LUCENE = new LuceneDataFormat();

    DataFormat TEXT = new TextDF();
}
