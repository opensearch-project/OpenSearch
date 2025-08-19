/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.IndexingExecutionEngine;

import javax.xml.crypto.Data;

public interface DataFormatPlugin  {

    <T extends DataFormat> IndexingExecutionEngine<T> indexingEngine();

    DataFormat getDataFormat();
}
