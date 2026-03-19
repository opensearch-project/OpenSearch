/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.ShardPath;

/**
 * SPI for pluggable data format engines.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface DataFormatPlugin {

    DataFormat getDataFormat();

    <T extends DataFormat, P extends DocumentInput<?>> IndexingExecutionEngine<T, P> indexingEngine(
        MapperService mapperService,
        ShardPath shardPath,
        IndexSettings indexSettings
    );
}
