/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.SearchEnginePlugin;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Shard-level coordinator managing read engines discovered via {@link SearchEnginePlugin}.
 * Read engines are registered per data format. The DQE obtains engines from here
 * via {@link #getReadEngine(String)}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CompositeEngine implements Closeable {

    private static final Logger logger = LogManager.getLogger(CompositeEngine.class);

    private final Map<String, List<SearchExecEngine<?, ?>>> readEngines;

    public CompositeEngine(List<SearchEnginePlugin> plugins, ShardPath shardPath) throws IOException {
        Map<String, List<SearchExecEngine<?, ?>>> engines = new HashMap<>();
        for (SearchEnginePlugin plugin : plugins) {
            SearchExecEngine<?, ?> engine = plugin.createSearchExecEngine(shardPath);
            String formatKey = plugin.getClass().getSimpleName();
            engines.computeIfAbsent(formatKey, k -> new ArrayList<>()).add(engine);
            if (shardPath != null) {
                logger.info("Registered read engine [{}] for shard [{}]", formatKey, shardPath.getShardId());
            } else {
                logger.info("Registered read engine [{}]", formatKey);
            }
        }
        this.readEngines = Collections.unmodifiableMap(engines);
    }

    public SearchExecEngine<?, ?> getReadEngine(String dataFormat) {
        List<SearchExecEngine<?, ?>> list = readEngines.get(dataFormat);
        return (list != null && !list.isEmpty()) ? list.get(0) : null;
    }

    public SearchExecEngine<?, ?> getPrimaryReadEngine() {
        return readEngines.values().stream().filter(l -> !l.isEmpty()).findFirst().map(l -> l.get(0)).orElse(null);
    }

    public Map<String, List<SearchExecEngine<?, ?>>> getReadEngines() {
        return readEngines;
    }

    @Override
    public void close() throws IOException {
        IOException first = null;
        for (List<SearchExecEngine<?, ?>> list : readEngines.values()) {
            for (SearchExecEngine<?, ?> engine : list) {
                try {
                    engine.close();
                } catch (IOException e) {
                    if (first == null) first = e;
                    else first.addSuppressed(e);
                }
            }
        }
        if (first != null) throw first;
    }
}
