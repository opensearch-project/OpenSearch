/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat.stub;

import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.exec.commit.CommitterFactory;
import org.opensearch.plugins.EnginePlugin;
import org.opensearch.plugins.Plugin;

import java.util.Optional;

public class MockCommitterEnginePlugin extends Plugin implements EnginePlugin {

    public MockCommitterEnginePlugin() {}

    @Override
    public Optional<CommitterFactory> getCommitterFactory(IndexSettings indexSettings) {
        return Optional.of(config -> new InMemoryCommitter(config.engineConfig().getStore()));
    }
}
