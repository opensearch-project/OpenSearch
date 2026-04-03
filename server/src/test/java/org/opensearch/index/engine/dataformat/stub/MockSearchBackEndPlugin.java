/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat.stub;

import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.SearchBackEndPlugin;

import java.util.List;

public class MockSearchBackEndPlugin implements SearchBackEndPlugin<Object> {
    private final List<DataFormat> formats;

    public MockSearchBackEndPlugin(List<DataFormat> formats) {
        this.formats = formats;
    }

    @Override
    public String name() {
        return "mock-backend";
    }

    @Override
    public List<DataFormat> getSupportedFormats() {
        return formats;
    }

    @Override
    public EngineReaderManager<?> createReaderManager(Committer committer, DataFormat format, ShardPath shardPath) {
        return new MockReaderManager("mock-columnar");
    }
}
