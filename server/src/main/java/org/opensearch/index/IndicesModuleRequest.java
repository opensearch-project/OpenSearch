/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.util.Objects;

/**
 * Request for onIndexModule extension point
 *
 * @opensearch.internal
 */
public class IndicesModuleRequest extends TransportRequest {
    private final Index index;
    private final Settings indexSettings;

    public IndicesModuleRequest(IndexModule indexModule) {
        this.index = indexModule.getIndex();
        this.indexSettings = indexModule.getSettings();
    }

    public IndicesModuleRequest(StreamInput in) throws IOException {
        super(in);
        this.index = new Index(in);
        this.indexSettings = Settings.readSettingsFromStream(in);
    }

    public IndicesModuleRequest(Index index, Settings settings) {
        this.index = index;
        this.indexSettings = settings;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        index.writeTo(out);
        Settings.writeSettingsToStream(indexSettings, out);
    }

    @Override
    public String toString() {
        return "IndicesModuleRequest{" + "index=" + index + ", indexSettings=" + indexSettings + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndicesModuleRequest that = (IndicesModuleRequest) o;
        return Objects.equals(index, that.index) && Objects.equals(indexSettings, that.indexSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, indexSettings);
    }
}
