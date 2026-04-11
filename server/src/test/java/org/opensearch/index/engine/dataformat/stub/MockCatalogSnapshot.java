/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat.stub;

import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A mock {@link CatalogSnapshot} for testing purposes.
 */
public class MockCatalogSnapshot extends CatalogSnapshot {
    private final List<Segment> segments;
    private final MockDataFormat format;

    public MockCatalogSnapshot(long generation, List<Segment> segments, MockDataFormat format) {
        super("mock-snapshot", generation, 1L);
        this.segments = segments;
        this.format = format;
    }

    @Override
    public Map<String, String> getUserData() {
        return Map.of();
    }

    @Override
    public long getId() {
        return generation;
    }

    @Override
    public List<Segment> getSegments() {
        return segments;
    }

    @Override
    public Collection<WriterFileSet> getSearchableFiles(String dataFormat) {
        List<WriterFileSet> result = new ArrayList<>();
        for (Segment seg : segments) {
            WriterFileSet wfs = seg.dfGroupedSearchableFiles().get(dataFormat);
            if (wfs != null) result.add(wfs);
        }
        return result;
    }

    @Override
    public Set<String> getDataFormats() {
        return Set.of(format.name());
    }

    @Override
    public long getLastWriterGeneration() {
        return generation;
    }

    @Override
    public String serializeToString() {
        return "mock-snapshot-" + generation;
    }

    @Override
    public void setUserData(Map<String, String> userData) {}

    @Override
    public CatalogSnapshot clone() {
        return new MockCatalogSnapshot(generation, segments, format);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    @Override
    protected void closeInternal() {}
}
