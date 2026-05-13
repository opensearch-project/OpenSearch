/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat.stub;

import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A mock {@link EngineReaderManager} for testing purposes.
 */
public class MockReaderManager implements EngineReaderManager<MockReader> {
    private final String formatName;
    private final Map<Long, MockReader> readers = new HashMap<>();
    public final List<String> addedFiles = new ArrayList<>();
    public final List<String> deletedFiles = new ArrayList<>();

    public MockReaderManager(String formatName) {
        this.formatName = formatName;
    }

    @Override
    public MockReader getReader(CatalogSnapshot snapshot) {
        return readers.get(snapshot.getGeneration());
    }

    public int readerCount() {
        return readers.size();
    }

    @Override
    public void beforeRefresh() {}

    @Override
    public void afterRefresh(boolean didRefresh, CatalogSnapshot snapshot) {
        if (didRefresh == false || readers.containsKey(snapshot.getGeneration())) return;
        Collection<WriterFileSet> files = snapshot.getSearchableFiles(formatName);
        List<String> allFiles = new ArrayList<>();
        long totalRows = 0;
        for (WriterFileSet wfs : files) {
            allFiles.addAll(wfs.files());
            totalRows += wfs.numRows();
        }
        readers.put(snapshot.getGeneration(), new MockReader(allFiles, totalRows));
    }

    @Override
    public void onDeleted(CatalogSnapshot snapshot) {
        MockReader reader = readers.remove(snapshot.getGeneration());
        if (reader != null) reader.close();
    }

    @Override
    public void onFilesDeleted(Collection<String> files) {
        deletedFiles.addAll(files);
    }

    @Override
    public void onFilesAdded(Collection<String> files) {
        addedFiles.addAll(files);
    }

    @Override
    public void close() {
        readers.values().forEach(MockReader::close);
        readers.clear();
    }
}
