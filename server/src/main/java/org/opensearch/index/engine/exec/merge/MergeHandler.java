/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.merge;

import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.Merger;
import org.opensearch.index.engine.exec.composite.CompositeIndexingExecutionEngine;
import org.opensearch.index.engine.exec.coord.Any;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.CompositeEngine;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


public abstract class MergeHandler {

    private final Any compositeDataFormat;

    private final CompositeIndexingExecutionEngine compositeIndexingExecutionEngine;

    private CompositeEngine compositeEngine;
    private Map<DataFormat, Merger> dataFormatMergerMap;

    public MergeHandler(CompositeEngine compositeEngine, CompositeIndexingExecutionEngine compositeIndexingExecutionEngine, Any dataFormats) {
        this.compositeDataFormat = dataFormats;
        this.compositeIndexingExecutionEngine = compositeIndexingExecutionEngine;
        this.compositeEngine = compositeEngine;
        dataFormatMergerMap = new HashMap<>();

        compositeIndexingExecutionEngine.getDelegates().forEach(engine -> {
            try {
                dataFormatMergerMap.put(engine.getDataFormat(), engine.getMerger());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public abstract Collection<OneMerge> findMerges();

    public abstract Collection<OneMerge> findForceMerges(int maxSegmentCount);

    public MergeResult doMerge(OneMerge oneMerge) {

        Map<DataFormat, Collection<FileMetadata>> mergedFiles = new HashMap<>();
        try(CompositeEngine.ReleasableRef<CatalogSnapshot> catalogSnapshot = compositeEngine.acquireSnapshot()) {

            Collection<FileMetadata> filesToMerge = getFilesToMerge(oneMerge, compositeDataFormat.getPrimaryDataFormat(), catalogSnapshot.getRef());

            // Merging primary data format
            MergeResult primaryMergeResult = dataFormatMergerMap.get(compositeDataFormat.getPrimaryDataFormat()).merge(filesToMerge);
            mergedFiles.put(compositeDataFormat.getPrimaryDataFormat(), primaryMergeResult.getMergedFileMetadata().get(compositeDataFormat.getPrimaryDataFormat()));

            // Merging other format as per the old segment + row id -> new row id mapping.
            compositeIndexingExecutionEngine.getDelegates().stream()
                    .filter(engine -> !engine.getDataFormat().equals(compositeDataFormat.getPrimaryDataFormat()))
                    .forEach(indexingExecutionEngine -> {
                        DataFormat dataFormat = indexingExecutionEngine.getDataFormat();
                        Collection<FileMetadata> files = getFilesToMerge(oneMerge, dataFormat, catalogSnapshot.getRef());
                        MergeResult secondaryMergeResult = dataFormatMergerMap.get(dataFormat).merge(files, primaryMergeResult.getRowIdMapping());
                        mergedFiles.put(dataFormat, secondaryMergeResult.getMergedFileMetadata().get(dataFormat));
                    });
            return new MergeResult(primaryMergeResult.getRowIdMapping(), mergedFiles);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Collection<FileMetadata> getFilesToMerge(OneMerge oneMerge, DataFormat dataFormat, CatalogSnapshot catalogSnapshot) {
        if(oneMerge.getDataFormat().name().equalsIgnoreCase(dataFormat.name())) {
            return oneMerge.getFilesToMerge();
        }

        // TODO get file mapping for other data format from catalog snapshot
        for(CatalogSnapshot.Segment segment : catalogSnapshot.getSegments()) {
            if(segment.getSearchableFiles(oneMerge.getDataFormat().name()).equals(oneMerge.getFilesToMerge())) {
                return segment.getSearchableFiles(dataFormat.name());
            }
        }
        throw new RuntimeException("Couldn't find the file to merge for data format [" + dataFormat + "]");
    }
}
