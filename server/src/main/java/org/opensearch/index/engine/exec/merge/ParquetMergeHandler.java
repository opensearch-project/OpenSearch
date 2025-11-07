/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.merge;

import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.composite.CompositeIndexingExecutionEngine;
import org.opensearch.index.engine.exec.coord.Any;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.CompositeEngine;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ParquetMergeHandler extends MergeHandler {

    private final ParquetTieredMergePolicy mergePolicy;
    private final CompositeEngine compositeEngine;
    private final CompositeIndexingExecutionEngine compositeIndexingExecutionEngine;
    private final String PARQUET_DATAFORMAT = "parquet";

    public ParquetMergeHandler(
        CompositeEngine compositeEngine,
        CompositeIndexingExecutionEngine compositeIndexingExecutionEngine,
        Any dataFormats,
        ParquetTieredMergePolicy parquetTieredMergePolicy
    ) {
        super(compositeEngine, compositeIndexingExecutionEngine, dataFormats);
        this.compositeEngine = compositeEngine;
        this.compositeIndexingExecutionEngine = compositeIndexingExecutionEngine;

        mergePolicy = parquetTieredMergePolicy;
    }

    @Override
    public Collection<OneMerge> findForceMerges(int maxSegmentCount) {
        List<OneMerge> oneMerges = new ArrayList<>();
        try (CompositeEngine.ReleasableRef<CatalogSnapshot> catalogSnapshotReleasableRef = compositeEngine.acquireSnapshot()) {
            CatalogSnapshot catalogSnapshot = catalogSnapshotReleasableRef.getRef();
            Collection<WriterFileSet> parquetWriterSet = catalogSnapshot.getSearchableFiles(PARQUET_DATAFORMAT);

            List<ParquetTieredMergePolicy.ParquetFileInfo> parquetSegmentInfos = new ArrayList<>();

            for(WriterFileSet writerFileSet : parquetWriterSet) {
                for(String file: writerFileSet.getFiles()) {
                    parquetSegmentInfos.add(new ParquetTieredMergePolicy.ParquetFileInfo(file));
                }
            }

            List<List<ParquetTieredMergePolicy.ParquetFileInfo>> mergeCandidates =
                mergePolicy.findForceMergeCandidates(parquetSegmentInfos, maxSegmentCount);

            // Process merge candidates
            for (int i = 0; i < mergeCandidates.size(); i++) {
                List<ParquetTieredMergePolicy.ParquetFileInfo> mergeGroup = mergeCandidates.get(i);

                Set<FileMetadata> files = new HashSet<>();
                for (ParquetTieredMergePolicy.ParquetFileInfo file : mergeGroup) {
                    Path path = Path.of(file.getSegmentName());
                    files.add(new FileMetadata(PARQUET_DATAFORMAT, path.getParent().toString(), path.getFileName().toString()));
                }
                oneMerges.add(new OneMerge(compositeIndexingExecutionEngine.getDataFormat().getDataFormats().get(0), files));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return oneMerges;
    }

    @Override
    public Collection<OneMerge> findMerges() {
        List<OneMerge> oneMerges = new ArrayList<>();
        try (CompositeEngine.ReleasableRef<CatalogSnapshot> catalogSnapshotReleasableRef = compositeEngine.acquireSnapshot()) {
            CatalogSnapshot catalogSnapshot = catalogSnapshotReleasableRef.getRef();
            Collection<WriterFileSet> parquetWriterSet = catalogSnapshot.getSearchableFiles(PARQUET_DATAFORMAT);

            List<ParquetTieredMergePolicy.ParquetFileInfo> parquetSegmentInfos = new ArrayList<>();

            for(WriterFileSet writerFileSet : parquetWriterSet) {
                for(String file: writerFileSet.getFiles()) {
                    parquetSegmentInfos.add(new ParquetTieredMergePolicy.ParquetFileInfo(file));
                }
            }

            List<List<ParquetTieredMergePolicy.ParquetFileInfo>> mergeCandidates =
                mergePolicy.findMergeCandidates(parquetSegmentInfos);

            // Process merge candidates
            for (int i = 0; i < mergeCandidates.size(); i++) {
                List<ParquetTieredMergePolicy.ParquetFileInfo> mergeGroup = mergeCandidates.get(i);

                Set<FileMetadata> files = new HashSet<>();
                for (ParquetTieredMergePolicy.ParquetFileInfo file : mergeGroup) {
                    Path path = Path.of(file.getSegmentName());
                    files.add(new FileMetadata(PARQUET_DATAFORMAT, path.getParent().toString(), path.getFileName().toString()));
                }
                oneMerges.add(new OneMerge(compositeIndexingExecutionEngine.getDataFormat().getDataFormats().get(0), files));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return oneMerges;
    }

    public synchronized void registerMerge(OneMerge oneMerge) {
        super.registerMerge(oneMerge);
        mergePolicy.addMergingSegment(oneMerge.getFilesToMerge());
    }

    public synchronized void onMergeFinished(OneMerge oneMerge) {
        super.onMergeFinished(oneMerge);
        mergePolicy.removeMergingSegment(oneMerge.getFilesToMerge());
    }

    public synchronized void onMergeFailure(OneMerge oneMerge) {
        super.onMergeFailure(oneMerge);
        mergePolicy.removeMergingSegment(oneMerge.getFilesToMerge());
    }
}
