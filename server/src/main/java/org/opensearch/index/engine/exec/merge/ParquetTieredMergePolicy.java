/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.merge;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.*;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.Version;
import org.opensearch.index.engine.exec.FileMetadata;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class ParquetTieredMergePolicy implements MergePolicy.MergeContext {
    private final TieredMergePolicy luceneMergePolicy;
    private final InfoStream infoStream;

    private static final HashSet<SegmentCommitInfo> mergingSegments = new HashSet<>();
    private static final Set<String> mergingFileNames = new HashSet<>();

    public ParquetTieredMergePolicy() {
        this.luceneMergePolicy = new TieredMergePolicy();
        this.infoStream = new InfoStream() {
            @Override
            public void message(String s, String s1) {
                // TODO: Add logger
                // System.out.println("Parquet merge: " + s + " : " + s1);
            }

            @Override
            public boolean isEnabled(String s) {
                return true;
            }

            @Override
            public void close() throws IOException {
            }
        };
    }

    public List<List<ParquetFileInfo>> findForceMergeCandidates(List<ParquetFileInfo> segments, int maxSegmentCount) throws IOException {
        // Convert Parquet segments to Lucene-style segments
        List<SegmentCommitInfo> luceneSegments = new ArrayList<>();
        Map<SegmentCommitInfo, ParquetFileInfo> segmentMap = new HashMap<>();
        Map<SegmentCommitInfo, Boolean> segmentsToMerge = new HashMap<>();

        for (ParquetFileInfo parquetSegment : segments) {
            ParquetSegmentWrapper wrapper = new ParquetSegmentWrapper(parquetSegment);
            luceneSegments.add(wrapper);
            segmentMap.put(wrapper, parquetSegment);
            segmentsToMerge.put(wrapper, true);
        }

        // Create SegmentInfos (required by Lucene 10)
        SegmentInfos segmentInfos = new SegmentInfos(Version.LATEST.major);
        luceneSegments.forEach(segmentInfos::add);

        // Find merge candidates using Lucene's policy
        List<List<ParquetFileInfo>> merges = new ArrayList<>();
        try {
            MergePolicy.MergeSpecification mergeSpecification = luceneMergePolicy.findForcedMerges(segmentInfos, maxSegmentCount, segmentsToMerge, this);

            if(mergeSpecification != null) {
                List<MergePolicy.OneMerge> luceneMerges = mergeSpecification.merges;

                // Convert back to Parquet segments
                for (MergePolicy.OneMerge merge : luceneMerges) {
                    List<ParquetFileInfo> parquetMerge = new ArrayList<>();

                    for(SegmentCommitInfo segment : merge.segments) {
                        parquetMerge.add(segmentMap.get(segment));
                    }
                    merges.add(parquetMerge);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Error finding merge candidates", e);
        }
        return merges;
    }

    public List<List<ParquetFileInfo>> findMergeCandidates(List<ParquetFileInfo> segments) throws IOException {

        // Convert Parquet segments to Lucene-style segments
        List<SegmentCommitInfo> luceneSegments = new ArrayList<>();
        Map<SegmentCommitInfo, ParquetFileInfo> segmentMap = new HashMap<>();

        for (ParquetFileInfo parquetSegment : segments) {
            ParquetSegmentWrapper wrapper = new ParquetSegmentWrapper(parquetSegment);
            luceneSegments.add(wrapper);
            segmentMap.put(wrapper, parquetSegment);
        }

        // Create SegmentInfos (required by Lucene 10)
        SegmentInfos segmentInfos = new SegmentInfos(Version.LATEST.major);
        luceneSegments.forEach(segmentInfos::add);

        // Find merge candidates using Lucene's policy
        List<List<ParquetFileInfo>> merges = new ArrayList<>();
        try {
            // Get merge candidates from Lucene's policy
            MergePolicy.MergeSpecification mergeSpecification = luceneMergePolicy.findMerges(MergeTrigger.COMMIT, segmentInfos
                , this);

            if(mergeSpecification != null) {
                List<MergePolicy.OneMerge> luceneMerges = mergeSpecification.merges;

                // Convert back to Parquet segments
                for (MergePolicy.OneMerge merge : luceneMerges) {
                    List<ParquetFileInfo> parquetMerge = new ArrayList<>();

                    for(SegmentCommitInfo segment : merge.segments) {
                        parquetMerge.add(segmentMap.get(segment));
                    }
                    merges.add(parquetMerge);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Error finding merge candidates", e);
        }

        return merges;
    }

    @Override
    public int numDeletesToMerge(SegmentCommitInfo segmentCommitInfo) throws IOException {
        return 0;
    }

    @Override
    public int numDeletedDocs(SegmentCommitInfo segmentCommitInfo) {
        return 0;
    }

    @Override
    public InfoStream getInfoStream() {
        return this.infoStream;
    }

    @Override
    public Set<SegmentCommitInfo> getMergingSegments() {
        return Collections.unmodifiableSet(mergingSegments);
    }

    // Configuration methods
    public void setMaxMergedSegmentMB(double mb) {
        luceneMergePolicy.setMaxMergedSegmentMB(mb);
    }

    public void setSegmentsPerTier(double segments) {
        luceneMergePolicy.setSegmentsPerTier(segments);
    }

    public synchronized void addMergingSegment(Collection<FileMetadata> files) {
        try {
            for (FileMetadata fileMetadata : files) {
                mergingSegments.add(new ParquetSegmentWrapper(new ParquetFileInfo(fileMetadata.directory() + "/" + fileMetadata.file())));
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public synchronized void removeMergingSegment(Collection<FileMetadata> files) {
        List<SegmentCommitInfo> segmentToRemove = new ArrayList<>();
        try {
            for(FileMetadata fileMetadata : files) {
                for(SegmentCommitInfo segment : mergingSegments) {
                    if(segment.info.name.equals(fileMetadata.directory()+"/"+fileMetadata.file())) {
                        segmentToRemove.add(segment);
                    }
                }
            }
            segmentToRemove.forEach(segment -> mergingSegments.remove(segment));
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public void setMaxMergeAtOnce(int count) {
        luceneMergePolicy.setMaxMergeAtOnce(count);
    }

    public void setFloorSegmentMB(double mb) {
        luceneMergePolicy.setFloorSegmentMB(mb);
    }


    public static class ParquetFileInfo {
        private final Path path;
        private final long sizeBytes;
        private final long docCount;
        private final String segmentName;  // Added for Lucene compatibility

        public ParquetFileInfo(String path) throws IOException {
            this.path = Paths.get(path);
            this.sizeBytes = Files.size(this.path);
            // TODO
            // For now doc count we are deriving from size of file.
            // Once we have that info as part of Refresh Result, we can change this.
            this.docCount = Files.size(this.path)/1000;

            // SegmentName is same as fileName
            this.segmentName = path;
        }

        public Path getPath() { return path; }
        public long getSizeBytes() { return sizeBytes; }
        public int getDocCount() { return (int)docCount; }
        public String getSegmentName() { return segmentName; }
    }

    private static class ParquetSegmentWrapper extends SegmentCommitInfo {
        private final ParquetFileInfo parquetInfo;

        public ParquetSegmentWrapper(ParquetFileInfo parquetInfo) throws IOException {
            super(
                // SegmentInfo
                new SegmentInfo(
                    // directory - not used for our purpose
                    new NIOFSDirectory(Paths.get("/tmp")),
                    // version
                    Version.LATEST,
                    // min version
                    Version.LATEST,
                    // segment name
                    parquetInfo.getSegmentName(),
                    // maxDoc - number of rows in parquet file
                    parquetInfo.getDocCount(),
                    // isCompound - false as we don't need compound file format
                    false,
                    // has block
                    false,
                    // codec - using default
                    Codec.getDefault(),
                    // diagnostics - map with dummy entry
                    new HashMap<String, String>(Map.of("dummy", "dummy")),
                    // segmentID - generate unique ID
                    UUID.randomUUID().toString().substring(0,16).getBytes(),
                    // map of attribute - map with dummy entru
                    new HashMap<String, String>(Map.of("dummy", "dummy")),
                    // index sort - no specific sort
                    null
                ),
                // Del Count
                0,
                // softDelCount
                0,
                // delGen - no deletions in parquet
                0,
                // fieldInfosGen - no separate field infos
                -1,
                // docValuesGen - no doc values updates
                -1,
                // id
                UUID.randomUUID().toString().substring(0,16).getBytes());
            this.parquetInfo = parquetInfo;
        }

        @Override
        public long sizeInBytes() {
            return parquetInfo.getSizeBytes();
        }

        @Override
        public int getDelCount() {
            return 0;
        }
    }
}

