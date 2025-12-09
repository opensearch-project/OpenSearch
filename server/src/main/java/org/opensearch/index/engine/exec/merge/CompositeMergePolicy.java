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
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class CompositeMergePolicy implements MergePolicy.MergeContext {
    private final MergePolicy luceneMergePolicy;
    private final InfoStream infoStream;

    private static final HashSet<SegmentCommitInfo> mergingSegments = new HashSet<>();

    public CompositeMergePolicy(MergePolicy mergePolicy) {
        this.luceneMergePolicy = mergePolicy;
        System.out.println("Merge Policy : " + mergePolicy);
        this.infoStream = new InfoStream() {
            @Override
            public void message(String s, String s1) {
                // TODO: Add logger
//                 System.out.println("Parquet merge: " + s + " : " + s1);
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

    public List<List<CatalogSnapshot.Segment>> findForceMergeCandidates(List<CatalogSnapshot.Segment> segments, int maxSegmentCount) throws IOException {
        // Convert segments to Lucene-style segments
        List<SegmentCommitInfo> luceneSegments = new ArrayList<>();
        Map<SegmentCommitInfo, CatalogSnapshot.Segment> segmentMap = new HashMap<>();
        Map<SegmentCommitInfo, Boolean> segmentsToMerge = new HashMap<>();

        for(CatalogSnapshot.Segment segment : segments) {
            SegmentWrapper wrapper = new SegmentWrapper(segment, calculateSegmentSize(segment));
            luceneSegments.add(wrapper);
            segmentMap.put(wrapper, segment);
            segmentsToMerge.put(wrapper, true);
        }

        // Create SegmentInfos (required by Lucene 10)
        SegmentInfos segmentInfos = new SegmentInfos(Version.LATEST.major);
        luceneSegments.forEach(segmentInfos::add);

        // Find merge candidates using Lucene's policy
        List<List<CatalogSnapshot.Segment>> merges = new ArrayList<>();
        try {
            MergePolicy.MergeSpecification mergeSpecification = luceneMergePolicy.findForcedMerges(segmentInfos, maxSegmentCount, segmentsToMerge, this);

            if(mergeSpecification != null) {
                List<MergePolicy.OneMerge> luceneMerges = mergeSpecification.merges;

                // Convert back to segments
                for (MergePolicy.OneMerge merge : luceneMerges) {
                    List<CatalogSnapshot.Segment> segmentMerge = new ArrayList<>();

                    for(SegmentCommitInfo segment : merge.segments) {
                        segmentMerge.add(segmentMap.get(segment));
                    }
                    merges.add(segmentMerge);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Error finding merge candidates", e);
        }
        return merges;
    }

    public List<List<CatalogSnapshot.Segment>> findMergeCandidates(List<CatalogSnapshot.Segment> segments) throws IOException {

        // Convert segments to Lucene-style segments
        List<SegmentCommitInfo> luceneSegments = new ArrayList<>();
        Map<SegmentCommitInfo, CatalogSnapshot.Segment> segmentMap = new HashMap<>();

        for(CatalogSnapshot.Segment segment : segments) {
            SegmentWrapper wrapper = new SegmentWrapper(segment, calculateSegmentSize(segment));
            luceneSegments.add(wrapper);
            segmentMap.put(wrapper, segment);
        }

        // Create SegmentInfos (required by Lucene 10)
        SegmentInfos segmentInfos = new SegmentInfos(Version.LATEST.major);
        luceneSegments.forEach(segmentInfos::add);

        // Find merge candidates using Lucene's policy
        List<List<CatalogSnapshot.Segment>> merges = new ArrayList<>();
        try {
            // Get merge candidates from Lucene's policy
            MergePolicy.MergeSpecification mergeSpecification = luceneMergePolicy.findMerges(MergeTrigger.COMMIT, segmentInfos
                , this);

            if(mergeSpecification != null) {
                List<MergePolicy.OneMerge> luceneMerges = mergeSpecification.merges;

                // Convert back to segments
                for (MergePolicy.OneMerge merge : luceneMerges) {
                    List<CatalogSnapshot.Segment> segmentMerge = new ArrayList<>();

                    for(SegmentCommitInfo segment : merge.segments) {
                        segmentMerge.add(segmentMap.get(segment));
                    }
                    merges.add(segmentMerge);
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

    private long calculateSegmentSize(CatalogSnapshot.Segment segment) {
        long totalSize = 0;
        try {
            for (WriterFileSet writerFileSet : segment.getDFGroupedSearchableFiles().values()) {
                for (String fileName : writerFileSet.getFiles()) {
                    Path filePath = Path.of(writerFileSet.getDirectory(), fileName);
                    if (java.nio.file.Files.exists(filePath)) {
                        totalSize += java.nio.file.Files.size(filePath);
                    }
                }
            }
        } catch (Exception e) {
            // Log error but continue with 0 size
            System.err.println("Error calculating segment size: " + e.getMessage());
        }
        return totalSize;
    }

    public synchronized void addMergingSegment(Collection<CatalogSnapshot.Segment> segments) {
        try {
            for (CatalogSnapshot.Segment segment : segments) {
                SegmentWrapper wrapper = new SegmentWrapper(segment, calculateSegmentSize(segment));
                mergingSegments.add(wrapper);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public synchronized void removeMergingSegment(Collection<CatalogSnapshot.Segment> segments) {
        List<SegmentCommitInfo> segmentToRemove = new ArrayList<>();
        try {

            for (CatalogSnapshot.Segment segment : segments) {
                SegmentWrapper wrapper = new SegmentWrapper(segment, calculateSegmentSize(segment));
                segmentToRemove.add(wrapper);
            }
            segmentToRemove.forEach(segment -> mergingSegments.remove(segment));
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private static class SegmentWrapper extends SegmentCommitInfo {
        private final long totalSizeBytes;

        public SegmentWrapper(CatalogSnapshot.Segment segment, long totalSizeBytes) throws IOException {
            super(
                // SegmentInfo
                new org.apache.lucene.index.SegmentInfo(
                    // directory - use temp directory
                    new NIOFSDirectory(Paths.get(System.getProperty("java.io.tmpdir"))),
                    // version
                    Version.LATEST,
                    // min version
                    Version.LATEST,
                    // segment name
                    "segment_" + segment.getGeneration(),
                    // maxDoc - total document count across all files in segment
                    // TODO: Get correct total doc from catalogSnaoshot or Segment
                    (int)(totalSizeBytes / 1000),
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
                    // map of attribute - map with dummy entry
                    new HashMap<String, String>(Map.of("dummy", "dummy")),
                    // index sort - no specific sort
                    null
                ),
                // Del Count
                0,
                // softDelCount
                0,
                // delGen - no deletions
                0,
                // fieldInfosGen - no separate field infos
                -1,
                // docValuesGen - no doc values updates
                -1,
                // id
                UUID.randomUUID().toString().substring(0,16).getBytes());
            this.totalSizeBytes = totalSizeBytes;
        }

        @Override
        public long sizeInBytes() {
            return totalSizeBytes;
        }

        @Override
        public int getDelCount() {
            return 0;
        }
    }
}

