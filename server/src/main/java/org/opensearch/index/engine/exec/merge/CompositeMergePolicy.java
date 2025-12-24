/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.merge;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.Version;
import org.opensearch.common.logging.Loggers;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class CompositeMergePolicy implements MergePolicy.MergeContext {
    private final MergePolicy luceneMergePolicy;
    private final InfoStream infoStream;
    private final Logger logger;

    private static final HashSet<SegmentCommitInfo> mergingSegments = new HashSet<>();

    public CompositeMergePolicy(
        MergePolicy mergePolicy,
        ShardId shardId
    ) {
        this.luceneMergePolicy = mergePolicy;
        this.logger = Loggers.getLogger(getClass(), shardId);
        logger.info("Initialized merge policy: {}", mergePolicy);
        this.infoStream = new InfoStream() {
            @Override
            public void message(String component, String message) {
                logger.trace(() -> new ParameterizedMessage("Merge [{}]: {}", component, message));
            }

            @Override
            public boolean isEnabled(String component) {
                return logger.isDebugEnabled();
            }

            @Override
            public void close() throws IOException {
            }
        };
    }

    public List<List<CatalogSnapshot.Segment>> findForceMergeCandidates(List<CatalogSnapshot.Segment> segments, int maxSegmentCount) throws IOException {
        Map<SegmentCommitInfo, CatalogSnapshot.Segment> segmentMap = new HashMap<>();
        SegmentInfos segmentInfos = convertToSegmentInfos(segments, segmentMap);

        Map<SegmentCommitInfo, Boolean> segmentsToMerge = new HashMap<>();
        segmentInfos.forEach(seg -> segmentsToMerge.put(seg, true));

        try {
            MergePolicy.MergeSpecification mergeSpec = luceneMergePolicy.findForcedMerges(
                segmentInfos, maxSegmentCount, segmentsToMerge, this
            );
            return convertMergeSpecification(mergeSpec, segmentMap);
        } catch (Exception e) {
            logger.error(() -> new ParameterizedMessage("Error finding force merge candidates", e));
            throw new RuntimeException("Error finding force merge candidates", e);
        }
    }

    public List<List<CatalogSnapshot.Segment>> findMergeCandidates(List<CatalogSnapshot.Segment> segments) throws IOException {
        Map<SegmentCommitInfo, CatalogSnapshot.Segment> segmentMap = new HashMap<>();
        SegmentInfos segmentInfos = convertToSegmentInfos(segments, segmentMap);

        try {
            MergePolicy.MergeSpecification mergeSpec = luceneMergePolicy.findMerges(
                MergeTrigger.COMMIT, segmentInfos, this
            );
            return convertMergeSpecification(mergeSpec, segmentMap);
        } catch (Exception e) {
            logger.error(() -> new ParameterizedMessage("Error finding merge candidates", e));
            throw new RuntimeException("Error finding merge candidates", e);
        }
    }

    private SegmentInfos convertToSegmentInfos(
        List<CatalogSnapshot.Segment> segments,
        Map<SegmentCommitInfo, CatalogSnapshot.Segment> segmentMap
    ) throws IOException {
        SegmentInfos segmentInfos = new SegmentInfos(Version.LATEST.major);

        for (CatalogSnapshot.Segment segment : segments) {
            SegmentWrapper wrapper = new SegmentWrapper(segment, calculateTotalSize(segment), calculateNumDocs(segment));
            segmentInfos.add(wrapper);
            segmentMap.put(wrapper, segment);
        }

        return segmentInfos;
    }

    private List<List<CatalogSnapshot.Segment>> convertMergeSpecification(
        MergePolicy.MergeSpecification mergeSpecification,
        Map<SegmentCommitInfo, CatalogSnapshot.Segment> segmentMap
    ) {
        List<List<CatalogSnapshot.Segment>> merges = new ArrayList<>();

        if (mergeSpecification != null) {
            for (MergePolicy.OneMerge merge : mergeSpecification.merges) {
                List<CatalogSnapshot.Segment> segmentMerge = new ArrayList<>();
                for (SegmentCommitInfo segment : merge.segments) {
                    segmentMerge.add(segmentMap.get(segment));
                }
                merges.add(segmentMerge);
            }
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

    private long calculateNumDocs(CatalogSnapshot.Segment segment) {
        try {
            return segment.getDFGroupedSearchableFiles().values()
                .stream()
                .mapToLong(WriterFileSet::getNumRows)
                .sum();
        } catch (Exception e) {
            // Log error but continue with 0 size
            logger.warn(() -> new ParameterizedMessage("Error calculating segment size", e));
        }
        return 0;
    }

    private long calculateTotalSize(CatalogSnapshot.Segment segment) {
        try {
            return segment.getDFGroupedSearchableFiles().values()
                .stream()
                .mapToLong(WriterFileSet::getTotalSize)
                .sum();
        } catch (Exception e) {
            // Log error but continue with 0 size
            logger.warn(() -> new ParameterizedMessage("Error calculating segment size", e));
        }
        return 0;
    }

    public synchronized void addMergingSegment(Collection<CatalogSnapshot.Segment> segments) {
        try {
            for (CatalogSnapshot.Segment segment : segments) {
                SegmentWrapper wrapper = new SegmentWrapper(segment, calculateTotalSize(segment), calculateNumDocs(segment));
                mergingSegments.add(wrapper);
            }
        } catch (Exception e) {
            logger.error(() -> new ParameterizedMessage("Failed to add merging segments", e));
            throw new RuntimeException(e);
        }
    }

    public synchronized void removeMergingSegment(Collection<CatalogSnapshot.Segment> segments) {
        List<SegmentCommitInfo> segmentToRemove = new ArrayList<>();
        try {

            for (CatalogSnapshot.Segment segment : segments) {
                SegmentWrapper wrapper = new SegmentWrapper(segment, calculateTotalSize(segment), calculateNumDocs(segment));
                segmentToRemove.add(wrapper);
            }
            segmentToRemove.forEach(mergingSegments::remove);
        } catch (Exception e) {
            logger.error(() -> new ParameterizedMessage("Failed to remove merging segments", e));
            throw new RuntimeException(e);
        }
    }

    private static class SegmentWrapper extends SegmentCommitInfo {
        private final long totalSizeBytes;

        public SegmentWrapper(CatalogSnapshot.Segment segment, long totalSizeBytes, long totalNumDocs) throws IOException {
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
                    (int)(totalNumDocs),
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

