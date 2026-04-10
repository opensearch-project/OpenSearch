/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat.merge;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.Version;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.logging.Loggers;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Adapts a Lucene {@link MergePolicy} to work with the data-format-aware segment model.
 * <p>
 * Converts {@link Segment} instances into Lucene {@link SegmentCommitInfo}
 * wrappers so the underlying merge policy can select merge candidates. Also implements
 * {@link MergePolicy.MergeContext} to supply merging-segment tracking and info-stream
 * logging to the Lucene merge policy.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DataFormatAwareMergePolicy implements MergePolicy.MergeContext {
    private final MergePolicy luceneMergePolicy;
    private final InfoStream infoStream;
    private final Logger logger;

    private final HashSet<SegmentCommitInfo> mergingSegments = new HashSet<>();
    private final Directory sharedDirectory;

    /**
     * Constructs a DataFormatAwareMergePolicy.
     *
     * @param mergePolicy the Lucene merge policy to delegate candidate selection to
     * @param shardId     the shard ID for logging context
     */
    public DataFormatAwareMergePolicy(MergePolicy mergePolicy, ShardId shardId) {
        this.luceneMergePolicy = mergePolicy;
        this.logger = Loggers.getLogger(getClass(), shardId);
        this.sharedDirectory = new ByteBuffersDirectory();
        this.infoStream = new InfoStream() {
            @Override
            public void message(String component, String message) {
                logger.debug(() -> new ParameterizedMessage("[DF_MERGE_POLICY] Merge [{}]: {}", component, message));
            }

            @Override
            public boolean isEnabled(String component) {
                return logger.isDebugEnabled();
            }

            @Override
            public void close() throws IOException {}
        };
    }

    /**
     * Finds force-merge candidates from the given segments, targeting the specified maximum segment count.
     *
     * @param segments        the current list of segments
     * @param maxSegmentCount the target maximum number of segments after merging
     * @return a list of segment groups, each group representing one merge operation
     * @throws IOException if an I/O error occurs during candidate selection
     */
    public List<List<Segment>> findForceMergeCandidates(List<Segment> segments, int maxSegmentCount) throws IOException {
        Map<SegmentCommitInfo, Segment> segmentMap = new HashMap<>();
        SegmentInfos segmentInfos = convertToSegmentInfos(segments, segmentMap);

        Map<SegmentCommitInfo, Boolean> segmentsToMerge = new HashMap<>();
        segmentInfos.forEach(seg -> segmentsToMerge.put(seg, true));

        try {
            MergePolicy.MergeSpecification mergeSpec = luceneMergePolicy.findForcedMerges(
                segmentInfos,
                maxSegmentCount,
                segmentsToMerge,
                this
            );
            return convertMergeSpecification(mergeSpec, segmentMap);
        } catch (Exception e) {
            logger.error("Error finding force merge candidates", e);
            throw new RuntimeException("Error finding force merge candidates", e);
        }
    }

    /**
     * Finds merge candidates from the given segments using the configured Lucene merge policy.
     *
     * @param segments the current list of segments
     * @return a list of segment groups, each group representing one merge operation
     * @throws IOException if an I/O error occurs during candidate selection
     */
    public List<List<Segment>> findMergeCandidates(List<Segment> segments) throws IOException {
        Map<SegmentCommitInfo, Segment> segmentMap = new HashMap<>();
        SegmentInfos segmentInfos = convertToSegmentInfos(segments, segmentMap);

        try {
            MergePolicy.MergeSpecification mergeSpec = luceneMergePolicy.findMerges(MergeTrigger.COMMIT, segmentInfos, this);
            return convertMergeSpecification(mergeSpec, segmentMap);
        } catch (Exception e) {
            logger.error("Error finding merge candidates", e);
            throw new RuntimeException("Error finding merge candidates", e);
        }
    }

    /**
     * Registers segments as currently merging so the merge policy excludes them from future candidates.
     *
     * @param segments the segments being merged
     */
    public synchronized void addMergingSegment(Collection<Segment> segments) {
        for (Segment segment : segments) {
            mergingSegments.add(createWrapper(segment));
        }
    }

    /**
     * Removes segments from the currently-merging set after a merge completes or fails.
     *
     * @param segments the segments to remove
     */
    public synchronized void removeMergingSegment(Collection<Segment> segments) {
        for (Segment segment : segments) {
            mergingSegments.remove(createWrapper(segment));
        }
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
    public synchronized Set<SegmentCommitInfo> getMergingSegments() {
        return Set.copyOf(mergingSegments);
    }

    /**
     * Creates a {@link SegmentWrapper} for the given segment.
     *
     * @param segment the segment to wrap
     * @return a Lucene-compatible {@link SegmentCommitInfo} wrapper
     */
    private SegmentWrapper createWrapper(Segment segment) {
        return new SegmentWrapper(sharedDirectory, segment, calculateTotalSize(segment), calculateNumDocs(segment));
    }

    /**
     * Converts a list of {@link Segment} instances into a Lucene {@link SegmentInfos}
     * and populates the reverse mapping from wrapper to original segment.
     *
     * @param segments   the segments to convert
     * @param segmentMap populated with wrapper → original segment mappings
     * @return the Lucene segment infos
     */
    private SegmentInfos convertToSegmentInfos(List<Segment> segments, Map<SegmentCommitInfo, Segment> segmentMap) {
        SegmentInfos segmentInfos = new SegmentInfos(Version.LATEST.major);

        for (Segment segment : segments) {
            SegmentWrapper wrapper = createWrapper(segment);
            segmentInfos.add(wrapper);
            segmentMap.put(wrapper, segment);
        }

        return segmentInfos;
    }

    /**
     * Converts a Lucene {@link MergePolicy.MergeSpecification} back into groups of
     * {@link Segment} instances using the reverse mapping.
     *
     * @param mergeSpecification the Lucene merge specification (may be {@code null})
     * @param segmentMap         the wrapper → original segment mapping
     * @return a list of segment groups, each representing one merge operation
     */
    private List<List<Segment>> convertMergeSpecification(
        MergePolicy.MergeSpecification mergeSpecification,
        Map<SegmentCommitInfo, Segment> segmentMap
    ) {
        List<List<Segment>> merges = new ArrayList<>();

        if (mergeSpecification != null) {
            for (MergePolicy.OneMerge merge : mergeSpecification.merges) {
                List<Segment> segmentMerge = new ArrayList<>();
                for (SegmentCommitInfo segment : merge.segments) {
                    segmentMerge.add(segmentMap.get(segment));
                }
                merges.add(segmentMerge);
            }
        }

        return merges;
    }

    /**
     * Calculates the total document count across all data-format file sets in a segment.
     *
     * @param segment the segment to inspect
     * @return the total number of documents
     */
    private long calculateNumDocs(Segment segment) {
        return segment.dfGroupedSearchableFiles().values().stream().mapToLong(WriterFileSet::numRows).sum();
    }

    /**
     * Calculates the total byte size across all data-format file sets in a segment.
     *
     * @param segment the segment to inspect
     * @return the total size in bytes
     */
    private long calculateTotalSize(Segment segment) {
        return segment.dfGroupedSearchableFiles().values().stream().mapToLong(WriterFileSet::getTotalSize).sum();
    }

    /**
     * Lucene {@link SegmentCommitInfo} wrapper that exposes segment
     * size and doc-count information to the underlying merge policy.
     * <p>
     * Identity is based on segment generation so that wrappers created
     * from the same {@link Segment} are equal, enabling correct
     * {@link HashSet} add/remove behaviour.
     */
    private static class SegmentWrapper extends SegmentCommitInfo {
        private static final byte[] DUMMY_ID = new byte[16];
        private static final Map<String, String> EMPTY_DIAGNOSTICS = Map.of();
        private static final Map<String, String> EMPTY_ATTRIBUTES = Map.of();

        private final long generation;
        private final long totalSizeBytes;

        public SegmentWrapper(Directory directory, Segment segment, long totalSizeBytes, long totalNumDocs) {
            super(
                new org.apache.lucene.index.SegmentInfo(
                    directory,
                    Version.LATEST,
                    Version.LATEST,
                    "segment_" + segment.generation(),
                    (int) Math.min(totalNumDocs, Integer.MAX_VALUE),
                    false,
                    false,
                    Codec.getDefault(),
                    EMPTY_DIAGNOSTICS,
                    DUMMY_ID,
                    EMPTY_ATTRIBUTES,
                    null
                ),
                0,
                0,
                0,
                -1,
                -1,
                DUMMY_ID
            );
            this.generation = segment.generation();
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o instanceof SegmentWrapper other) {
                return generation == other.generation;
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(generation);
        }
    }
}
