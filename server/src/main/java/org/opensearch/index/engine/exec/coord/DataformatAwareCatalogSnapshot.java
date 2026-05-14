/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.store.FileMetadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Concrete implementation of {@link CatalogSnapshot} for the composite multi-format engine.
 * Holds segments grouped by data format, supports searchable file lookups across formats,
 * and tracks snapshot metadata including user data and writer generation.
 */
@ExperimentalApi
public class DataformatAwareCatalogSnapshot extends CatalogSnapshot {

    private final long id;
    private final List<Segment> segments;
    private final long lastWriterGeneration;
    private final long numDocs;
    private Map<String, String> userData;
    // Lazily built; racy construction is safe (idempotent result).
    private volatile Map<String, Long> fileToFormatVersion;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    // Mutated via setCommitInfo after a flush from a different thread than
    // upload/recovery consumers; volatile ensures the latest commit is visible.
    private volatile String lastCommitFileName;
    private volatile long lastCommitGeneration = -1;
    // Long-encoded format version (see LuceneVersionConverter) that wrote the last commit.
    // Kept as raw long so this class has no dependency on Lucene's Version type.
    private volatile long lastCommitDataFormatVersion = 0L;

    /**
     * Constructs a new DataformatAwareCatalogSnapshot.
     *
     * @param id the unique snapshot identifier
     * @param generation the monotonically increasing generation number
     * @param version the schema version for serialization compatibility
     * @param segments the list of segments in this snapshot
     * @param lastWriterGeneration the generation of the last writer that contributed to this snapshot
     * @param userData user-defined metadata key-value pairs
     */
    DataformatAwareCatalogSnapshot(
        long id,
        long generation,
        long version,
        List<Segment> segments,
        long lastWriterGeneration,
        Map<String, String> userData
    ) {
        this(id, generation, version, segments, lastWriterGeneration, userData, null, -1, 0L);
    }

    /**
     * Constructs a new DataformatAwareCatalogSnapshot carrying forward the last committed segments file name.
     * <p>
     * This constructor ensures that the {@code segmentsFileName} from a prior flush is preserved
     * across refreshes and merges, which would otherwise create new snapshots without it.
     *
     * @param id the unique snapshot identifier
     * @param generation the monotonically increasing generation number
     * @param version the schema version for serialization compatibility
     * @param segments the list of segments in this snapshot
     * @param lastWriterGeneration the generation of the last writer that contributed to this snapshot
     * @param userData user-defined metadata key-value pairs
     * @param lastCommittedFileName the segments_N file name from the most recent flush, or null
     * @param lastCommitGeneration the Lucene generation of the most recent commit, or -1
     * @param lastCommitDataFormatVersion the format version that wrote the most recent commit, or ""
     */
    DataformatAwareCatalogSnapshot(
        long id,
        long generation,
        long version,
        List<Segment> segments,
        long lastWriterGeneration,
        Map<String, String> userData,
        String lastCommittedFileName,
        long lastCommitGeneration,
        long lastCommitDataFormatVersion
    ) {
        super("dataformat_aware_catalog_snapshot", generation, version);
        this.id = id;
        this.segments = Collections.unmodifiableList(new ArrayList<>(segments));
        this.lastWriterGeneration = lastWriterGeneration;
        this.numDocs = computeNumDocs(this.segments);
        this.userData = Map.copyOf(userData);
        this.lastCommitFileName = lastCommittedFileName;
        this.lastCommitGeneration = lastCommitGeneration;
        this.lastCommitDataFormatVersion = lastCommitDataFormatVersion;
    }

    /**
     * Constructs a DataformatAwareCatalogSnapshot from a {@link StreamInput}.
     *
     * @param in the stream input to read from
     * @param directoryResolver function that maps a data format name to its directory path
     * @throws IOException if an I/O error occurs
     */
    DataformatAwareCatalogSnapshot(StreamInput in, Function<String, String> directoryResolver) throws IOException {
        super(in);
        this.userData = in.readMap(StreamInput::readString, StreamInput::readString);
        this.id = in.readLong();
        this.lastWriterGeneration = in.readLong();
        int segmentCount = in.readVInt();
        if (segmentCount < 0 || segmentCount > in.available()) {
            throw new IOException("Invalid segment count: " + segmentCount);
        }
        List<Segment> segmentList = new ArrayList<>(segmentCount);
        for (int i = 0; i < segmentCount; i++) {
            segmentList.add(new Segment(in, directoryResolver));
        }
        this.segments = Collections.unmodifiableList(segmentList);
        this.numDocs = computeNumDocs(this.segments);
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public List<Segment> getSegments() {
        return List.copyOf(segments);
    }

    @Override
    public Collection<WriterFileSet> getSearchableFiles(String dataFormat) {
        List<WriterFileSet> result = new ArrayList<>();
        for (Segment segment : segments) {
            WriterFileSet writerFileSet = segment.dfGroupedSearchableFiles().get(dataFormat);
            if (writerFileSet != null) {
                result.add(writerFileSet);
            }
        }
        return result;
    }

    @Override
    public Set<String> getDataFormats() {
        Set<String> formats = new HashSet<>();
        for (Segment segment : segments) {
            formats.addAll(segment.dfGroupedSearchableFiles().keySet());
        }
        return formats;
    }

    @Override
    public long getLastWriterGeneration() {
        return lastWriterGeneration;
    }

    @Override
    public Map<String, String> getUserData() {
        return userData;
    }

    @Override
    public void setUserData(Map<String, String> userData, boolean commitData) {
        this.userData = Map.copyOf(userData);
    }

    @Override
    public String serializeToString() throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            this.writeTo(out);
            return Base64.getEncoder().encodeToString(BytesReference.toBytes(out.bytes()));
        }
    }

    /**
     * Deserializes a {@link DataformatAwareCatalogSnapshot} from a Base64-encoded binary string.
     *
     * @param serializedData the Base64 string produced by {@link #serializeToString()}
     * @param directoryResolver function that maps a data format name to its directory path
     * @return a reconstructed {@link DataformatAwareCatalogSnapshot}
     * @throws IOException if the data is malformed or missing required fields
     */
    public static DataformatAwareCatalogSnapshot deserializeFromString(String serializedData, Function<String, String> directoryResolver)
        throws IOException {
        if (serializedData == null || serializedData.isEmpty()) {
            throw new IOException("Cannot deserialize DataformatAwareCatalogSnapshot: input is null or empty");
        }
        try {
            byte[] bytes = Base64.getDecoder().decode(serializedData);
            try (BytesStreamInput in = new BytesStreamInput(bytes)) {
                return new DataformatAwareCatalogSnapshot(in, directoryResolver);
            }
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Failed to deserialize DataformatAwareCatalogSnapshot: " + e.getMessage(), e);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        Map<String, String> userData = new HashMap<>(this.userData);
        userData.remove(DataformatAwareCatalogSnapshot.CATALOG_SNAPSHOT_KEY);
        out.writeMap(userData, StreamOutput::writeString, StreamOutput::writeString);
        out.writeLong(id);
        out.writeLong(lastWriterGeneration);
        out.writeVInt(segments.size());
        for (Segment seg : segments) {
            seg.writeTo(out);
        }
    }

    @Override
    public DataformatAwareCatalogSnapshot clone() {
        return new DataformatAwareCatalogSnapshot(
            id,
            generation,
            version,
            segments,
            lastWriterGeneration,
            userData,
            this.getLastCommitFileName(),
            this.lastCommitGeneration,
            this.lastCommitDataFormatVersion
        );
    }

    @Override
    public long getFormatVersionForFile(String file) {
        Map<String, Long> map = fileToFormatVersion;
        if (map == null) {
            map = buildFileToFormatVersionMap();
            fileToFormatVersion = map;
        }
        Long v = map.get(file);
        return v == null ? 0L : v;
    }

    private Map<String, Long> buildFileToFormatVersionMap() {
        Map<String, Long> m = new HashMap<>();
        for (Segment s : segments) {
            for (WriterFileSet wfs : s.dfGroupedSearchableFiles().values()) {
                for (String f : wfs.files()) {
                    m.put(f, wfs.formatVersion());
                }
            }
        }
        return Map.copyOf(m);
    }

    @Override
    public long getMinSegmentFormatVersion() {
        return 0L;  // cross-format min is semantically meaningless
    }

    @Override
    public long getCommitDataFormatVersion() {
        // Populated by setLastCommitInfo from the Committer's CommitResult on flush.
        // 0L means no commit has landed on this snapshot yet.
        return lastCommitDataFormatVersion;
    }

    /**
     * Returns the total document count, precomputed during construction by summing
     * {@code numRows} from each segment's first available format.
     */
    @Override
    public long getNumDocs() {
        return numDocs;
    }

    private static long computeNumDocs(List<Segment> segments) {
        return segments.stream()
            .mapToLong(segment -> segment.dfGroupedSearchableFiles().values().stream().findFirst().map(WriterFileSet::numRows).orElse(0L))
            .sum();
    }

    /**
     * Returns the {@code segments_N} filename associated with this snapshot's Lucene commit,
     * or {@code null} if this snapshot has not yet been committed (e.g., refresh-only snapshots).
     */
    @Override
    public synchronized String getLastCommitFileName() {
        return lastCommitFileName;
    }

    @Override
    public long getLastCommitGeneration() {
        if (lastCommitGeneration >= 0) {
            return lastCommitGeneration;
        }
        return getGeneration();
    }

    /**
     * Sets the commit file name and Lucene generation produced by the commit that persisted this snapshot.
     * Called by the engine after {@code Committer.commit()} or by the replica after
     * {@code store.commitSegmentInfos()}.
     *
     * @param commitFileName the segments_N filename (e.g., "segments_2")
     * @param commitGeneration the Lucene generation of the commit
     * @param commitDataFormatVersion the format version string that wrote this commit
     */
    public synchronized void setLastCommitInfo(String commitFileName, long commitGeneration, long commitDataFormatVersion) {
        this.lastCommitFileName = commitFileName;
        this.lastCommitGeneration = commitGeneration;
        this.lastCommitDataFormatVersion = commitDataFormatVersion;
    }

    @Override
    protected void closeInternal() {
        closed.set(true);
    }

    /**
     * Returns {@code true} if {@link #closeInternal()} has been invoked (ref count reached zero).
     * This method is intended for testing only.
     */
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public Collection<String> getFiles(boolean includeSegmentsFile) throws IOException {
        List<String> fileNames = new ArrayList<>();
        for (Segment segment : segments) {
            for (Map.Entry<String, WriterFileSet> entry : segment.dfGroupedSearchableFiles().entrySet()) {
                String formatName = entry.getKey();
                for (String file : entry.getValue().files()) {
                    fileNames.add(FileMetadata.serialize(formatName, file));
                }
            }
        }
        if (includeSegmentsFile) {
            String segFile = getLastCommitFileName();
            if (segFile != null) {
                fileNames.add(segFile);
            }
        }
        return fileNames;
    }

    @Override
    public String toString() {
        return "DataformatAwareCatalogSnapshot{"
            + "id="
            + id
            + ", segments="
            + segments
            + ", lastWriterGeneration="
            + lastWriterGeneration
            + ", userData="
            + userData
            + ", closed="
            + closed
            + '}';
    }

    /**
     * Transient {@link org.apache.lucene.index.SegmentInfos} that produced this snapshot, when
     * available. Set by segment replication on the replica side so that the replica's commit
     * path can write a {@code segments_N} containing the real Lucene segment entries (not an
     * empty synthetic one). This guarantees that if the replica is later promoted to primary,
     * the new {@code IndexWriter} opens on a valid commit with all Lucene segments visible.
     *
     * <p>Not serialized — only lives in memory during the replication cycle. Null on the
     * primary's own snapshots and on snapshots loaded from disk.
     */
    private volatile org.apache.lucene.index.SegmentInfos commitSegmentInfos;

    public org.apache.lucene.index.SegmentInfos getCommitSegmentInfos() {
        return commitSegmentInfos;
    }

    public void setCommitSegmentInfos(org.apache.lucene.index.SegmentInfos infos) {
        this.commitSegmentInfos = infos;
    }
}
