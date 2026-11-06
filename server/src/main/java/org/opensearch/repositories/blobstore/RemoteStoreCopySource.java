/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.blobstore;

import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.blobstore.ServerSideCopySource;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory.UploadedSegmentMetadata;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The remote store, viewed as a place a snapshot can copy segment bytes <em>from</em> on the storage provider's side,
 * pinned to one commit.
 *
 * <p>Exists because a remote-store-backed shard already holds its segments in object storage. A snapshot of such a
 * shard would otherwise read every byte out of the local store and write it to the repository through this process,
 * when the provider could copy object to object without the bytes being read at all.
 *
 * <p>Resolved once per shard, then consulted per file. {@link #blobNameFor} is deliberately the only way to get a
 * blob name out: it will not hand back a name unless the commit recorded that file at the size and checksum the
 * caller expects, so a caller cannot copy a blob that merely shares a Lucene file name. That check matters because
 * the remote store mints a fresh UUID suffix on every upload, so one Lucene name can correspond to several blobs
 * written by different primaries.
 *
 * @opensearch.internal
 */
@InternalApi
public final class RemoteStoreCopySource {

    private final ServerSideCopySource source;
    private final Map<String, UploadedSegmentMetadata> segmentsAtCommit;

    // Package-private so that the per-file matching logic can be tested without standing up a remote store.
    RemoteStoreCopySource(final ServerSideCopySource source, final Map<String, UploadedSegmentMetadata> segmentsAtCommit) {
        this.source = Objects.requireNonNull(source);
        this.segmentsAtCommit = Objects.requireNonNull(segmentsAtCommit);
    }

    /**
     * Resolves the remote store as a copy source for one specific commit.
     *
     * <p>Reads the commit's own metadata file rather than the directory's current view, and takes no lock, so it is
     * safe to call while the shard keeps writing. See
     * {@link RemoteSegmentStoreDirectory#readSegmentsForCommit}.
     *
     * @param remoteDirectory the shard's remote segment directory
     * @param primaryTerm     primary term of the commit being snapshotted
     * @param generation      generation of the commit being snapshotted
     * @return the copy source, or empty when this shard cannot serve one. Empty is a normal, expected outcome and
     *         means the caller should upload the bytes itself; it arises when the commit has aged out of the remote
     *         store's metadata retention window, when the shard routes blobs per data format, or when the remote
     *         store's provider has no server-side copy primitive.
     * @throws IOException if the commit's metadata file exists but cannot be read
     */
    public static Optional<RemoteStoreCopySource> resolve(
        final RemoteSegmentStoreDirectory remoteDirectory,
        final long primaryTerm,
        final long generation
    ) throws IOException {
        final Optional<ServerSideCopySource> source = remoteDirectory.asServerSideCopySource();
        if (source.isEmpty()) {
            return Optional.empty();
        }

        final Map<String, UploadedSegmentMetadata> segmentsAtCommit;
        try {
            segmentsAtCommit = remoteDirectory.readSegmentsForCommit(primaryTerm, generation);
        } catch (final NoSuchFileException absent) {
            // The commit aged out of the retention window. Nothing is wrong; there is just nothing to copy from.
            return Optional.empty();
        }

        return Optional.of(new RemoteStoreCopySource(source.get(), segmentsAtCommit));
    }

    /**
     * The source container, prefix and expected owner, as the destination needs them to issue the copy.
     */
    public ServerSideCopySource source() {
        return source;
    }

    /**
     * Name of the blob holding {@code expected}, if the commit recorded that file and the record agrees with
     * {@code expected} on both length and checksum.
     *
     * <p>Returns empty rather than throwing when it does not match, because every negative outcome here has the same
     * correct response: upload the bytes instead. The disagreements are not equivalent in cause, though. A missing
     * entry is routine - the segments file is never uploaded to the remote store at all. A length or checksum
     * disagreement means the blob under that Lucene name is not the content this commit refers to, which is the case
     * worth logging.
     *
     * @param expected the file as the snapshot knows it, from the local store's metadata for the commit
     * @return the blob name to copy from, or empty if it cannot be established that the blob holds this content
     */
    public Optional<String> blobNameFor(final StoreFileMetadata expected) {
        final UploadedSegmentMetadata recorded = segmentsAtCommit.get(expected.name());
        if (recorded == null) {
            return Optional.empty();
        }
        if (recorded.getLength() != expected.length()) {
            return Optional.empty();
        }
        if (checksumsAgree(recorded, expected) == false) {
            return Optional.empty();
        }
        return Optional.of(recorded.getUploadedFilename());
    }

    /**
     * Compares the two recorded checksums across the encodings they are stored in.
     *
     * <p>Both sides hold the same Lucene footer checksum, but as strings in different radices. The remote store
     * writes {@code Long.toString(checksum)}, base 10, while {@link StoreFileMetadata#checksum()} holds
     * {@link Store#digestToString}, which is base 36. Comparing the strings directly would therefore disagree for
     * almost every file and quietly disable the copy path. The conversion mirrors what {@code Store} itself does when
     * it has to bridge the two.
     */
    private static boolean checksumsAgree(final UploadedSegmentMetadata recorded, final StoreFileMetadata expected) {
        try {
            return Store.digestToString(Long.parseLong(recorded.getChecksum())).equals(expected.checksum());
        } catch (final NumberFormatException e) {
            // An unparseable checksum is not something to copy against.
            return false;
        }
    }

    @Override
    public String toString() {
        return "RemoteStoreCopySource{container="
            + source.container()
            + ", prefix="
            + source.keyPrefix()
            + ", files="
            + segmentsAtCommit.size()
            + "}";
    }
}
