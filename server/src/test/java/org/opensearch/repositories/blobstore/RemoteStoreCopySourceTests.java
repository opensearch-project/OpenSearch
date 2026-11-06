/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.blobstore;

import org.apache.lucene.util.Version;
import org.opensearch.common.blobstore.ServerSideCopyBlobContainer;
import org.opensearch.common.blobstore.ServerSideCopySource;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.index.store.BaseRemoteSegmentStoreDirectoryTests;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory.UploadedSegmentMetadata;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.RemoteStoreSettings;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * {@link RemoteStoreCopySource} is split between pure per-file matching, exercised directly, and resolution against a
 * real {@link RemoteSegmentStoreDirectory}, exercised through the shared remote store fixture.
 */
public class RemoteStoreCopySourceTests extends BaseRemoteSegmentStoreDirectoryTests {

    private static final String LUCENE_NAME = "_0.fdt";
    private static final String BLOB_NAME = "_0.fdt__gX7bNIIBrs0AUNsR2yEG";
    private static final long LENGTH = 6_442_450_944L;
    /** The raw Lucene footer checksum that both sides describe, in their own encodings. */
    private static final long CHECKSUM = 1234567890123L;

    private final ServerSideCopySource serverSideCopySource = new ServerSideCopySource(
        "aos-rs-euw1-798157642085",
        "7f3a9c2b1d/base/Mv2bK5iQTvKo7Slm_pHcBw/0/segments/data",
        "798157642085"
    );

    @Before
    public void setup() throws IOException {
        new RemoteStoreSettings(
            Settings.builder().put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_ENABLED.getKey(), false).build(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        setupRemoteSegmentStoreDirectory();
    }

    /** The remote store records the checksum in base 10, as {@code Long.toString(checksum)}. */
    private static UploadedSegmentMetadata recorded(String luceneName, String blobName, long length, long checksum) {
        return UploadedSegmentMetadata.fromString(
            luceneName + "::" + blobName + "::" + checksum + "::" + length + "::" + Version.LATEST.major
        );
    }

    /** {@link Store} records the same checksum in base 36, via {@link Store#digestToString}. */
    private static StoreFileMetadata expected(String luceneName, long length, long checksum) {
        return new StoreFileMetadata(luceneName, length, Store.digestToString(checksum), Version.LATEST);
    }

    private RemoteStoreCopySource copySourceFor(Map<String, UploadedSegmentMetadata> segmentsAtCommit) {
        return new RemoteStoreCopySource(serverSideCopySource, segmentsAtCommit);
    }

    private Map<String, UploadedSegmentMetadata> oneFileAtCommit() {
        return Map.of(LUCENE_NAME, recorded(LUCENE_NAME, BLOB_NAME, LENGTH, CHECKSUM));
    }

    // ---------- per-file matching ----------

    public void testResolvesBlobNameForAMatchingFile() {
        RemoteStoreCopySource copySource = copySourceFor(oneFileAtCommit());

        assertEquals(serverSideCopySource, copySource.source());
        assertEquals(Optional.of(BLOB_NAME), copySource.blobNameFor(expected(LUCENE_NAME, LENGTH, CHECKSUM)));
    }

    /**
     * The two sides store the same checksum in different radices. Comparing the strings as-is would disagree for
     * essentially every file and silently disable the copy path, so this is the assertion that keeps the feature
     * working at all.
     */
    public void testMatchesAcrossTheTwoChecksumEncodings() {
        UploadedSegmentMetadata fromRemoteStore = recorded(LUCENE_NAME, BLOB_NAME, LENGTH, CHECKSUM);
        StoreFileMetadata fromLocalStore = expected(LUCENE_NAME, LENGTH, CHECKSUM);

        // Guard the premise: one checksum, two different strings.
        assertNotEquals(fromRemoteStore.getChecksum(), fromLocalStore.checksum());
        assertEquals(Long.toString(CHECKSUM), fromRemoteStore.getChecksum());
        assertEquals(Long.toString(CHECKSUM, Character.MAX_RADIX), fromLocalStore.checksum());

        assertEquals(Optional.of(BLOB_NAME), copySourceFor(Map.of(LUCENE_NAME, fromRemoteStore)).blobNameFor(fromLocalStore));
    }

    public void testNoBlobWhenCommitNeverRecordedTheFile() {
        // Routine: the segments file is never uploaded to the remote store.
        assertEquals(Optional.empty(), copySourceFor(oneFileAtCommit()).blobNameFor(expected("segments_7", 1194, CHECKSUM)));
    }

    public void testNoBlobWhenLengthDisagrees() {
        assertEquals(Optional.empty(), copySourceFor(oneFileAtCommit()).blobNameFor(expected(LUCENE_NAME, LENGTH - 1, CHECKSUM)));
    }

    /**
     * The guard against the UUID-remapping hazard: a blob under the right Lucene name holding different content,
     * uploaded by a different primary.
     */
    public void testNoBlobWhenChecksumDisagrees() {
        assertEquals(Optional.empty(), copySourceFor(oneFileAtCommit()).blobNameFor(expected(LUCENE_NAME, LENGTH, CHECKSUM + 1)));
    }

    public void testNoBlobWhenRecordedChecksumIsUnparseable() {
        UploadedSegmentMetadata malformed = UploadedSegmentMetadata.fromString(
            LUCENE_NAME + "::" + BLOB_NAME + "::not-a-number::" + LENGTH + "::" + Version.LATEST.major
        );

        assertEquals(Optional.empty(), copySourceFor(Map.of(LUCENE_NAME, malformed)).blobNameFor(expected(LUCENE_NAME, LENGTH, CHECKSUM)));
    }

    // ---------- resolution against a real directory ----------

    private void stubCommitLookup(long primaryTerm, long generation, List<String> resolvesTo) throws IOException {
        // Mirrors MetadataFilenameUtils.getMetadataFilePrefixForCommit, which is package-private to index.store.
        String prefix = String.join(
            RemoteSegmentStoreDirectory.MetadataFilenameUtils.SEPARATOR,
            RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX,
            RemoteStoreUtils.invertLong(primaryTerm),
            RemoteStoreUtils.invertLong(generation)
        );
        when(remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(prefix, 1)).thenReturn(resolvesTo);
    }

    private void makeDataContainerCopyCapable() {
        ServerSideCopyBlobContainer container = mock(ServerSideCopyBlobContainer.class);
        when(container.asCopySource()).thenReturn(serverSideCopySource);
        when(remoteDataDirectory.getBlobContainer()).thenReturn(container);
    }

    public void testResolveReadsTheCommitsOwnMapping() throws IOException {
        Map<String, Map<String, String>> contentByMetadataFile = populateMetadata();
        remoteSegmentStoreDirectory.init(); // initialises to metadataFilename, generation 23
        stubCommitLookup(12, 13, List.of(metadataFilename2));
        makeDataContainerCopyCapable();

        Optional<RemoteStoreCopySource> resolved = RemoteStoreCopySource.resolve(remoteSegmentStoreDirectory, 12, 13);
        assertTrue(resolved.isPresent());

        // The blob name must come from the requested commit, not from the directory's current view.
        UploadedSegmentMetadata atCommit = UploadedSegmentMetadata.fromString(contentByMetadataFile.get(metadataFilename2).get("_0.cfs"));
        StoreFileMetadata asStoreKnowsIt = expected("_0.cfs", atCommit.getLength(), Long.parseLong(atCommit.getChecksum()));
        assertEquals(Optional.of(atCommit.getUploadedFilename()), resolved.get().blobNameFor(asStoreKnowsIt));
    }

    public void testResolveIsEmptyWhenDataCannotActAsCopySource() throws IOException {
        populateMetadata();
        stubCommitLookup(12, 13, List.of(metadataFilename2));
        // remoteDataDirectory is a plain mock, so its container is not a ServerSideCopyBlobContainer.

        assertEquals(Optional.empty(), RemoteStoreCopySource.resolve(remoteSegmentStoreDirectory, 12, 13));
    }

    public void testResolveIsEmptyWhenCommitHasAgedOut() throws IOException {
        populateMetadata();
        stubCommitLookup(12, 99, List.of()); // no metadata file for that commit
        makeDataContainerCopyCapable();

        // Expected rather than exceptional: the caller uploads the bytes itself.
        assertEquals(Optional.empty(), RemoteStoreCopySource.resolve(remoteSegmentStoreDirectory, 12, 99));
    }
}
