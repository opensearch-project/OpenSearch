/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.ServerSideCopySource;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class S3ServerSideCopyTests extends OpenSearchTestCase {

    private static final String AP_ARN = "arn:aws:s3:eu-west-1:798157642085:accesspoint/aos-7k2m9p4x";

    /** Realistic source path taken from an encryption-enabled domain's automated snapshot repository. */
    private static final String SOURCE_BASE_PATH = "SHdXDU0CaN87zue6ezON2A/798157642085/cs/swift-eu-west-1-staging/mustang-40-tb-2/1/enc";

    public void testBucketCopySourceUsesBucketKeyForm() {
        assertEquals("my-bucket/base/snap-1.dat", S3ServerSideCopy.encodeCopySource("my-bucket", "base/snap-1.dat"));
    }

    /**
     * The ARN form is what makes the access point policy apply to the read. Getting this wrong would silently bypass
     * the prefix confinement the access point exists to provide.
     */
    public void testAccessPointArnCopySourceUsesObjectForm() {
        assertEquals(AP_ARN + "/object/base/snap-1.dat", S3ServerSideCopy.encodeCopySource(AP_ARN, "base/snap-1.dat"));
    }

    /**
     * An alias is usable wherever a bucket name is, so it must NOT be rewritten into the ARN object form.
     */
    public void testAccessPointAliasIsTreatedAsBucket() {
        final String alias = "aos-7k2m9p4x-abcdefghijklmnopqrstuvwxyz012345-s3alias";
        assertEquals(alias + "/base/snap-1.dat", S3ServerSideCopy.encodeCopySource(alias, "base/snap-1.dat"));
    }

    public void testCopySourcePreservesPathSeparatorsAndEncodesSpaces() {
        final String encoded = S3ServerSideCopy.encodeCopySource("my-bucket", "a b/c d.dat");
        assertEquals("my-bucket/a%20b/c%20d.dat", encoded);
        assertFalse("path separators must survive encoding", encoded.contains("%2F"));
        assertFalse("'+' is not a valid space encoding here", encoded.contains("+"));
    }

    /**
     * Percent-encoding the ARN's colons produces a copy source S3 rejects. Only the key may be encoded.
     */
    public void testAccessPointArnIsNotPercentEncoded() {
        final String encoded = S3ServerSideCopy.encodeCopySource(AP_ARN, "base/snap 1.dat");
        assertTrue("ARN must appear verbatim", encoded.startsWith(AP_ARN + "/object/"));
        assertFalse("ARN colons must not be encoded", encoded.contains("%3A"));
        assertTrue("key must still be encoded", encoded.endsWith("base/snap%201.dat"));
    }

    public void testPartSizeHonouredWhenPartCountFits() {
        final long configured = new ByteSizeValue(1, ByteSizeUnit.GB).getBytes();
        final long sourceSize = new ByteSizeValue(100, ByteSizeUnit.GB).getBytes();
        assertEquals(configured, S3ServerSideCopy.resolvePartSize(configured, sourceSize));
    }

    /**
     * A large object with a small configured part size would exceed the 10,000 part limit and fail only after the
     * multipart upload had been created, leaving a billable orphan. The part size is raised instead.
     */
    public void testPartSizeRaisedToRespectPartLimit() {
        final long configured = new ByteSizeValue(5, ByteSizeUnit.MB).getBytes();
        final long sourceSize = new ByteSizeValue(1, ByteSizeUnit.TB).getBytes();
        final long resolved = S3ServerSideCopy.resolvePartSize(configured, sourceSize);

        assertTrue("part size must be raised above the configured value", resolved > configured);
        final long partCount = (sourceSize + resolved - 1) / resolved;
        assertTrue("resolved part size must keep part count within the limit", partCount <= S3ServerSideCopy.MAX_PARTS);
    }

    /**
     * Every part but the last must reach S3's 5 MB minimum. A part size below it is rejected only after the multipart
     * upload has been created, so the floor is applied up front rather than relying on the caller's setting bounds.
     */
    public void testPartSizeRaisedToProviderMinimum() {
        // Just over the single-copy threshold, so the part-count limit alone would permit a part under 5 MB.
        final long sourceSize = S3ServerSideCopy.MAX_SINGLE_COPY_SIZE.getBytes() + 1;
        final long partCountFloor = (sourceSize + S3ServerSideCopy.MAX_PARTS - 1) / S3ServerSideCopy.MAX_PARTS;
        assertTrue(
            "fixture should leave the part-count floor below the provider minimum",
            partCountFloor < S3ServerSideCopy.MIN_PART_SIZE.getBytes()
        );

        for (long configured : new long[] { 0L, -1L, 1024L, S3ServerSideCopy.MIN_PART_SIZE.getBytes() - 1 }) {
            assertEquals(
                "part size below the provider minimum must be raised",
                S3ServerSideCopy.MIN_PART_SIZE.getBytes(),
                S3ServerSideCopy.resolvePartSize(configured, sourceSize)
            );
        }
    }

    /**
     * Repository blob names are generated - a {@code __}/{@code v__} prefix plus a base64url UUID - or Lucene file
     * names. Neither contains a character the encoder alters, so the copy source carries them verbatim.
     */
    public void testGeneratedBlobNamesSurviveEncodingUnchanged() {
        for (String blobName : new String[] {
            "__gX7bNIIBrs0AUNsR2yEG",
            "v__caQ0lbasTf29kBFmUqiI_w",
            "__gX7bNIIBrs0AUNsR2yEG.part12",
            "_0.cfs",
            "segments_7" }) {
            final String key = "base/indices/abc/0/" + blobName;
            assertEquals("my-bucket/" + key, S3ServerSideCopy.encodeCopySource("my-bucket", key));
        }
    }

    /**
     * Source addressing comes entirely from the existing repository: its access point and its {@code base_path}.
     */
    public void testSourceResolvesKeyUnderExistingRepositoryBasePath() {
        final ServerSideCopySource source = new ServerSideCopySource(AP_ARN, SOURCE_BASE_PATH + "/", "798157642085");

        assertEquals(AP_ARN, source.container());
        assertEquals("798157642085", source.expectedContainerOwner());
        assertEquals(SOURCE_BASE_PATH + "/indices/abc/0/snap-1.dat", source.resolveKey("indices/abc/0/snap-1.dat"));
    }

    public void testSourceResolvesKeyWithAndWithoutTrailingSlash() {
        assertEquals("base/snap-1.dat", new ServerSideCopySource("c", "base", null).resolveKey("snap-1.dat"));
        assertEquals("base/snap-1.dat", new ServerSideCopySource("c", "base/", null).resolveKey("snap-1.dat"));
        assertEquals("snap-1.dat", new ServerSideCopySource("c", "", null).resolveKey("snap-1.dat"));
    }

    public void testSourceRequiresContainer() {
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new ServerSideCopySource("", "base", null));
        assertTrue(e.getMessage().contains("source container"));
    }

    /**
     * Server-side copy adds exactly one repository setting.
     */
    public void testSourceAccessPointIsTheOnlyAddedSetting() {
        assertEquals("", S3Repository.SOURCE_ACCESS_POINT_SETTING.get(Settings.EMPTY));
        assertEquals(AP_ARN, S3Repository.SOURCE_ACCESS_POINT_SETTING.get(Settings.builder().put("source_access_point", AP_ARN).build()));
    }

    /**
     * A source repository describes its bucket and base path, and nothing about how a reader should reach it.
     */
    public void testSourceRepositoryDescribesBucketAndBasePath() {
        final S3BlobContainer container = container("", "svc-indexes", "798157642085");
        final ServerSideCopySource source = container.asCopySource();

        assertEquals("svc-indexes", source.container());
        assertEquals("798157642085", source.expectedContainerOwner());
        assertEquals("base/snap-1.dat", source.resolveKey("snap-1.dat"));
    }

    /**
     * The access point belongs only on the destination. On a source it is silently ignored, so a repository carrying
     * one is either mis-registered or being used in the wrong role - both must fail loudly rather than copy from
     * somewhere the operator did not intend.
     */
    public void testSourceRepositoryCarryingAccessPointIsRejected() {
        final S3BlobContainer container = container(AP_ARN, "svc-indexes", "798157642085");

        final IllegalStateException e = expectThrows(IllegalStateException.class, container::asCopySource);
        assertTrue("must name the offending repository", e.getMessage().contains("cs-automated-enc"));
        assertTrue("must name the offending setting", e.getMessage().contains("source_access_point"));
        assertTrue("must say where the setting belongs", e.getMessage().contains("destination"));
    }

    /**
     * The mirror of the above: a destination without an access point has no endpoint through which it can reach a
     * foreign source bucket, so the capability is reported absent.
     */
    public void testDestinationWithoutAccessPointIsNotConfiguredForCopy() {
        assertFalse(container("", "customer-bucket", "").isServerSideCopyConfigured());
        assertTrue(container(AP_ARN, "customer-bucket", "").isServerSideCopyConfigured());
    }

    private static S3BlobContainer container(final String accessPoint, final String bucket, final String expectedOwner) {
        final S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.sourceAccessPoint()).thenReturn(accessPoint);
        when(blobStore.bucket()).thenReturn(bucket);
        when(blobStore.expectedBucketOwner()).thenReturn(expectedOwner);
        when(blobStore.repositoryMetadata()).thenReturn(new RepositoryMetadata("cs-automated-enc", "s3", Settings.EMPTY));
        return new S3BlobContainer(BlobPath.cleanPath().add("base"), blobStore);
    }
}
