/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.MetadataDirective;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.services.s3.model.StorageClass;
import software.amazon.awssdk.services.s3.model.TaggingDirective;

import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.ServerSideCopySource;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.NoSuchFileException;

import org.mockito.ArgumentCaptor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class S3ServerSideCopyTests extends OpenSearchTestCase {

    private static final String KMS_KEY_ARN = "arn:aws:kms:eu-west-1:123456789012:key/11111111-2222-3333-4444-555555555555";

    private static final String AP_ARN = "arn:aws:s3:eu-west-1:123456789012:accesspoint/source-ap";

    /** Shape of a real source path: several path segments, as a repository base path tends to have. */
    private static final String SOURCE_BASE_PATH = "base/path/with/several/segments";

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
     * An Outposts access point carries the access point inside the resource portion as
     * {@code outpost/<id>/accesspoint/<name>}, so a substring search for {@code ":accesspoint"} misses it and the ARN
     * would be addressed as though it were a bucket name.
     */
    public void testOutpostsAccessPointArnUsesObjectForm() {
        final String outpostArn = "arn:aws:s3-outposts:us-west-2:123456789012:outpost/op-01234567890123456/accesspoint/source-ap";

        assertTrue(S3ServerSideCopy.isAccessPointArn(outpostArn));
        assertEquals(outpostArn + "/object/base/snap-1.dat", S3ServerSideCopy.encodeCopySource(outpostArn, "base/snap-1.dat"));
    }

    public void testNonAccessPointArnsAreNotTreatedAsAccessPoints() {
        // A bucket ARN, an Outposts bucket ARN, another service, and a malformed ARN.
        assertFalse(S3ServerSideCopy.isAccessPointArn("arn:aws:s3:::my-bucket"));
        assertFalse(S3ServerSideCopy.isAccessPointArn("arn:aws:s3-outposts:us-west-2:123456789012:outpost/op-0123/bucket/my-bucket"));
        assertFalse(S3ServerSideCopy.isAccessPointArn("arn:aws:sqs:us-west-2:123456789012:accesspoint/not-s3"));
        assertFalse(S3ServerSideCopy.isAccessPointArn("arn:aws:s3:us-west-2"));
        assertFalse(S3ServerSideCopy.isAccessPointArn("my-bucket"));
        assertFalse(S3ServerSideCopy.isAccessPointArn(null));
    }

    /**
     * An alias is usable wherever a bucket name is, so it must NOT be rewritten into the ARN object form.
     */
    public void testAccessPointAliasIsTreatedAsBucket() {
        final String alias = "source-ap-abcdefghijklmnopqrstuvwxyz012345-s3alias";
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
        final ServerSideCopySource source = new ServerSideCopySource(AP_ARN, SOURCE_BASE_PATH + "/", "123456789012");

        assertEquals(AP_ARN, source.container());
        assertEquals("123456789012", source.expectedContainerOwner());
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
        final S3BlobContainer container = container("", "source-bucket", "123456789012");
        final ServerSideCopySource source = container.asCopySource();

        assertEquals("source-bucket", source.container());
        assertEquals("123456789012", source.expectedContainerOwner());
        assertEquals("base/snap-1.dat", source.resolveKey("snap-1.dat"));
    }

    /**
     * The access point belongs only on the destination. On a source it is silently ignored, so a repository carrying
     * one is either mis-registered or being used in the wrong role - both must fail loudly rather than copy from
     * somewhere the operator did not intend.
     */
    public void testSourceRepositoryCarryingAccessPointIsRejected() {
        final S3BlobContainer container = container(AP_ARN, "source-bucket", "123456789012");

        final IllegalStateException e = expectThrows(IllegalStateException.class, container::asCopySource);
        assertTrue("must name the offending repository", e.getMessage().contains("source-repo"));
        assertTrue("must name the offending setting", e.getMessage().contains("source_access_point"));
        assertTrue("must say where the setting belongs", e.getMessage().contains("destination"));
    }

    /**
     * The mirror of the above: a destination without an access point has no endpoint through which it can reach a
     * foreign source bucket, so the capability is reported absent.
     */
    public void testDestinationWithoutAccessPointIsNotConfiguredForCopy() {
        assertFalse(container("", "destination-bucket", "").isServerSideCopyConfigured());
        assertTrue(container(AP_ARN, "destination-bucket", "").isServerSideCopyConfigured());
    }

    private static S3BlobContainer container(final String accessPoint, final String bucket, final String expectedOwner) {
        return new S3BlobContainer(BlobPath.cleanPath().add("base"), blobStore(accessPoint, bucket, expectedOwner, ""));
    }

    /**
     * A destination blob store carrying the settings a copied object must honour.
     *
     * @param sseType {@code ""} for no server-side encryption, else an {@link ServerSideEncryption} value
     */
    private static S3BlobStore blobStore(final String accessPoint, final String bucket, final String expectedOwner, final String sseType) {
        final S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.sourceAccessPoint()).thenReturn(accessPoint);
        when(blobStore.bucket()).thenReturn(bucket);
        when(blobStore.expectedBucketOwner()).thenReturn(expectedOwner);
        when(blobStore.repositoryMetadata()).thenReturn(new RepositoryMetadata("source-repo", "s3", Settings.EMPTY));
        when(blobStore.getStorageClass()).thenReturn(StorageClass.INTELLIGENT_TIERING);
        when(blobStore.getCannedACL()).thenReturn(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL);
        when(blobStore.getStatsMetricPublisher()).thenReturn(new StatsMetricPublisher());
        when(blobStore.bufferSizeInBytes()).thenReturn(new ByteSizeValue(16, ByteSizeUnit.MB).getBytes());
        when(blobStore.serverSideEncryptionType()).thenReturn(sseType);
        when(blobStore.serverSideEncryptionKmsKey()).thenReturn(KMS_KEY_ARN);
        when(blobStore.serverSideEncryptionBucketKey()).thenReturn(true);
        when(blobStore.serverSideEncryptionEncryptionContext()).thenReturn(null);
        return blobStore;
    }

    private static CopyObjectRequest captureSingleCopy(final S3BlobStore blobStore, final String sourceETag) throws IOException {
        final S3Client client = mock(S3Client.class);
        when(client.copyObject(any(CopyObjectRequest.class))).thenReturn(CopyObjectResponse.builder().build());

        S3ServerSideCopy.copy(client, blobStore, AP_ARN, "src/base/_0.cfs__u1", 1024L, sourceETag, "123456789012", "base/__dst", null);

        final ArgumentCaptor<CopyObjectRequest> captor = ArgumentCaptor.forClass(CopyObjectRequest.class);
        verify(client).copyObject(captor.capture());
        return captor.getValue();
    }

    /**
     * A copied object must be stored the way an uploaded one would be. Without this the destination repository's
     * storage class and ACL are silently dropped and the object lands under the bucket defaults.
     */
    public void testCopyAppliesDestinationStorageClassAndAcl() throws IOException {
        final CopyObjectRequest request = captureSingleCopy(blobStore(AP_ARN, "destination-bucket", "210987654321", ""), "\"etag-1\"");

        assertEquals("destination-bucket", request.destinationBucket());
        assertEquals("base/__dst", request.destinationKey());
        assertEquals(StorageClass.INTELLIGENT_TIERING, request.storageClass());
        assertEquals(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL, request.acl());
    }

    /**
     * A repository configured for SSE-KMS would otherwise get copied objects under the bucket default key while its
     * uploads used the configured one.
     */
    public void testCopyAppliesServerSideEncryptionSettings() throws IOException {
        final S3BlobStore blobStore = blobStore(AP_ARN, "destination-bucket", "", ServerSideEncryption.AWS_KMS.toString());
        final CopyObjectRequest request = captureSingleCopy(blobStore, "\"etag-1\"");

        assertEquals(ServerSideEncryption.AWS_KMS, request.serverSideEncryption());
        assertEquals(KMS_KEY_ARN, request.ssekmsKeyId());
        assertEquals(Boolean.TRUE, request.bucketKeyEnabled());
    }

    public void testCopyAppliesAes256WhenConfigured() throws IOException {
        final S3BlobStore blobStore = blobStore(AP_ARN, "destination-bucket", "", ServerSideEncryption.AES256.toString());

        assertEquals(ServerSideEncryption.AES256, captureSingleCopy(blobStore, "\"etag-1\"").serverSideEncryption());
    }

    /**
     * {@code CopyObject} copies user metadata and tags by default while the multipart path cannot, so both are set
     * explicitly to keep the two paths agreeing. Copying tags would also need {@code s3:GetObjectTagging}.
     */
    public void testCopyDoesNotCarryOverSourceMetadataOrTags() throws IOException {
        final CopyObjectRequest request = captureSingleCopy(blobStore(AP_ARN, "destination-bucket", "", ""), "\"etag-1\"");

        assertEquals(MetadataDirective.REPLACE, request.metadataDirective());
        assertEquals(TaggingDirective.REPLACE, request.taggingDirective());
    }

    /**
     * The size used to choose single versus multipart comes from a separate HEAD, so the copy is pinned to the
     * version that HEAD saw.
     */
    public void testCopyPinsTheSourceVersionItSized() throws IOException {
        assertEquals("\"etag-1\"", captureSingleCopy(blobStore(AP_ARN, "destination-bucket", "", ""), "\"etag-1\"").copySourceIfMatch());
    }

    public void testCopyOmitsPreconditionWhenSourceETagIsUnknown() throws IOException {
        assertNull(captureSingleCopy(blobStore(AP_ARN, "destination-bucket", "", ""), null).copySourceIfMatch());
    }

    public void testCopyCarriesBothOwnershipGuards() throws IOException {
        final CopyObjectRequest request = captureSingleCopy(blobStore(AP_ARN, "destination-bucket", "210987654321", ""), "\"etag-1\"");

        assertEquals("123456789012", request.expectedSourceBucketOwner());
        assertEquals("210987654321", request.expectedBucketOwner());
    }

    /**
     * Without a metric publisher a copy is invisible in repository stats, so the bytes appear to move for free.
     */
    public void testCopyReportsToRepositoryStats() throws IOException {
        final CopyObjectRequest request = captureSingleCopy(blobStore(AP_ARN, "destination-bucket", "", ""), "\"etag-1\"");

        assertFalse("copy must publish metrics", request.overrideConfiguration().orElseThrow().metricPublishers().isEmpty());
    }

    /**
     * A source that disappeared must read as a missing file rather than a generic copy failure, so callers can treat
     * it as "nothing to copy".
     */
    public void testMissingSourceSurfacesAsNoSuchFile() {
        final S3Client client = mock(S3Client.class);
        when(client.copyObject(any(CopyObjectRequest.class))).thenThrow(NoSuchKeyException.builder().message("gone").build());

        expectThrows(
            NoSuchFileException.class,
            () -> S3ServerSideCopy.copy(
                client,
                blobStore(AP_ARN, "destination-bucket", "", ""),
                AP_ARN,
                "src/base/_0.cfs__u1",
                1024L,
                "\"etag-1\"",
                "123456789012",
                "base/__dst",
                null
            )
        );
    }

    /**
     * Any other provider failure is wrapped, and must name both ends so the operator can tell which grant is wrong.
     */
    public void testOtherCopyFailuresAreWrappedWithBothEnds() {
        final S3Client client = mock(S3Client.class);
        when(client.copyObject(any(CopyObjectRequest.class))).thenThrow(S3Exception.builder().message("Access Denied").build());

        final IOException e = expectThrows(
            IOException.class,
            () -> S3ServerSideCopy.copy(
                client,
                blobStore(AP_ARN, "destination-bucket", "", ""),
                AP_ARN,
                "src/base/_0.cfs__u1",
                1024L,
                "\"etag-1\"",
                "123456789012",
                "base/__dst",
                null
            )
        );
        assertTrue(e.getMessage(), e.getMessage().contains(AP_ARN));
        assertTrue(e.getMessage(), e.getMessage().contains("destination-bucket/base/__dst"));
    }
}
