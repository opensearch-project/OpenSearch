/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest;
import software.amazon.awssdk.services.s3.model.UploadPartCopyResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.Nullable;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.secure_sm.AccessController;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;

/**
 * Performs server-side object copies between two S3 locations that may be in different accounts.
 *
 * <p>Bytes never enter this process. Both paths - {@code CopyObject} and multipart {@code UploadPartCopy} - instruct
 * S3 to move the data internally.
 *
 * <p>The source and destination are supplied separately and come from different places by design: the source
 * addresses an existing service-owned repository, while the destination addresses the repository the customer
 * registered. See {@link S3BlobContainer#copyBlobFrom}.
 *
 * <p>No encryption headers are set on the destination. The destination bucket belongs to the customer and its default
 * encryption governs the written object; requesting encryption on their behalf would override a posture we do not own.
 *
 * @opensearch.internal
 */
final class S3ServerSideCopy {

    private static final Logger logger = LogManager.getLogger(S3ServerSideCopy.class);

    /** Hard S3 limit for a single {@code CopyObject}. Above this, multipart copy is mandatory. */
    static final ByteSizeValue MAX_SINGLE_COPY_SIZE = new ByteSizeValue(5, ByteSizeUnit.GB);

    /** Hard S3 limit on parts in one multipart upload. */
    static final int MAX_PARTS = 10_000;

    private S3ServerSideCopy() {}

    /**
     * Copies one object from a source location to a destination location.
     *
     * @param client                   client whose identity holds read on the source and write on the destination
     * @param sourceContainer          source bucket, access point alias, or access point ARN - from the existing repository
     * @param sourceKey                full source key, already including the source repository's base path
     * @param sourceSize               size of the source object in bytes
     * @param sourceExpectedOwner      account expected to own the source, or {@code null} to skip the precondition
     * @param destinationBucket        destination bucket - from the repository registration
     * @param destinationKey           full destination key, already including the registered repository's base path
     * @param destinationExpectedOwner account expected to own the destination, or {@code null} to skip the precondition
     * @param configuredPartSize       requested multipart part size; raised if needed to respect the part limit
     */
    static void copy(
        final S3Client client,
        final String sourceContainer,
        final String sourceKey,
        final long sourceSize,
        @Nullable final String sourceExpectedOwner,
        final String destinationBucket,
        final String destinationKey,
        @Nullable final String destinationExpectedOwner,
        final long configuredPartSize
    ) throws IOException {
        final String copySource = encodeCopySource(sourceContainer, sourceKey);

        try {
            if (sourceSize <= MAX_SINGLE_COPY_SIZE.getBytes()) {
                singleCopy(client, copySource, sourceExpectedOwner, destinationBucket, destinationKey, destinationExpectedOwner);
            } else {
                multipartCopy(
                    client,
                    copySource,
                    sourceSize,
                    sourceExpectedOwner,
                    destinationBucket,
                    destinationKey,
                    destinationExpectedOwner,
                    configuredPartSize
                );
            }
        } catch (final NoSuchKeyException e) {
            final NoSuchFileException nsfe = new NoSuchFileException(sourceKey);
            nsfe.initCause(e);
            throw nsfe;
        } catch (final Exception e) {
            throw new IOException(
                "server-side copy of ["
                    + sourceContainer
                    + "/"
                    + sourceKey
                    + "] to ["
                    + destinationBucket
                    + "/"
                    + destinationKey
                    + "] failed",
                e
            );
        }
    }

    private static void singleCopy(
        final S3Client client,
        final String copySource,
        @Nullable final String sourceExpectedOwner,
        final String destinationBucket,
        final String destinationKey,
        @Nullable final String destinationExpectedOwner
    ) {
        final CopyObjectRequest.Builder request = CopyObjectRequest.builder()
            .copySource(copySource)
            .destinationBucket(destinationBucket)
            .destinationKey(destinationKey);
        if (Strings.hasText(sourceExpectedOwner)) {
            request.expectedSourceBucketOwner(sourceExpectedOwner);
        }
        if (Strings.hasText(destinationExpectedOwner)) {
            request.expectedBucketOwner(destinationExpectedOwner);
        }
        AccessController.doPrivileged(() -> client.copyObject(request.build()));
        logger.debug("copied [{}] to [{}/{}] in a single request", copySource, destinationBucket, destinationKey);
    }

    private static void multipartCopy(
        final S3Client client,
        final String copySource,
        final long sourceSize,
        @Nullable final String sourceExpectedOwner,
        final String destinationBucket,
        final String destinationKey,
        @Nullable final String destinationExpectedOwner,
        final long configuredPartSize
    ) throws IOException {
        final long partSize = resolvePartSize(configuredPartSize, sourceSize);
        final int partCount = Math.toIntExact((sourceSize + partSize - 1) / partSize);

        final CreateMultipartUploadRequest.Builder create = CreateMultipartUploadRequest.builder()
            .bucket(destinationBucket)
            .key(destinationKey);
        if (Strings.hasText(destinationExpectedOwner)) {
            create.expectedBucketOwner(destinationExpectedOwner);
        }

        final CreateMultipartUploadResponse created = AccessController.doPrivileged(() -> client.createMultipartUpload(create.build()));
        final String uploadId = created.uploadId();

        boolean completed = false;
        try {
            final List<CompletedPart> parts = new ArrayList<>(partCount);
            for (int partNumber = 1; partNumber <= partCount; partNumber++) {
                final long firstByte = (partNumber - 1L) * partSize;
                final long lastByte = Math.min(firstByte + partSize, sourceSize) - 1;

                final UploadPartCopyRequest.Builder part = UploadPartCopyRequest.builder()
                    .copySource(copySource)
                    .destinationBucket(destinationBucket)
                    .destinationKey(destinationKey)
                    .uploadId(uploadId)
                    .partNumber(partNumber)
                    .copySourceRange("bytes=" + firstByte + "-" + lastByte);
                if (Strings.hasText(sourceExpectedOwner)) {
                    part.expectedSourceBucketOwner(sourceExpectedOwner);
                }
                if (Strings.hasText(destinationExpectedOwner)) {
                    part.expectedBucketOwner(destinationExpectedOwner);
                }

                final int currentPart = partNumber;
                final UploadPartCopyResponse response = AccessController.doPrivileged(() -> client.uploadPartCopy(part.build()));
                parts.add(CompletedPart.builder().partNumber(currentPart).eTag(response.copyPartResult().eTag()).build());
            }

            final CompleteMultipartUploadRequest.Builder complete = CompleteMultipartUploadRequest.builder()
                .bucket(destinationBucket)
                .key(destinationKey)
                .uploadId(uploadId)
                .multipartUpload(CompletedMultipartUpload.builder().parts(parts).build());
            if (Strings.hasText(destinationExpectedOwner)) {
                complete.expectedBucketOwner(destinationExpectedOwner);
            }
            AccessController.doPrivileged(() -> client.completeMultipartUpload(complete.build()));
            completed = true;
            logger.debug("copied [{}] to [{}/{}] in {} parts", copySource, destinationBucket, destinationKey, partCount);
        } finally {
            if (completed == false) {
                // An incomplete multipart upload is billable and invisible in a listing, so abandoning it silently
                // would leak storage in the customer's bucket.
                abortQuietly(client, destinationBucket, destinationKey, destinationExpectedOwner, uploadId);
            }
        }
    }

    private static void abortQuietly(
        final S3Client client,
        final String destinationBucket,
        final String destinationKey,
        @Nullable final String destinationExpectedOwner,
        final String uploadId
    ) {
        try {
            final AbortMultipartUploadRequest.Builder abort = AbortMultipartUploadRequest.builder()
                .bucket(destinationBucket)
                .key(destinationKey)
                .uploadId(uploadId);
            if (Strings.hasText(destinationExpectedOwner)) {
                abort.expectedBucketOwner(destinationExpectedOwner);
            }
            AccessController.doPrivileged(() -> client.abortMultipartUpload(abort.build()));
        } catch (final Exception e) {
            logger.warn(
                "failed to abort multipart copy [{}] for [{}/{}]; it may linger as a billable incomplete upload",
                uploadId,
                destinationBucket,
                destinationKey,
                e
            );
        }
    }

    /**
     * Chooses a part size that both respects the configured value and keeps the part count inside the provider limit.
     * A large object with a small configured part size would otherwise fail late, after the upload has been created.
     */
    static long resolvePartSize(final long configuredPartSize, final long sourceSize) {
        final long minimumForPartLimit = (sourceSize + MAX_PARTS - 1) / MAX_PARTS;
        return Math.max(configuredPartSize, minimumForPartLimit);
    }

    /**
     * Builds the {@code x-amz-copy-source} value.
     *
     * <p>Two encodings exist and the choice is not cosmetic. A bucket source is {@code bucket/key}. An access point
     * source is the ARN form {@code <access-point-arn>/object/<key>}, which is what makes the access point policy -
     * and therefore its prefix confinement - apply to the read. Nothing in the AWS SDK constructs the latter, so it
     * is built here.
     */
    static String encodeCopySource(final String sourceContainer, final String sourceKey) {
        final String encodedKey = encodeKey(sourceKey);
        if (isAccessPointArn(sourceContainer)) {
            return sourceContainer + "/object/" + encodedKey;
        }
        return sourceContainer + "/" + encodedKey;
    }

    /**
     * Encodes only the key, never the container.
     *
     * <p>Encoding the container would percent-encode the colons of an access point ARN into {@code %3A}, which S3
     * does not accept as a copy source. Path separators are restored because S3 parses them, and {@code +} is
     * rewritten to {@code %20} because S3 does not treat {@code +} as a space here.
     */
    private static String encodeKey(final String key) {
        return URLEncoder.encode(key, StandardCharsets.UTF_8).replace("%2F", "/").replace("+", "%20");
    }

    /**
     * Access point <em>aliases</em> are deliberately not treated as ARNs: an alias is usable wherever a bucket name
     * is, so it takes the {@code bucket/key} form.
     */
    private static boolean isAccessPointArn(@Nullable final String sourceContainer) {
        return sourceContainer != null && sourceContainer.startsWith("arn:") && sourceContainer.contains(":accesspoint");
    }
}
