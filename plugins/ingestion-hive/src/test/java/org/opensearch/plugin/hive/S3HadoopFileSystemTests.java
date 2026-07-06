/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.hive;

import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.opensearch.test.OpenSearchTestCase;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Instant;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class S3HadoopFileSystemTests extends OpenSearchTestCase {

    private S3Client s3Client;
    private S3HadoopFileSystem fileSystem;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        s3Client = mock(S3Client.class);
        fileSystem = new S3HadoopFileSystem();
        fileSystem.s3Client = s3Client;
    }

    public void testGetFileStatusReturnsFileWhenObjectExists() throws Exception {
        when(s3Client.headObject(any(HeadObjectRequest.class))).thenReturn(
            HeadObjectResponse.builder().contentLength(42L).lastModified(Instant.ofEpochMilli(1000)).build()
        );

        FileStatus status = fileSystem.getFileStatus(new Path("s3://bucket/data/file.parquet"));

        assertFalse(status.isDirectory());
        assertEquals(42L, status.getLen());
    }

    public void testGetFileStatusReturnsDirectoryWhenPrefixHasObjects() throws Exception {
        when(s3Client.headObject(any(HeadObjectRequest.class))).thenThrow(
            (S3Exception) S3Exception.builder().statusCode(404).message("no such key").build()
        );
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(
            ListObjectsV2Response.builder().contents(S3Object.builder().key("warehouse/db/table/dt=2024-01-01/f.parquet").build()).build()
        );

        FileStatus status = fileSystem.getFileStatus(new Path("s3://bucket/warehouse/db/table/dt=2024-01-01"));

        assertTrue(status.isDirectory());
    }

    public void testGetFileStatusThrowsFileNotFoundWhenNothingExists() {
        when(s3Client.headObject(any(HeadObjectRequest.class))).thenThrow(
            (S3Exception) S3Exception.builder().statusCode(404).message("no such key").build()
        );
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(ListObjectsV2Response.builder().build());

        expectThrows(FileNotFoundException.class, () -> fileSystem.getFileStatus(new Path("s3://bucket/missing/path")));
    }

    public void testGetFileStatusPropagatesAuthErrorsAsIOException() {
        when(s3Client.headObject(any(HeadObjectRequest.class))).thenThrow(
            (S3Exception) S3Exception.builder().statusCode(403).message("access denied").build()
        );

        IOException e = expectThrows(IOException.class, () -> fileSystem.getFileStatus(new Path("s3://bucket/secret/file")));
        assertTrue(e.getMessage(), e.getMessage().contains("Failed to get status"));
    }

    public void testGetFileStatusPropagatesNetworkErrorsAsIOException() {
        when(s3Client.headObject(any(HeadObjectRequest.class))).thenThrow(SdkClientException.create("connection reset"));

        IOException e = expectThrows(IOException.class, () -> fileSystem.getFileStatus(new Path("s3://bucket/data/file.parquet")));
        assertTrue(e.getMessage(), e.getMessage().contains("Failed to get status"));
    }
}
