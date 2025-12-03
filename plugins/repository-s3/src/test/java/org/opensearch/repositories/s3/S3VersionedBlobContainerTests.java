/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.repositories.s3;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.versioned.VersionedInputStream;
import org.opensearch.test.OpenSearchTestCase;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.core.sync.RequestBody;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class S3VersionedBlobContainerTests extends OpenSearchTestCase {

    public void testWriteBlobWithVersionSuccess() throws IOException {
        S3BlobStore mockBlobStore = mock(S3BlobStore.class);
        AmazonS3Reference mockClientRef = mock(AmazonS3Reference.class);
        software.amazon.awssdk.services.s3.S3Client mockS3Client = mock(software.amazon.awssdk.services.s3.S3Client.class);
        
        when(mockBlobStore.clientReference()).thenReturn(mockClientRef);
        when(mockClientRef.get()).thenReturn(mockS3Client);
        when(mockBlobStore.bucket()).thenReturn("test-bucket");
        
        PutObjectResponse mockResponse = PutObjectResponse.builder().eTag("test-etag").build();
        when(mockS3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenReturn(mockResponse);
        
        S3BlobContainer container = new S3BlobContainer(BlobPath.cleanPath(), mockBlobStore);
        
        VersionedInputStream result = container.writeBlobWithVersion(
            "test-blob", 
            new ByteArrayInputStream("data".getBytes()), 
            4, 
            "expected-etag"
        );
        
        assertEquals("test-etag", result.getVersion());
        verify(mockS3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }

    public void testWriteBlobWithVersionMismatch() throws IOException {
        S3BlobStore mockBlobStore = mock(S3BlobStore.class);
        AmazonS3Reference mockClientRef = mock(AmazonS3Reference.class);
        software.amazon.awssdk.services.s3.S3Client mockS3Client = mock(software.amazon.awssdk.services.s3.S3Client.class);
        
        when(mockBlobStore.clientReference()).thenReturn(mockClientRef);
        when(mockClientRef.get()).thenReturn(mockS3Client);
        when(mockBlobStore.bucket()).thenReturn("test-bucket");
        
        S3Exception s3Exception = (S3Exception) S3Exception.builder().statusCode(412).build();
        when(mockS3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenThrow(s3Exception);
        
        S3BlobContainer container = new S3BlobContainer(BlobPath.cleanPath(), mockBlobStore);
        
        IOException exception = expectThrows(IOException.class, () -> {
            container.writeBlobWithVersion(
                "test-blob", 
                new ByteArrayInputStream("data".getBytes()), 
                4, 
                "wrong-etag"
            );
        });
        
        assertTrue(exception.getMessage().contains("Version mismatch"));
    }

    public void testGetVersion() throws IOException {
        S3BlobStore mockBlobStore = mock(S3BlobStore.class);
        AmazonS3Reference mockClientRef = mock(AmazonS3Reference.class);
        software.amazon.awssdk.services.s3.S3Client mockS3Client = mock(software.amazon.awssdk.services.s3.S3Client.class);
        
        when(mockBlobStore.clientReference()).thenReturn(mockClientRef);
        when(mockClientRef.get()).thenReturn(mockS3Client);
        when(mockBlobStore.bucket()).thenReturn("test-bucket");
        
        HeadObjectResponse mockResponse = HeadObjectResponse.builder().eTag("test-etag").build();
        when(mockS3Client.headObject(any(HeadObjectRequest.class))).thenReturn(mockResponse);
        
        S3BlobContainer container = new S3BlobContainer(BlobPath.cleanPath(), mockBlobStore);
        
        String version = container.getVersion("test-blob");
        
        assertEquals("test-etag", version);
        verify(mockS3Client).headObject(any(HeadObjectRequest.class));
    }
}