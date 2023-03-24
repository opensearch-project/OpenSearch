package org.opensearch.repositories.s3.async;

import org.opensearch.common.blobstore.stream.write.WritePriority;

public class UploadRequest {
    private final String bucket;
    private final String key;
    private final long contentLength;
    private final long checksum;
    private final WritePriority writePriority;

    public UploadRequest(String bucket, String key, long contentLength, long checksum, WritePriority writePriority) {
        this.bucket = bucket;
        this.key = key;
        this.contentLength = contentLength;
        this.checksum = checksum;
        this.writePriority = writePriority;
    }

    public String getBucket() {
        return bucket;
    }

    public String getKey() {
        return key;
    }

    public long getContentLength() {
        return contentLength;
    }

    public long getChecksum() {
        return checksum;
    }

    public WritePriority getWritePriority() {
        return writePriority;
    }
}
