/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.hive;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Minimal Hadoop FileSystem implementation for S3 using AWS SDK v2.
 * Supports read-only operations needed for Hive ingestion: open, listStatus, getFileStatus.
 * Registered as the implementation for s3://, s3a://, and s3n:// schemes so that
 * Hadoop's HadoopInputFile and FileSystem.get() work transparently with S3 paths.
 */
public class S3HadoopFileSystem extends FileSystem {

    private URI uri;
    private S3Client s3Client;
    private Path workingDir;

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        super.initialize(name, conf);
        this.uri = URI.create(name.getScheme() + "://" + name.getAuthority());
        this.s3Client = S3Client.builder().build();
        this.workingDir = new Path("/");
    }

    @Override
    public URI getUri() {
        return uri;
    }

    @Override
    public String getScheme() {
        return uri != null ? uri.getScheme() : "s3";
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        String bucket = path.toUri().getHost();
        String key = pathToKey(path);

        HeadObjectResponse head = s3Client.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build());
        long contentLength = head.contentLength();

        return new FSDataInputStream(new S3InputStream(s3Client, bucket, key, contentLength));
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        String bucket = path.toUri().getHost();
        String key = pathToKey(path);

        if (key.isEmpty()) {
            return new FileStatus(0, true, 1, 0, 0, path);
        }

        try {
            HeadObjectResponse head = s3Client.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build());
            return new FileStatus(head.contentLength(), false, 1, 0, head.lastModified().toEpochMilli(), path);
        } catch (Exception e) {
            // May be a directory prefix
            return new FileStatus(0, true, 1, 0, 0, path);
        }
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        String bucket = path.toUri().getHost();
        String prefix = pathToKey(path);
        if (!prefix.isEmpty() && !prefix.endsWith("/")) {
            prefix += "/";
        }

        List<FileStatus> statuses = new ArrayList<>();
        ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).delimiter("/");
        ListObjectsV2Response response;
        do {
            response = s3Client.listObjectsV2(requestBuilder.build());
            for (S3Object obj : response.contents()) {
                String key = obj.key();
                if (key.equals(prefix)) continue;
                Path filePath = keyToPath(bucket, key);
                statuses.add(new FileStatus(obj.size(), false, 1, 0, obj.lastModified().toEpochMilli(), filePath));
            }
            for (var commonPrefix : response.commonPrefixes()) {
                Path dirPath = keyToPath(bucket, commonPrefix.prefix());
                statuses.add(new FileStatus(0, true, 1, 0, 0, dirPath));
            }
            requestBuilder.continuationToken(response.nextContinuationToken());
        } while (Boolean.TRUE.equals(response.isTruncated()));

        return statuses.toArray(new FileStatus[0]);
    }

    @Override
    public Path getWorkingDirectory() {
        return workingDir;
    }

    @Override
    public void setWorkingDirectory(Path newDir) {
        this.workingDir = newDir;
    }

    @Override
    public boolean mkdirs(Path path, FsPermission permission) {
        return true;
    }

    @Override
    public boolean rename(Path src, Path dst) {
        throw new UnsupportedOperationException("S3HadoopFileSystem is read-only");
    }

    @Override
    public boolean delete(Path path, boolean recursive) {
        throw new UnsupportedOperationException("S3HadoopFileSystem is read-only");
    }

    @Override
    public FSDataOutputStream create(
        Path path,
        FsPermission permission,
        boolean overwrite,
        int bufferSize,
        short replication,
        long blockSize,
        Progressable progress
    ) {
        throw new UnsupportedOperationException("S3HadoopFileSystem is read-only");
    }

    @Override
    public FSDataOutputStream append(Path path, int bufferSize, Progressable progress) {
        throw new UnsupportedOperationException("S3HadoopFileSystem is read-only");
    }

    @Override
    public void close() throws IOException {
        if (s3Client != null) {
            s3Client.close();
        }
        super.close();
    }

    private static String pathToKey(Path path) {
        String key = path.toUri().getPath();
        if (key.startsWith("/")) {
            key = key.substring(1);
        }
        return key;
    }

    private Path keyToPath(String bucket, String key) {
        return new Path(uri.getScheme() + "://" + bucket + "/" + key);
    }

    /**
     * InputStream for reading S3 objects that supports seeking via range-request re-open.
     */
    private static class S3InputStream extends InputStream implements Seekable, PositionedReadable {
        private final S3Client s3Client;
        private final String bucket;
        private final String key;
        private final long contentLength;
        private ResponseInputStream<GetObjectResponse> stream;
        private long pos;

        S3InputStream(S3Client s3Client, String bucket, String key, long contentLength) {
            this.s3Client = s3Client;
            this.bucket = bucket;
            this.key = key;
            this.contentLength = contentLength;
            this.pos = 0;
        }

        private void openStream() {
            if (stream != null) return;
            String range = "bytes=" + pos + "-" + (contentLength - 1);
            stream = s3Client.getObject(GetObjectRequest.builder().bucket(bucket).key(key).range(range).build());
        }

        @Override
        public int read() throws IOException {
            openStream();
            int b = stream.read();
            if (b >= 0) pos++;
            return b;
        }

        @Override
        public int read(byte[] buf, int off, int len) throws IOException {
            openStream();
            int n = stream.read(buf, off, len);
            if (n > 0) pos += n;
            return n;
        }

        @Override
        public void seek(long newPos) throws IOException {
            if (newPos != pos) {
                closeStream();
                pos = newPos;
            }
        }

        @Override
        public long getPos() {
            return pos;
        }

        @Override
        public boolean seekToNewSource(long targetPos) {
            return false;
        }

        @Override
        public int read(long position, byte[] buffer, int offset, int length) throws IOException {
            String range = "bytes=" + position + "-" + (position + length - 1);
            try (
                ResponseInputStream<GetObjectResponse> resp = s3Client.getObject(
                    GetObjectRequest.builder().bucket(bucket).key(key).range(range).build()
                )
            ) {
                int totalRead = 0;
                while (totalRead < length) {
                    int n = resp.read(buffer, offset + totalRead, length - totalRead);
                    if (n < 0) break;
                    totalRead += n;
                }
                return totalRead == 0 ? -1 : totalRead;
            }
        }

        @Override
        public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
            int totalRead = 0;
            while (totalRead < length) {
                int n = read(position + totalRead, buffer, offset + totalRead, length - totalRead);
                if (n < 0) throw new IOException("Unexpected EOF at position " + (position + totalRead));
                totalRead += n;
            }
        }

        @Override
        public void readFully(long position, byte[] buffer) throws IOException {
            readFully(position, buffer, 0, buffer.length);
        }

        @Override
        public void close() throws IOException {
            closeStream();
        }

        private void closeStream() throws IOException {
            if (stream != null) {
                stream.close();
                stream = null;
            }
        }
    }
}
