/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.hive;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;
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
import org.opensearch.secure_sm.AccessController;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Minimal Hadoop FileSystem implementation for S3 using AWS SDK v2.
 * Supports read-only operations needed for Hive ingestion: open, listStatus, getFileStatus.
 * Registered as the implementation for s3://, s3a://, and s3n:// schemes so that
 * Hadoop's HadoopInputFile and FileSystem.get() work transparently with S3 paths.
 */
public class S3HadoopFileSystem extends FileSystem {

    URI uri;
    S3Client s3Client;
    private Path workingDir;

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        super.initialize(name, conf);
        this.uri = URI.create(name.getScheme() + "://" + name.getAuthority());
        this.s3Client = createS3Client(conf);
        this.workingDir = new Path("/");
    }

    /**
     * Builds an S3 client honoring the standard fs.s3a.* keys from the Hadoop
     * configuration, so that {@code hadoop_config.fs.s3a.*} ingestion parameters
     * take effect. Keys that are not set fall back to the AWS SDK default
     * provider chains (environment, instance profile, etc.).
     *
     * Supported keys: fs.s3a.endpoint, fs.s3a.endpoint.region,
     * fs.s3a.path.style.access, fs.s3a.access.key, fs.s3a.secret.key.
     */
    static S3Client createS3Client(Configuration conf) {
        S3ClientBuilder builder = S3Client.builder();
        String endpoint = conf.getTrimmed("fs.s3a.endpoint", "");
        if (endpoint.isEmpty() == false) {
            // s3a convention allows an endpoint without a scheme; default to https
            String endpointUri = endpoint.contains("://") ? endpoint : "https://" + endpoint;
            builder.endpointOverride(URI.create(endpointUri));
        }
        String region = conf.getTrimmed("fs.s3a.endpoint.region", "");
        if (region.isEmpty() == false) {
            builder.region(Region.of(region));
        }
        if (conf.getBoolean("fs.s3a.path.style.access", false)) {
            // Required by most S3-compatible stores (e.g., MinIO) where
            // virtual-hosted-style bucket DNS is not available.
            builder.forcePathStyle(true);
        }
        String accessKey = conf.getTrimmed("fs.s3a.access.key", "");
        String secretKey = conf.getTrimmed("fs.s3a.secret.key", "");
        if (accessKey.isEmpty() == false && secretKey.isEmpty() == false) {
            builder.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)));
        }
        // Building the client reads AWS profile files (~/.aws/config, ~/.aws/credentials)
        // through the default provider chains; run privileged so the plugin's own
        // FilePermission applies regardless of unprivileged callers on the stack.
        return AccessController.doPrivileged((Supplier<S3Client>) builder::build);
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
        } catch (S3Exception e) {
            if (e.statusCode() != 404) {
                throw new IOException("Failed to get status of s3://" + bucket + "/" + key, e);
            }
            // No object at this key: it may still be a directory prefix. Probe below.
        } catch (SdkException e) {
            throw new IOException("Failed to get status of s3://" + bucket + "/" + key, e);
        }

        String prefix = key.endsWith("/") ? key : key + "/";
        ListObjectsV2Response probe;
        try {
            probe = s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).maxKeys(1).build());
        } catch (SdkException e) {
            throw new IOException("Failed to get status of s3://" + bucket + "/" + key, e);
        }
        // contents() is never null in SDK v2; keyCount() is a nullable Integer that
        // S3-compatible stores may omit, so do not rely on it.
        if (probe.contents().isEmpty() == false) {
            return new FileStatus(0, true, 1, 0, 0, path);
        }
        throw new FileNotFoundException("No such file or directory: s3://" + bucket + "/" + key);
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
            if (Boolean.TRUE.equals(response.isTruncated()) == false || response.nextContinuationToken() == null) {
                // The null-token check guards against S3-compatible stores that
                // report a truncated listing without a continuation token, which
                // would otherwise loop forever re-listing the first page.
                break;
            }
            requestBuilder.continuationToken(response.nextContinuationToken());
        } while (true);

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
            if (pos >= contentLength) {
                // At or past EOF: a range like "bytes=N-(N-1)" is invalid and S3
                // rejects it with 416 instead of signaling EOF. Leave the stream
                // unopened; the read methods report EOF when the stream is null.
                return;
            }
            String range = "bytes=" + pos + "-" + (contentLength - 1);
            stream = s3Client.getObject(GetObjectRequest.builder().bucket(bucket).key(key).range(range).build());
        }

        @Override
        public int read() throws IOException {
            openStream();
            if (stream == null) return -1;
            int b = stream.read();
            if (b >= 0) pos++;
            return b;
        }

        @Override
        public int read(byte[] buf, int off, int len) throws IOException {
            if (len == 0) return 0;
            openStream();
            if (stream == null) return -1;
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
            if (length == 0) return 0;
            if (position >= contentLength) return -1;
            // Clamp the end of the range; a start at or past EOF or an inverted
            // range would be rejected by S3 with 416 instead of signaling EOF.
            long endInclusive = Math.min(position + length - 1, contentLength - 1);
            String range = "bytes=" + position + "-" + endInclusive;
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
