package org.opensearch.storage.indexinput;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.opensearch.common.annotation.ExperimentalApi;

import java.nio.file.Path;

/**
 * Class to represent a fetch request for a block of a file.
 */
@ExperimentalApi
public class BlockFetchRequest {

    private final Directory directory;

    private final String fileName;

    private final String blockFileName;

    private final long blockStart;

    private final long blockSize;

    private final Path filePath;

    private BlockFetchRequest(Builder builder) {
        this.fileName = builder.fileName;
        this.blockFileName = builder.blockFileName;
        this.filePath = builder.directory.getDirectory().resolve(blockFileName);
        this.directory = builder.directory;
        this.blockSize = builder.blockSize;
        this.blockStart = builder.blockStart;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Path getFilePath() {
        return filePath;
    }

    public Directory getDirectory() {
        return directory;
    }

    public String getFileName() {
        return fileName;
    }

    public String getBlockFileName() {
        return blockFileName;
    }

    public long getBlockSize() {
        return blockSize;
    }

    public long getBlockStart() {
        return blockStart;
    }

    @Override
    public String toString() {
        return "BlockFetchRequest{"
                + "filePath=" + filePath.toString()
                + ", directory=" + directory.toString()
                + ", fileName='" + fileName
                + ", blockFileName='" + blockFileName
                + ", blockStart=" + blockStart
                + ", blockSize=" + blockSize
                + ", filePath=" + filePath
                + '}';
    }

    /**
     * Builder for BlobFetchRequest
     */
    @ExperimentalApi
    public static final class Builder {
        private FSDirectory directory;
        private String fileName;
        private String blockFileName;
        private long blockSize;
        private long blockStart;

        private Builder() {
        }

        public Builder directory(FSDirectory directory) {
            this.directory = directory;
            return this;
        }

        public Builder fileName(String fileName) {
            this.fileName = fileName;
            return this;
        }

        public Builder blockFileName(String blockFileName) {
            this.blockFileName = blockFileName;
            return this;
        }

        public Builder blockSize(long blockSize) {
            this.blockSize = blockSize;
            return this;
        }

        public Builder blockStart(long blockStart) {
            this.blockStart = blockStart;
            return this;
        }

        public BlockFetchRequest build() {
            return new BlockFetchRequest(this);
        }
    }
}

