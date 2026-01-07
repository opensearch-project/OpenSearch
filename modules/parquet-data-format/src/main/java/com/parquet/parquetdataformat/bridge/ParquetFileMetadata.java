package com.parquet.parquetdataformat.bridge;

/**
 * Represents metadata information for a Parquet file.
 * <p>
 * This class holds the essential metadata extracted from a Parquet file
 * when the writer is closed, providing visibility into the file's characteristics.
 */
public record ParquetFileMetadata(int version, long numRows, String createdBy) {
    /**
     * Constructs a new ParquetFileMetadata instance.
     *
     * @param version   the Parquet format version used
     * @param numRows   the total number of rows in the file
     * @param createdBy the application/library that created the file (can be null)
     */
    public ParquetFileMetadata {
    }

    /**
     * Gets the Parquet format version.
     *
     * @return the version number
     */
    @Override
    public int version() {
        return version;
    }

    /**
     * Gets the total number of rows in the Parquet file.
     *
     * @return the number of rows
     */
    @Override
    public long numRows() {
        return numRows;
    }

    /**
     * Gets information about what created this Parquet file.
     *
     * @return the creator information, or null if not available
     */
    @Override
    public String createdBy() {
        return createdBy;
    }

    @Override
    public String toString() {
        return "ParquetFileMetadata{" +
            "version=" + version +
            ", numRows=" + numRows +
            ", createdBy='" + createdBy + '\'' +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ParquetFileMetadata that = (ParquetFileMetadata) o;

        if (version != that.version) return false;
        if (numRows != that.numRows) return false;
        return createdBy != null ? createdBy.equals(that.createdBy) : that.createdBy == null;
    }

    @Override
    public int hashCode() {
        int result = version;
        result = 31 * result + (int) (numRows ^ (numRows >>> 32));
        result = 31 * result + (createdBy != null ? createdBy.hashCode() : 0);
        return result;
    }
}
