/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.bridge;

/**
 * Immutable settings passed to the native Rust writer via JNI.
 * The Rust side reads values through the getter methods.
 * All fields are nullable; the native side falls back to defaults when null.
 */
public class NativeSettings {

    private final String indexName;
    private final String compressionType;
    private final Integer compressionLevel;
    private final Long pageSizeBytes;
    private final Integer pageRowLimit;
    private final Long dictSizeBytes;
    private final Long rowGroupSizeBytes;

    private NativeSettings(Builder builder) {
        this.indexName = builder.indexName;
        this.compressionType = builder.compressionType;
        this.compressionLevel = builder.compressionLevel;
        this.pageSizeBytes = builder.pageSizeBytes;
        this.pageRowLimit = builder.pageRowLimit;
        this.dictSizeBytes = builder.dictSizeBytes;
        this.rowGroupSizeBytes = builder.rowGroupSizeBytes;
    }

    public String getIndexName() { return indexName; }
    public String getCompressionType() { return compressionType; }
    public Integer getCompressionLevel() { return compressionLevel; }
    public Long getPageSizeBytes() { return pageSizeBytes; }
    public Integer getPageRowLimit() { return pageRowLimit; }
    public Long getDictSizeBytes() { return dictSizeBytes; }
    public Long getRowGroupSizeBytes() { return rowGroupSizeBytes; }

    public static Builder builder() { return new Builder(); }

    public static class Builder {
        private String indexName;
        private String compressionType;
        private Integer compressionLevel;
        private Long pageSizeBytes;
        private Integer pageRowLimit;
        private Long dictSizeBytes;
        private Long rowGroupSizeBytes;

        public Builder indexName(String v) { this.indexName = v; return this; }
        public Builder compressionType(String v) { this.compressionType = v; return this; }
        public Builder compressionLevel(Integer v) { this.compressionLevel = v; return this; }
        public Builder pageSizeBytes(Long v) { this.pageSizeBytes = v; return this; }
        public Builder pageRowLimit(Integer v) { this.pageRowLimit = v; return this; }
        public Builder dictSizeBytes(Long v) { this.dictSizeBytes = v; return this; }
        public Builder rowGroupSizeBytes(Long v) { this.rowGroupSizeBytes = v; return this; }

        public NativeSettings build() { return new NativeSettings(this); }
    }
}
