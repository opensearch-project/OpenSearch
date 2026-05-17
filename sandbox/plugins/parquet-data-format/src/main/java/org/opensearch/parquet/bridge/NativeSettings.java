/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.bridge;

import java.util.Collections;
import java.util.Map;

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
    private final Boolean bloomFilterEnabled;
    private final Double bloomFilterFpp;
    private final Long bloomFilterNdv;
    private final Long sortInMemoryThresholdBytes;
    private final Integer sortBatchSize;
    private final Integer rowGroupMaxRows;
    private final Long rowGroupMaxBytes;
    private final Integer mergeBatchSize;
    private final Integer mergeRayonThreads;
    private final Integer mergeIoThreads;
    private final Map<String, String> fieldEncodings;
    private final Map<String, String> fieldCompressions;
    private final Map<String, String> typeEncodings;
    private final Map<String, String> typeCompressions;

    private NativeSettings(Builder builder) {
        this.indexName = builder.indexName;
        this.compressionType = builder.compressionType;
        this.compressionLevel = builder.compressionLevel;
        this.pageSizeBytes = builder.pageSizeBytes;
        this.pageRowLimit = builder.pageRowLimit;
        this.dictSizeBytes = builder.dictSizeBytes;
        this.bloomFilterEnabled = builder.bloomFilterEnabled;
        this.bloomFilterFpp = builder.bloomFilterFpp;
        this.bloomFilterNdv = builder.bloomFilterNdv;
        this.sortInMemoryThresholdBytes = builder.sortInMemoryThresholdBytes;
        this.sortBatchSize = builder.sortBatchSize;
        this.rowGroupMaxRows = builder.rowGroupMaxRows;
        this.rowGroupMaxBytes = builder.rowGroupMaxBytes;
        this.mergeBatchSize = builder.mergeBatchSize;
        this.mergeRayonThreads = builder.mergeRayonThreads;
        this.mergeIoThreads = builder.mergeIoThreads;
        this.fieldEncodings = builder.fieldEncodings != null ? Collections.unmodifiableMap(builder.fieldEncodings) : Collections.emptyMap();
        this.fieldCompressions = builder.fieldCompressions != null
            ? Collections.unmodifiableMap(builder.fieldCompressions)
            : Collections.emptyMap();
        this.typeEncodings = builder.typeEncodings != null ? Collections.unmodifiableMap(builder.typeEncodings) : Collections.emptyMap();
        this.typeCompressions = builder.typeCompressions != null
            ? Collections.unmodifiableMap(builder.typeCompressions)
            : Collections.emptyMap();
    }

    public String getIndexName() {
        return indexName;
    }

    public String getCompressionType() {
        return compressionType;
    }

    public Integer getCompressionLevel() {
        return compressionLevel;
    }

    public Long getPageSizeBytes() {
        return pageSizeBytes;
    }

    public Integer getPageRowLimit() {
        return pageRowLimit;
    }

    public Long getDictSizeBytes() {
        return dictSizeBytes;
    }

    public Boolean getBloomFilterEnabled() {
        return bloomFilterEnabled;
    }

    public Double getBloomFilterFpp() {
        return bloomFilterFpp;
    }

    public Long getBloomFilterNdv() {
        return bloomFilterNdv;
    }

    public Long getSortInMemoryThresholdBytes() {
        return sortInMemoryThresholdBytes;
    }

    public Integer getSortBatchSize() {
        return sortBatchSize;
    }

    public Integer getRowGroupMaxRows() {
        return rowGroupMaxRows;
    }

    public Long getRowGroupMaxBytes() {
        return rowGroupMaxBytes;
    }

    public Integer getMergeBatchSize() {
        return mergeBatchSize;
    }

    public Integer getMergeRayonThreads() {
        return mergeRayonThreads;
    }

    public Integer getMergeIoThreads() {
        return mergeIoThreads;
    }

    public Map<String, String> getFieldEncodings() {
        return fieldEncodings;
    }

    public Map<String, String> getFieldCompressions() {
        return fieldCompressions;
    }

    public Map<String, String> getTypeEncodings() {
        return typeEncodings;
    }

    public Map<String, String> getTypeCompressions() {
        return typeCompressions;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String indexName;
        private String compressionType;
        private Integer compressionLevel;
        private Long pageSizeBytes;
        private Integer pageRowLimit;
        private Long dictSizeBytes;
        private Boolean bloomFilterEnabled;
        private Double bloomFilterFpp;
        private Long bloomFilterNdv;
        private Long sortInMemoryThresholdBytes;
        private Integer sortBatchSize;
        private Integer rowGroupMaxRows;
        private Long rowGroupMaxBytes;
        private Integer mergeBatchSize;
        private Integer mergeRayonThreads;
        private Integer mergeIoThreads;
        private Map<String, String> fieldEncodings;
        private Map<String, String> fieldCompressions;
        private Map<String, String> typeEncodings;
        private Map<String, String> typeCompressions;

        public Builder indexName(String v) {
            this.indexName = v;
            return this;
        }

        public Builder compressionType(String v) {
            this.compressionType = v;
            return this;
        }

        public Builder compressionLevel(Integer v) {
            this.compressionLevel = v;
            return this;
        }

        public Builder pageSizeBytes(Long v) {
            this.pageSizeBytes = v;
            return this;
        }

        public Builder pageRowLimit(Integer v) {
            this.pageRowLimit = v;
            return this;
        }

        public Builder dictSizeBytes(Long v) {
            this.dictSizeBytes = v;
            return this;
        }

        public Builder bloomFilterEnabled(Boolean v) {
            this.bloomFilterEnabled = v;
            return this;
        }

        public Builder bloomFilterFpp(Double v) {
            this.bloomFilterFpp = v;
            return this;
        }

        public Builder bloomFilterNdv(Long v) {
            this.bloomFilterNdv = v;
            return this;
        }

        public Builder sortInMemoryThresholdBytes(Long v) {
            this.sortInMemoryThresholdBytes = v;
            return this;
        }

        public Builder sortBatchSize(Integer v) {
            this.sortBatchSize = v;
            return this;
        }

        public Builder rowGroupMaxRows(Integer v) {
            this.rowGroupMaxRows = v;
            return this;
        }

        public Builder rowGroupMaxBytes(Long v) {
            this.rowGroupMaxBytes = v;
            return this;
        }

        public Builder mergeBatchSize(Integer v) {
            this.mergeBatchSize = v;
            return this;
        }

        public Builder mergeRayonThreads(Integer v) {
            this.mergeRayonThreads = v;
            return this;
        }

        public Builder mergeIoThreads(Integer v) {
            this.mergeIoThreads = v;
            return this;
        }

        public Builder fieldEncodings(Map<String, String> v) {
            this.fieldEncodings = v;
            return this;
        }

        public Builder fieldCompressions(Map<String, String> v) {
            this.fieldCompressions = v;
            return this;
        }

        public Builder typeEncodings(Map<String, String> v) {
            this.typeEncodings = v;
            return this;
        }

        public Builder typeCompressions(Map<String, String> v) {
            this.typeCompressions = v;
            return this;
        }

        public NativeSettings build() {
            return new NativeSettings(this);
        }
    }
}
