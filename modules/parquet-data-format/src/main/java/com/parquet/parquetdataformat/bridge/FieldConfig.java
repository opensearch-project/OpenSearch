/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.bridge;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Field-level configuration that overrides index-level defaults.
 * Null values inherit from index-level configuration.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FieldConfig {
    
    private Integer compressionLevel;
    private String compressionType;
    
    public Integer getCompressionLevel() { return compressionLevel; }
    public void setCompressionLevel(Integer compressionLevel) { this.compressionLevel = compressionLevel; }
    
    public String getCompressionType() { return compressionType; }
    public void setCompressionType(String compressionType) { this.compressionType = compressionType; }
}
