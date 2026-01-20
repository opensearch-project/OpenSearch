/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.bridge;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

/**
 * Index-level configuration for Parquet writer.
 * Field-level configs override these defaults.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class WriterConfig {
    
    private static final ObjectMapper MAPPER = new ObjectMapper();
    
    // Index-level defaults
    private String compressionType;
    private Integer compressionLevel;
    private Long pageSize;
    private Integer pageRowLimit;
    private Long dictSizeBytes;
    private Long rowGroupSizeBytes;
    
    // Field-level overrides
    private Map<String, FieldConfig> fieldConfigs;
    private Map<String, Object> customSettings;
    
    public String getCompressionType() { return compressionType; }
    public void setCompressionType(String compressionType) { this.compressionType = compressionType; }
    
    public Integer getCompressionLevel() { return compressionLevel; }
    public void setCompressionLevel(Integer compressionLevel) { this.compressionLevel = compressionLevel; }
    
    public Long getPageSize() { return pageSize; }
    public void setPageSize(Long pageSize) { this.pageSize = pageSize; }
    
    public Integer getPageRowLimit() { return pageRowLimit; }
    public void setPageRowLimit(Integer pageRowLimit) { this.pageRowLimit = pageRowLimit; }
    
    public Long getDictSizeBytes() { return dictSizeBytes; }
    public void setDictSizeBytes(Long dictSizeBytes) { this.dictSizeBytes = dictSizeBytes; }
    
    public Long getRowGroupSizeBytes() { return rowGroupSizeBytes; }
    public void setRowGroupSizeBytes(Long rowGroupSizeBytes) { this.rowGroupSizeBytes = rowGroupSizeBytes; }
    
    public Map<String, FieldConfig> getFieldConfigs() { return fieldConfigs; }
    public void setFieldConfigs(Map<String, FieldConfig> fieldConfigs) { this.fieldConfigs = fieldConfigs; }
    
    public void addFieldConfig(String fieldName, FieldConfig config) {
        if (fieldConfigs == null) fieldConfigs = new HashMap<>();
        fieldConfigs.put(fieldName, config);
    }
    
    public Map<String, Object> getCustomSettings() { return customSettings; }
    public void setCustomSettings(Map<String, Object> customSettings) { this.customSettings = customSettings; }
    
    public String toJson() throws JsonProcessingException {
        return MAPPER.writeValueAsString(this);
    }
}
