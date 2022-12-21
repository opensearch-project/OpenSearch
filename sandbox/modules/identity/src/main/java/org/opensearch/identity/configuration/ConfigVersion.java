/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.configuration;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class ConfigVersion {

    private String type;
    private int config_version;

    private CType cType;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
        cType = CType.fromString(type);
    }

    public int getConfig_version() {
        return config_version;
    }

    public void setConfig_version(int config_version) {
        this.config_version = config_version;
    }

    @JsonIgnore
    public CType getCType() {
        return cType;
    }

    @Override
    public String toString() {
        return "ConfigVersion [type=" + type + ", config_version=" + config_version + ", cType=" + cType + "]";
    }

}
