/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gradle.testclusters;

import java.util.List;

public class ExtensionsProperties {
    private String name;
    private String uniqueId;
    private String hostAddress;
    private String port;
    private String version;
    private String opensearchVersion;
    private String minimumCompatibleVersion;
    private Boolean bwcPluginMode;
    private List<String> extensionDistinguishedNames;

    public ExtensionsProperties(
        String name,
        String uniqueId,
        String hostAddress,
        String port,
        String version,
        String opensearchVersion,
        String minimumCompatibleVersion,
        String bwcPluginMode,
        List<String> extensionDistinguishedNames
    ) {
        this.name = name;
        this.uniqueId = uniqueId;
        this.hostAddress = hostAddress;
        this.port = port;
        this.version = version;
        this.opensearchVersion = opensearchVersion;
        this.minimumCompatibleVersion = minimumCompatibleVersion;
        this.bwcPluginMode = Boolean.parseBoolean(bwcPluginMode);
        this.extensionDistinguishedNames = extensionDistinguishedNames;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUniqueId() {
        return uniqueId;
    }

    public void setUniqueId(String uniqueId) {
        this.uniqueId = uniqueId;
    }

    public String getHostAddress() {
        return hostAddress;
    }

    public void setHostAddress(String hostAddress) {
        this.hostAddress = hostAddress;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public Boolean getBwcPluginMode() { return bwcPluginMode; }

    public void setBwcPluginMode(Boolean bwcPluginMode) {this.bwcPluginMode = bwcPluginMode; }

    public List<String> getextensionDistinguishedNames() {return extensionDistinguishedNames;}

    public void setextensionDistinguishedNames(List<String> extensionDistinguishedNames) {this.extensionDistinguishedNames = extensionDistinguishedNames; }

    public String getOpensearchVersion() {
        return opensearchVersion;
    }

    public void setOpensearchVersion(String opensearchVersion) {
        this.opensearchVersion = opensearchVersion;
    }

    public String getMinimumCompatibleVersion() {
        return minimumCompatibleVersion;
    }

    public void setMinimumCompatibleVersion(String minimumCompatibleVersion) {
        this.minimumCompatibleVersion = minimumCompatibleVersion;
    }
}
