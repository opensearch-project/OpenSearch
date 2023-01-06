/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * List of extension configurations from extension.yml
 *
 * @opensearch.internal
 */
public class ExtensionsSettings {

    private List<Extension> extensions;

    public ExtensionsSettings() {
        extensions = new ArrayList<Extension>();
    }

    /**
     * Extension configuration used for extension discovery
     *
     * @opensearch.internal
    */
    public static class Extension {

        private String name;
        private String uniqueId;
        private String hostName;
        private String hostAddress;
        private String port;
        private String version;
        private String description;
        private String opensearchVersion;
        private String jvmVersion;
        private String className;
        private String customFolderName;
        private String hasNativeController;
        private List<ExtensionDependency> dependencies = Collections.emptyList();

        public Extension() {
            name = "";
            uniqueId = "";
            hostName = "";
            hostAddress = "";
            port = "";
            version = "";
            description = "";
            opensearchVersion = "";
            jvmVersion = "";
            className = "";
            customFolderName = "";
            hasNativeController = "false";
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

        public String getHostName() {
            return hostName;
        }

        public void setHostName(String hostName) {
            this.hostName = hostName;
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

        @Override
        public String toString() {
            return "Extension [className="
                + className
                + ", customFolderName="
                + customFolderName
                + ", description="
                + description
                + ", hasNativeController="
                + hasNativeController
                + ", hostAddress="
                + hostAddress
                + ", hostName="
                + hostName
                + ", jvmVersion="
                + jvmVersion
                + ", name="
                + name
                + ", opensearchVersion="
                + opensearchVersion
                + ", port="
                + port
                + ", uniqueId="
                + uniqueId
                + ", version="
                + version
                + "]";
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        public String getOpensearchVersion() {
            return opensearchVersion;
        }

        public void setOpensearchVersion(String opensearchVersion) {
            this.opensearchVersion = opensearchVersion;
        }

        public String getJavaVersion() {
            return jvmVersion;
        }

        public void setJavaVersion(String jvmVersion) {
            this.jvmVersion = jvmVersion;
        }

        public String getClassName() {
            return className;
        }

        public void setClassName(String className) {
            this.className = className;
        }

        public String getCustomFolderName() {
            return customFolderName;
        }

        public void setCustomFolderName(String customFolderName) {
            this.customFolderName = customFolderName;
        }

        public String hasNativeController() {
            return hasNativeController;
        }

        public void setHasNativeController(String hasNativeController) {
            this.hasNativeController = hasNativeController;
        }

        public List<ExtensionDependency> getDependencies() {
            return dependencies;
        }

    }

    public List<Extension> getExtensions() {
        return extensions;
    }

    public void setExtensions(List<Extension> extensions) {
        this.extensions = extensions;
    }

    @Override
    public String toString() {
        return "ExtensionsSettings [extensions=" + extensions + "]";
    }

}
