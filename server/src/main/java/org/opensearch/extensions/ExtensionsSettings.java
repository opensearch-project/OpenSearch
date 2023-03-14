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

    public ExtensionsSettings(List<Extension> extensions) {
        this.extensions = extensions;
    }

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
        private String hostAddress;
        private String port;
        private String version;
        private String opensearchVersion;
        private String minimumCompatibleVersion;
        private List<ExtensionDependency> dependencies = Collections.emptyList();

        public Extension(
            String name,
            String uniqueId,
            String hostAddress,
            String port,
            String version,
            String opensearchVersion,
            String minimumCompatibleVersion,
            List<ExtensionDependency> dependencies
        ) {
            this.name = name;
            this.uniqueId = uniqueId;
            this.hostAddress = hostAddress;
            this.port = port;
            this.version = version;
            this.opensearchVersion = opensearchVersion;
            this.minimumCompatibleVersion = minimumCompatibleVersion;
            this.dependencies = dependencies;
        }

        public Extension() {
            name = "";
            uniqueId = "";
            hostAddress = "";
            port = "";
            version = "";
            opensearchVersion = "";
            minimumCompatibleVersion = "";
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

        public String getOpensearchVersion() {
            return opensearchVersion;
        }

        public void setOpensearchVersion(String opensearchVersion) {
            this.opensearchVersion = opensearchVersion;
        }

        public List<ExtensionDependency> getDependencies() {
            return dependencies;
        }

        public String getMinimumCompatibleVersion() {
            return minimumCompatibleVersion;
        }

        public void setMinimumCompatibleVersion(String minimumCompatibleVersion) {
            this.minimumCompatibleVersion = minimumCompatibleVersion;
        }

        @Override
        public String toString() {
            return "Extension [name="
                + name
                + ", uniqueId="
                + uniqueId
                + ", hostAddress="
                + hostAddress
                + ", port="
                + port
                + ", version="
                + version
                + ", opensearchVersion="
                + opensearchVersion
                + ", minimumCompatibleVersion="
                + minimumCompatibleVersion
                + "]";
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
