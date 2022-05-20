/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions;

import java.util.ArrayList;
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
        private String ephemeralId;
        private String hostName;
        private String hostAddress;
        private String port;
        private String version;

        public Extension() {
            name = "";
            ephemeralId = "";
            hostName = "";
            hostAddress = "";
            port = "";
            version = "";
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getEphemeralId() {
            return ephemeralId;
        }

        public void setEphemeralId(String ephemeralId) {
            this.ephemeralId = ephemeralId;
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
            return "Extension [ephemeralId="
                + ephemeralId
                + ", hostAddress="
                + hostAddress
                + ", hostName="
                + hostName
                + ", name="
                + name
                + ", port="
                + port
                + ", version="
                + version
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
