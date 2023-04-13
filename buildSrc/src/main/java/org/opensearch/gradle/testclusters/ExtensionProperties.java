/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.opensearch.gradle.testclusters;

public class ExtensionProperties {
    private String name;
    private String uniqueId;
    private String hostAddress;
    private String port;
    private String version;
    private String opensearchVersion;
    private String minimumCompatibleVersion;
    
    public ExtensionProperties(String name, String uniqueId, String hostAddress, String port, String version,
            String opensearchVersion, String minimumCompatibleVersion) {
        this.name = name;
        this.uniqueId = uniqueId;
        this.hostAddress = hostAddress;
        this.port = port;
        this.version = version;
        this.opensearchVersion = opensearchVersion;
        this.minimumCompatibleVersion = minimumCompatibleVersion;
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
    public String getMinimumCompatibleVersion() {
        return minimumCompatibleVersion;
    }
    public void setMinimumCompatibleVersion(String minimumCompatibleVersion) {
        this.minimumCompatibleVersion = minimumCompatibleVersion;
    }
}
