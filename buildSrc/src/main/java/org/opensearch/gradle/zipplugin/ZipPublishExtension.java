/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.gradle.zipplugin;

public class ZipPublishExtension {

    private String zipGroup = "org.opensearch.plugin";

    public void setZipgroup(String zipGroup) {
        this.zipGroup = zipGroup;
    }

    public String getZipgroup() {
        return zipGroup;
    }

}
