/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gradle.testclusters;

import java.util.List;

public class TestExtensionsList {
    private List<ExtensionsProperties> extensions;

    public TestExtensionsList(List<ExtensionsProperties> extensionsList) {
        extensions = extensionsList;
    }

    public List<ExtensionsProperties> getExtensions() {
        return extensions;
    }

    public void setExtensions(List<ExtensionsProperties> extensionsList) {
        extensions = extensionsList;
    }
}
