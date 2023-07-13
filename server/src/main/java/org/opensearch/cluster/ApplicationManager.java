/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster;

import java.util.concurrent.atomic.AtomicReference;
import org.opensearch.extensions.ExtensionsManager;

/**
 * The ApplicationManager class handles the processing and resolution of multiple types of applications. Using the class, OpenSearch can
 * continue to resolve requests even when specific application types are disabled. For example, the ExtensionManager can be Noop in which case
 * the ApplicationManager is able to resolve requests for other application types still
 *
 * @opensearch.experimental
 */
public class ApplicationManager {

    AtomicReference<ExtensionsManager> extensionManager;
    public static ApplicationManager instance; // Required for access in static contexts

    public ApplicationManager() {
        instance = this;
        extensionManager = new AtomicReference<>();
    }

    public void register(ExtensionsManager manager) {
        if (this.extensionManager == null) {
            this.extensionManager = new AtomicReference<>(manager);
        } else {
            this.extensionManager.set(manager);
        }
    }

}
