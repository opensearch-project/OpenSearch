/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions;

import java.io.IOException;
import java.nio.file.Path;
import org.opensearch.common.settings.Settings;

/**
 * Noop class for ExtensionsManager
 *
 * @opensearch.internal
 */
public class NoopExtensionsManager extends ExtensionsManager {

    public NoopExtensionsManager(Settings settings, Path extensionsPath) throws IOException {
        super(settings, extensionsPath);
    }
}
