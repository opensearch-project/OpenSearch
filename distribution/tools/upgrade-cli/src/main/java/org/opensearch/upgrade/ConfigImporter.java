/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.upgrade;

import org.opensearch.cli.Terminal;
import org.opensearch.cli.UserException;

import java.util.Arrays;
import java.util.List;

interface ConfigImporter {
    static List<ConfigImporter> getImporters(ImportConfigOptions icOptions) {
        return Arrays.asList(new YmlConfigImporter(icOptions), new KeystoreSettingsImporter(icOptions));
    }

    void doImport(Terminal terminal) throws UserException;
}
