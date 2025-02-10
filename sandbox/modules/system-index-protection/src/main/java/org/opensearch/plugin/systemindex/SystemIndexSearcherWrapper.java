/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.plugin.systemindex;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexService;
import org.opensearch.indices.SystemIndexRegistry;

import java.io.IOException;
import java.util.Set;

import static org.opensearch.plugin.systemindex.SystemIndexProtectionPlugin.SYSTEM_INDEX_PROTECTION_ENABLED_KEY;

public class SystemIndexSearcherWrapper implements CheckedFunction<DirectoryReader, DirectoryReader, IOException> {

    protected final Logger log = LogManager.getLogger(this.getClass());
    protected final ThreadContext threadContext;
    protected final Index index;
    private final Boolean systemIndexEnabled;

    // constructor is called per index, so avoid costly operations here
    public SystemIndexSearcherWrapper(final IndexService indexService, final Settings settings) {
        index = indexService.index();
        threadContext = indexService.getThreadPool().getThreadContext();

        this.systemIndexEnabled = settings.getAsBoolean(SYSTEM_INDEX_PROTECTION_ENABLED_KEY, false);
    }

    @Override
    public final DirectoryReader apply(DirectoryReader reader) throws IOException {

        if (systemIndexEnabled && isBlockedSystemIndexRequest() && !threadContext.isSystemContext()) {
            log.warn("search action for {} is not allowed", index.getName());
            return new EmptyFilterLeafReader.EmptyDirectoryReader(reader);
        }

        return wrap(reader);
    }

    protected DirectoryReader wrap(final DirectoryReader reader) {
        return reader;
    }

    protected final boolean isBlockedSystemIndexRequest() {
        return !SystemIndexRegistry.matchesSystemIndexPattern(Set.of(index.getName())).isEmpty();
    }
}
