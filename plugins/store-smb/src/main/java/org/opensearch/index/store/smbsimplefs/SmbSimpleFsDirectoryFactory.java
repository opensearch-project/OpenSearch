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

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.store.smbsimplefs;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.store.FsDirectoryFactory;
import org.opensearch.index.store.SmbDirectoryWrapper;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Factory to create a new Simple File System type directory accessible as a SMB share
 *
 * @deprecated use {@link org.opensearch.index.store.smbniofs.SmbNIOFsDirectoryFactory} instead
 */
@Deprecated
public final class SmbSimpleFsDirectoryFactory extends FsDirectoryFactory {

    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(SmbSimpleFsDirectoryFactory.class);

    @Override
    protected Directory newFSDirectory(Path location, LockFactory lockFactory, IndexSettings indexSettings) throws IOException {
        DEPRECATION_LOGGER.deprecate(IndexModule.Type.SIMPLEFS.getSettingsKey(), IndexModule.Type.SIMPLEFS.getSettingsKey()
            + " is deprecated and will be removed in 2.0");
        return new SmbDirectoryWrapper(new SimpleFSDirectory(location, lockFactory));
    }
}
