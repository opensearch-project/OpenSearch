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

package org.opensearch.index.shard;

import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.CheckIndex.Level;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.opensearch.OpenSearchException;
import org.opensearch.cli.Terminal;
import org.opensearch.common.collect.Tuple;

import java.io.IOException;
import java.io.PrintStream;

/**
 * Removes corrupted Lucene index segments
 *
 * @opensearch.internal
 */
public class RemoveCorruptedLuceneSegmentsAction {

    public Tuple<RemoveCorruptedShardDataCommand.CleanStatus, String> getCleanStatus(
        Directory indexDirectory,
        Lock writeLock,
        PrintStream printStream,
        boolean verbose
    ) throws IOException {
        boolean markedCorrupted = RemoveCorruptedShardDataCommand.isCorruptMarkerFileIsPresent(indexDirectory);

        final CheckIndex.Status status;
        try (CheckIndex checker = new CheckIndex(indexDirectory, writeLock)) {
            checker.setLevel(Level.MIN_LEVEL_FOR_CHECKSUM_CHECKS);
            checker.setInfoStream(printStream, verbose);

            status = checker.checkIndex(null);

            if (status.missingSegments) {
                return Tuple.tuple(
                    RemoveCorruptedShardDataCommand.CleanStatus.UNRECOVERABLE,
                    "Index is unrecoverable - there are missing segments"
                );
            }

            return status.clean
                ? Tuple.tuple(
                    markedCorrupted
                        ? RemoveCorruptedShardDataCommand.CleanStatus.CLEAN_WITH_CORRUPTED_MARKER
                        : RemoveCorruptedShardDataCommand.CleanStatus.CLEAN,
                    null
                )
                : Tuple.tuple(
                    RemoveCorruptedShardDataCommand.CleanStatus.CORRUPTED,
                    "Corrupted Lucene index segments found - " + status.totLoseDocCount + " documents will be lost."
                );
        }
    }

    public void execute(Terminal terminal, Directory indexDirectory, Lock writeLock, PrintStream printStream, boolean verbose)
        throws IOException {
        final CheckIndex.Status status;
        try (CheckIndex checker = new CheckIndex(indexDirectory, writeLock)) {

            checker.setLevel(Level.MIN_LEVEL_FOR_CHECKSUM_CHECKS);
            checker.setInfoStream(printStream, verbose);

            status = checker.checkIndex(null);

            if (status.missingSegments == false) {
                if (status.clean == false) {
                    terminal.println("Writing...");
                    checker.exorciseIndex(status);

                    terminal.println("OK");
                    terminal.println("Wrote new segments file \"" + status.segmentsFileName + "\"");
                }
            } else {
                throw new OpenSearchException("Index is unrecoverable - there are missing segments");
            }
        }
    }
}
