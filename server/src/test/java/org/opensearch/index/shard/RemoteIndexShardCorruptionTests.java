/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.core.util.FileSystemUtils;
import org.opensearch.test.CorruptionUtils;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.stream.Stream;

@LuceneTestCase.SuppressFileSystems("WindowsFS")
public class RemoteIndexShardCorruptionTests extends IndexShardTestCase {

    public void testLocalDirectoryContains() throws IOException {
        IndexShard indexShard = newStartedShard(true);
        int numDocs = between(1, 10);
        for (int i = 0; i < numDocs; i++) {
            indexDoc(indexShard, "_doc", Integer.toString(i));
        }
        flushShard(indexShard);
        indexShard.store().incRef();
        Directory localDirectory = indexShard.store().directory();
        Path shardPath = indexShard.shardPath().getDataPath().resolve(ShardPath.INDEX_FOLDER_NAME);
        Path tempDir = createTempDir();
        for (String file : localDirectory.listAll()) {
            if (file.equals("write.lock") || file.startsWith("extra")) {
                continue;
            }
            boolean corrupted = randomBoolean();
            long checksum = 0;
            try (IndexInput indexInput = localDirectory.openInput(file, IOContext.READONCE)) {
                checksum = CodecUtil.retrieveChecksum(indexInput);
            }
            if (corrupted) {
                Files.copy(shardPath.resolve(file), tempDir.resolve(file));
                try (FileChannel raf = FileChannel.open(shardPath.resolve(file), StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                    CorruptionUtils.corruptAt(shardPath.resolve(file), raf, (int) (raf.size() - 8));
                }
            }
            if (corrupted == false) {
                assertTrue(indexShard.localDirectoryContains(localDirectory, file, checksum));
            } else {
                assertFalse(indexShard.localDirectoryContains(localDirectory, file, checksum));
                assertFalse(Files.exists(shardPath.resolve(file)));
            }
        }
        try (Stream<Path> files = Files.list(tempDir)) {
            files.forEach(p -> {
                try {
                    Files.copy(p, shardPath.resolve(p.getFileName()));
                } catch (IOException e) {
                    // Ignore
                }
            });
        }
        FileSystemUtils.deleteSubDirectories(tempDir);
        indexShard.store().decRef();
        closeShards(indexShard);
    }
}
