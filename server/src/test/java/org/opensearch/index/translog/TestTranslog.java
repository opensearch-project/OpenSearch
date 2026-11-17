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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.index.translog;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.common.UUIDs;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CRC32;

import static org.opensearch.index.translog.Translog.CHECKPOINT_FILE_NAME;
import static org.opensearch.index.translog.Translog.TRANSLOG_FILE_SUFFIX;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;

/**
 * Helpers for testing translog.
 */
public class TestTranslog {
    private static final Pattern TRANSLOG_FILE_PATTERN = Pattern.compile("^translog-(\\d+)\\.(tlog|ckp)$");

    /**
     * Writes a closed translog generation (header + a few operation bytes, optionally followed by a
     * {@link TranslogFooter}) and its numbered checkpoint file at {@code location}, laid out exactly as
     * {@link TranslogWriter#closeIntoReader()} leaves them for a remote-enabled translog. Returns the content
     * checksum, i.e. the CRC32 over {@code [0, checkpoint.offset)} that the footer carries.
     */
    public static long createTranslogGeneration(Random random, Path location, long generation, boolean withFooter) throws IOException {
        Path translogPath = location.resolve(Translog.getFilename(generation));
        Path checkpointPath = location.resolve(Translog.getCommitCheckpointFileName(generation));
        Files.createFile(translogPath);
        try (FileChannel channel = FileChannel.open(translogPath, StandardOpenOption.WRITE)) {
            TranslogHeader header = new TranslogHeader(UUIDs.randomBase64UUID(random), 1);
            header.write(channel, true);
            byte[] operationBytes = new byte[RandomNumbers.randomIntBetween(random, 4, 64)];
            random.nextBytes(operationBytes);
            channel.write(ByteBuffer.wrap(operationBytes));
            long offset = channel.position();
            CRC32 crc = new CRC32();
            crc.update(Files.readAllBytes(translogPath), 0, Math.toIntExact(offset));
            long contentChecksum = crc.getValue();
            if (withFooter) {
                TranslogFooter.write(channel, contentChecksum, true);
            }
            Checkpoint checkpoint = new Checkpoint(offset, 1, generation, 0, 0, 0, generation, SequenceNumbers.NO_OPS_PERFORMED);
            Checkpoint.write(FileChannel::open, checkpointPath, checkpoint, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
            return contentChecksum;
        }
    }

    /**
     * Flips one byte inside the body of the last operation of a randomly chosen generation that holds operations, and
     * returns the corrupted translog file. The operation's size prefix, the numbered checkpoint and any
     * {@link TranslogFooter} are left intact, so the generation still passes every structural check and the rot only
     * surfaces as a per-operation checksum failure when the operation is read.
     */
    public static Path corruptLastOperationOfRandomGeneration(Random random, Path translogDir) throws IOException {
        List<Path> nonEmpty = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(translogDir, "translog-*" + TRANSLOG_FILE_SUFFIX)) {
            for (Path translogPath : stream) {
                Checkpoint checkpoint = readCheckpointOfGeneration(translogDir, translogPath);
                if (checkpoint != null && checkpoint.numOps > 0) {
                    nonEmpty.add(translogPath);
                }
            }
        }
        assertThat("expected at least one generation with operations in " + translogDir, nonEmpty, not(empty()));
        nonEmpty.sort(Comparator.naturalOrder());
        Path victim = RandomPicks.randomFrom(random, nonEmpty);
        Checkpoint checkpoint = readCheckpointOfGeneration(translogDir, victim);
        // The last operation ends at checkpoint.offset with its 4-byte checksum; six bytes back is inside its body.
        long position = checkpoint.offset - 6;
        try (FileChannel channel = FileChannel.open(victim, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ByteBuffer one = ByteBuffer.allocate(1);
            channel.read(one, position);
            one.flip();
            byte original = one.get();
            one.clear();
            one.put((byte) (original ^ 0x1)).flip();
            channel.write(one, position);
        }
        return victim;
    }

    /**
     * Reads the checkpoint describing {@code translogPath}: its numbered checkpoint for a closed generation, or
     * {@code translog.ckp} for the current writer's generation, which has no numbered checkpoint yet. Returns
     * {@code null} if neither describes this generation.
     */
    private static Checkpoint readCheckpointOfGeneration(Path translogDir, Path translogPath) throws IOException {
        long generation = Translog.parseIdFromFileName(translogPath);
        Path checkpointPath = translogDir.resolve(Translog.getCommitCheckpointFileName(generation));
        if (Files.exists(checkpointPath) == false) {
            checkpointPath = translogDir.resolve(CHECKPOINT_FILE_NAME);
            if (Files.exists(checkpointPath) == false) {
                return null;
            }
        }
        Checkpoint checkpoint = Checkpoint.read(checkpointPath);
        return checkpoint.generation == generation ? checkpoint : null;
    }

    /**
     * Corrupts random translog file (translog-N.tlog or translog-N.ckp or translog.ckp) from the given translog directory, ignoring
     * translogs and checkpoints with generations below the generation recorded in the latest index commit found in translogDir/../index/,
     * or writes a corrupted translog-N.ckp file as if from a crash while rolling a generation.
     *
     * <p>
     * See {@link TestTranslog#corruptFile(Logger, Random, Path, boolean)} for details of the corruption applied.
     */
    public static void corruptRandomTranslogFile(Logger logger, Random random, Path translogDir) throws IOException {
        corruptRandomTranslogFile(logger, random, translogDir, Translog.readCheckpoint(translogDir).minTranslogGeneration);
    }

    /**
     * Corrupts random translog file (translog-N.tlog or translog-N.ckp or translog.ckp) from the given translog directory, or writes a
     * corrupted translog-N.ckp file as if from a crash while rolling a generation.
     * <p>
     * See {@link TestTranslog#corruptFile(Logger, Random, Path, boolean)} for details of the corruption applied.
     *
     * @param minGeneration the minimum generation (N) to corrupt. Translogs and checkpoints with lower generation numbers are ignored.
     */
    static void corruptRandomTranslogFile(Logger logger, Random random, Path translogDir, long minGeneration) throws IOException {
        logger.info("--> corruptRandomTranslogFile: translogDir [{}], minUsedTranslogGen [{}]", translogDir, minGeneration);

        Path unnecessaryCheckpointCopyPath = null;
        try {
            final Path checkpointPath = translogDir.resolve(CHECKPOINT_FILE_NAME);
            final Checkpoint checkpoint = Checkpoint.read(checkpointPath);
            unnecessaryCheckpointCopyPath = translogDir.resolve(Translog.getCommitCheckpointFileName(checkpoint.generation));
            if (LuceneTestCase.rarely(random) && Files.exists(unnecessaryCheckpointCopyPath) == false) {
                // if we crashed while rolling a generation then we might have copied `translog.ckp` to its numbered generation file but
                // have not yet written a new `translog.ckp`. During recovery we must also verify that this file is intact, so it's ok to
                // corrupt this file too (either by writing the wrong information, correctly formatted, or by properly corrupting it)
                final Checkpoint checkpointCopy;
                if (LuceneTestCase.usually(random)) {
                    checkpointCopy = checkpoint;
                } else {
                    long newTranslogGeneration = checkpoint.generation + random.nextInt(2);
                    long newMinTranslogGeneration = Math.min(newTranslogGeneration, checkpoint.minTranslogGeneration + random.nextInt(2));
                    long newMaxSeqNo = checkpoint.maxSeqNo + random.nextInt(2);
                    long newMinSeqNo = Math.min(newMaxSeqNo, checkpoint.minSeqNo + random.nextInt(2));
                    long newTrimmedAboveSeqNo = Math.min(newMaxSeqNo, checkpoint.trimmedAboveSeqNo + random.nextInt(2));

                    checkpointCopy = new Checkpoint(
                        checkpoint.offset + random.nextInt(2),
                        checkpoint.numOps + random.nextInt(2),
                        newTranslogGeneration,
                        newMinSeqNo,
                        newMaxSeqNo,
                        checkpoint.globalCheckpoint + random.nextInt(2),
                        newMinTranslogGeneration,
                        newTrimmedAboveSeqNo
                    );
                }
                Checkpoint.write(
                    FileChannel::open,
                    unnecessaryCheckpointCopyPath,
                    checkpointCopy,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE_NEW
                );

                if (checkpointCopy.equals(checkpoint) == false) {
                    logger.info(
                        "corruptRandomTranslogFile: created [{}] containing [{}] instead of [{}]",
                        unnecessaryCheckpointCopyPath,
                        checkpointCopy,
                        checkpoint
                    );
                    return;
                } // else checkpoint copy has the correct content so it's now a candidate for the usual kinds of corruption
            }
        } catch (TranslogCorruptedException e) {
            // missing or corrupt checkpoint already, find something else to break...
        }

        Set<Path> candidates = new TreeSet<>(); // TreeSet makes sure iteration order is deterministic
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(translogDir)) {
            for (Path item : stream) {
                if (Files.isRegularFile(item) && Files.size(item) > 0) {
                    final String filename = item.getFileName().toString();
                    final Matcher matcher = TRANSLOG_FILE_PATTERN.matcher(filename);
                    if (filename.equals("translog.ckp") || (matcher.matches() && Long.parseLong(matcher.group(1)) >= minGeneration)) {
                        candidates.add(item);
                    }
                }
            }
        }
        assertThat("no corruption candidates found in " + translogDir, candidates, is(not(empty())));

        final Path fileToCorrupt = RandomPicks.randomFrom(random, candidates);

        // deleting the unnecessary checkpoint file doesn't count as a corruption
        final boolean maybeDelete = fileToCorrupt.equals(unnecessaryCheckpointCopyPath) == false;

        corruptFile(logger, random, fileToCorrupt, maybeDelete);
    }

    /**
     * Corrupt an (existing and nonempty) file by replacing any byte in the file with a random (different) byte, or by truncating the file
     * to a random (strictly shorter) length, or by deleting the file.
     */
    static void corruptFile(Logger logger, Random random, Path fileToCorrupt, boolean maybeDelete) throws IOException {
        assertThat(fileToCorrupt + " should be a regular file", Files.isRegularFile(fileToCorrupt));
        final long fileSize = Files.size(fileToCorrupt);
        assertThat(fileToCorrupt + " should not be an empty file", fileSize, greaterThan(0L));

        if (maybeDelete && random.nextBoolean() && random.nextBoolean()) {
            logger.info("corruptFile: deleting file {}", fileToCorrupt);
            IOUtils.rm(fileToCorrupt);
            return;
        }

        try (FileChannel fileChannel = FileChannel.open(fileToCorrupt, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            final long corruptPosition = RandomNumbers.randomLongBetween(random, 0, fileSize - 1);

            if (random.nextBoolean()) {
                do {
                    // read
                    fileChannel.position(corruptPosition);
                    assertThat(fileChannel.position(), equalTo(corruptPosition));
                    ByteBuffer bb = ByteBuffer.wrap(new byte[1]);
                    fileChannel.read(bb);
                    bb.flip();

                    // corrupt
                    byte oldValue = bb.get(0);
                    byte newValue;
                    do {
                        newValue = (byte) random.nextInt(0x100);
                    } while (newValue == oldValue);
                    bb.put(0, newValue);

                    // rewrite
                    fileChannel.position(corruptPosition);
                    fileChannel.write(bb);
                    logger.info(
                        "corruptFile: corrupting file {} at position {} turning 0x{} into 0x{}",
                        fileToCorrupt,
                        corruptPosition,
                        Integer.toHexString(oldValue & 0xff),
                        Integer.toHexString(newValue & 0xff)
                    );
                } while (isTranslogHeaderVersionFlipped(fileToCorrupt, fileChannel));

            } else {
                logger.info("corruptFile: truncating file {} from length {} to length {}", fileToCorrupt, fileSize, corruptPosition);
                fileChannel.truncate(corruptPosition);
            }
        }
    }

    /**
     * Returns the primary term associated with the current translog writer of the given translog.
     */
    public static long getCurrentTerm(Translog translog) {
        return translog.getCurrent().getPrimaryTerm();
    }

    public static List<Translog.Operation> drainSnapshot(Translog.Snapshot snapshot, boolean sortBySeqNo) throws IOException {
        final List<Translog.Operation> ops = new ArrayList<>(snapshot.totalOperations());
        Translog.Operation op;
        while ((op = snapshot.next()) != null) {
            ops.add(op);
        }
        if (sortBySeqNo) {
            ops.sort(Comparator.comparing(Translog.Operation::seqNo));
        }
        return ops;
    }

    public static Translog.Snapshot newSnapshotFromOperations(List<Translog.Operation> operations) {
        final Iterator<Translog.Operation> iterator = operations.iterator();
        return new Translog.Snapshot() {
            @Override
            public int totalOperations() {
                return operations.size();
            }

            @Override
            public Translog.Operation next() {
                if (iterator.hasNext()) {
                    return iterator.next();
                } else {
                    return null;
                }
            }

            @Override
            public void close() {

            }
        };
    }

    /**
     * An old translog header does not have a checksum. If we flip the header version of an empty translog from 3 to 2,
     * then we won't detect that corruption, and the translog will be considered clean as before.
     */
    static boolean isTranslogHeaderVersionFlipped(Path corruptedFile, FileChannel channel) throws IOException {
        if (corruptedFile.toString().endsWith(TRANSLOG_FILE_SUFFIX) == false) {
            return false;
        }
        channel.position(0);
        final InputStreamStreamInput in = new InputStreamStreamInput(Channels.newInputStream(channel), channel.size());
        try {
            final int version = TranslogHeader.readHeaderVersion(corruptedFile, channel, in);
            return version == TranslogHeader.VERSION_CHECKPOINTS;
        } catch (IllegalStateException | TranslogCorruptedException | IOException e) {
            return false;
        }
    }

    static class LocationOperation implements Comparable<LocationOperation> {
        final Translog.Operation operation;
        final Translog.Location location;

        LocationOperation(Translog.Operation operation, Translog.Location location) {
            this.operation = operation;
            this.location = location;
        }

        @Override
        public int compareTo(LocationOperation o) {
            return location.compareTo(o.location);
        }
    }

    static class FailSwitch {
        private volatile int failRate;
        private volatile boolean onceFailedFailAlways = false;

        public boolean fail() {
            final int rnd = OpenSearchTestCase.randomIntBetween(1, 100);
            boolean fail = rnd <= failRate;
            if (fail && onceFailedFailAlways) {
                failAlways();
            }
            return fail;
        }

        public void failNever() {
            failRate = 0;
        }

        public void failAlways() {
            failRate = 100;
        }

        public void failRandomly() {
            failRate = OpenSearchTestCase.randomIntBetween(1, 100);
        }

        public void failRate(int rate) {
            failRate = rate;
        }

        public void onceFailedFailAlways() {
            onceFailedFailAlways = true;
        }
    }

    static class SlowDownWriteSwitch {
        private volatile int sleepSeconds;

        public void setSleepSeconds(int sleepSeconds) {
            this.sleepSeconds = sleepSeconds;
        }

        public int getSleepSeconds() {
            return sleepSeconds;
        }
    }

    static class SortedSnapshot implements Translog.Snapshot {
        private final Translog.Snapshot snapshot;
        private List<Translog.Operation> operations = null;

        SortedSnapshot(Translog.Snapshot snapshot) {
            this.snapshot = snapshot;
        }

        @Override
        public int totalOperations() {
            return snapshot.totalOperations();
        }

        @Override
        public Translog.Operation next() throws IOException {
            if (operations == null) {
                operations = new ArrayList<>();
                Translog.Operation op;
                while ((op = snapshot.next()) != null) {
                    operations.add(op);
                }
                operations.sort(Comparator.comparing(Translog.Operation::seqNo));
            }
            if (operations.isEmpty()) {
                return null;
            }
            return operations.remove(0);
        }

        @Override
        public void close() throws IOException {
            snapshot.close();
        }
    }
}
