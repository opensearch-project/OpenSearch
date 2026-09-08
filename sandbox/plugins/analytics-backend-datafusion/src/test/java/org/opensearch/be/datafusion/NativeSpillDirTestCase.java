/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.common.io.PathUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Base class for tests that hand a spill directory to {@code NativeBridge.createGlobalRuntime}.
 *
 * <p><b>Why this exists.</b> {@code createGlobalRuntime} renames the spill directory's entries to
 * {@code "<name>.stale"} and deletes them on a <em>detached background thread</em> that
 * {@code closeGlobalRuntime} never joins. Lucene's {@code TestRuleTemporaryFilesCleanup} does a
 * strict recursive {@code rm} of every {@code createTempDir()} tree at suite end. If the spill dir
 * is Lucene-tracked, those two race and the suite fails in {@code classMethod} with
 * {@code NoSuchFileException} on a {@code "*.stale"} file — a flaky failure unrelated to any
 * assertion.
 *
 * <p><b>The fix.</b> Subclasses call {@link #newSpillDir()} instead of
 * {@code createTempDir("datafusion-spill")}. That returns an OS temp dir that Lucene does NOT track,
 * making the native cleanup thread its sole owner, and registers it for best-effort reaping in
 * {@link #tearDown()}. The reaping deliberately tolerates concurrent deletion (it must never use the
 * strict {@code IOUtils.rm}), so it cannot reintroduce the race it exists to remove.
 *
 * <p>Only the <em>spill</em> directory needs this. Data directories (e.g.
 * {@code createTempDir("datafusion-data")}) are never renamed or deleted by native code, so they stay
 * Lucene-tracked as normal.
 *
 * <p>Subclasses overriding {@link #tearDown()} must call {@code super.tearDown()} (as the OpenSearch
 * test convention already requires) so the spill dirs are reaped after the subclass closes its
 * runtime.
 */
public abstract class NativeSpillDirTestCase extends OpenSearchTestCase {

    /** Spill dirs handed to the native runtime; reaped best-effort in {@link #tearDown()}. */
    private final List<Path> spillDirs = new ArrayList<>();

    /**
     * Creates an untracked OS temp directory to use as a native spill directory, and registers it for
     * best-effort cleanup in {@link #tearDown()}.
     *
     * <p>Rooted at {@code java.io.tmpdir} — an explicit location, as forbidden-apis requires — rather
     * than the no-location {@code Files.createTempDirectory(String)} overload, and deliberately NOT
     * Lucene's {@code createTempDir()} (see the class javadoc for why).
     */
    protected final Path newSpillDir() {
        Path spillDir;
        try {
            spillDir = Files.createTempDirectory(PathUtils.get(System.getProperty("java.io.tmpdir")), "datafusion-spill");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        spillDirs.add(spillDir);
        return spillDir;
    }

    @Override
    public void tearDown() throws Exception {
        // Best-effort, race-tolerant delete. By now each test has closed its runtime, so the native
        // cleanup thread has been triggered but may still be unlinking "*.stale" entries concurrently.
        // Using the strict IOUtils.rm here would throw NoSuchFileException on a file the native thread
        // deleted first — the very bug this base class exists to prevent. Swallow per-entry failures;
        // the OS reaps the temp root either way.
        for (Path dir : spillDirs) {
            deleteBestEffort(dir);
        }
        spillDirs.clear();
        super.tearDown();
    }

    private static void deleteBestEffort(Path root) {
        if (Files.notExists(root)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(root)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                    // Concurrent native cleanup may have removed it, or a child reappeared mid-walk.
                }
            });
        } catch (IOException ignored) {
            // Directory vanished under us (native thread finished the wipe) — nothing left to do.
        }
    }
}
