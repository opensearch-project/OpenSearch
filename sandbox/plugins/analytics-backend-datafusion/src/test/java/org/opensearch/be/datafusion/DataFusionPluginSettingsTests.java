/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Verifies the settings declared by {@link DataFusionPlugin} — in particular that
 * {@code datafusion.memory_pool_limit_bytes} is registered and marked dynamic so
 * the cluster settings API can update it at runtime.
 */
public class DataFusionPluginSettingsTests extends OpenSearchTestCase {

    public void testMemoryPoolLimitIsDynamic() {
        assertTrue(
            "datafusion.memory_pool_limit_bytes must be dynamic to support runtime updates",
            DataFusionPlugin.DATAFUSION_MEMORY_POOL_LIMIT.isDynamic()
        );
    }

    public void testSpillMemoryLimitIsDynamic() {
        assertTrue(
            "datafusion.spill_memory_limit_bytes must be dynamic so cluster-state updates are accepted; "
                + "live propagation depends on the loaded native library",
            DataFusionPlugin.DATAFUSION_SPILL_MEMORY_LIMIT.isDynamic()
        );
        assertTrue(
            "datafusion.spill_memory_limit_bytes must have node scope",
            DataFusionPlugin.DATAFUSION_SPILL_MEMORY_LIMIT.hasNodeScope()
        );
    }

    /**
     * The cluster-settings listener can fire before {@link DataFusionPlugin#createComponents}
     * is called (service field still null). {@code updateSpillMemoryLimit} must swallow this
     * quietly to mirror {@link DataFusionPlugin#updateMemoryPoolLimit} so cluster-state
     * application does not log a spurious failure during node startup.
     */
    public void testUpdateSpillMemoryLimitBeforeServiceStartDoesNotThrow() {
        try (DataFusionPlugin plugin = new DataFusionPlugin()) {
            plugin.updateSpillMemoryLimit(32L * 1024 * 1024);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    public void testMemoryPoolLimitHasNodeScope() {
        assertTrue("datafusion.memory_pool_limit_bytes must have node scope", DataFusionPlugin.DATAFUSION_MEMORY_POOL_LIMIT.hasNodeScope());
    }

    public void testPluginRegistersMemoryPoolLimitSetting() {
        try (DataFusionPlugin plugin = new DataFusionPlugin()) {
            List<Setting<?>> settings = plugin.getSettings();
            assertTrue(
                "Plugin must register DATAFUSION_MEMORY_POOL_LIMIT via getSettings()",
                settings.contains(DataFusionPlugin.DATAFUSION_MEMORY_POOL_LIMIT)
            );
            assertTrue(
                "Plugin must register DATAFUSION_SPILL_MEMORY_LIMIT via getSettings()",
                settings.contains(DataFusionPlugin.DATAFUSION_SPILL_MEMORY_LIMIT)
            );
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    /**
     * H1 — the cluster-settings listener can fire before {@link DataFusionPlugin#createComponents}
     * is called (service field still null). {@code updateMemoryPoolLimit} must swallow this quietly
     * so the cluster-state update does not log a failure during node startup.
     */
    public void testUpdateMemoryPoolLimitBeforeServiceStartDoesNotThrow() {
        try (DataFusionPlugin plugin = new DataFusionPlugin()) {
            // Service field is null — should be a no-op, not an NPE.
            plugin.updateMemoryPoolLimit(64L * 1024 * 1024);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    public void testGetSettingsReturnsAllIndexedSettings() {
        try (DataFusionPlugin plugin = new DataFusionPlugin()) {
            List<Setting<?>> settings = plugin.getSettings();
            Set<String> settingKeys = settings.stream().map(Setting::getKey).collect(Collectors.toSet());

            assertTrue(settingKeys.contains("datafusion.indexed.batch_size"));
            assertTrue(settingKeys.contains("datafusion.indexed.parquet_pushdown_filters"));
            assertTrue(settingKeys.contains("datafusion.indexed.bloom_filter_on_read"));
            assertTrue(settingKeys.contains("datafusion.indexed.min_skip_run_default"));
            assertTrue(settingKeys.contains("datafusion.indexed.min_skip_run_selectivity_threshold"));
            assertTrue(settingKeys.contains("datafusion.indexed.single_collector_strategy"));
            assertTrue(settingKeys.contains("datafusion.indexed.tree_collector_strategy"));
            assertTrue(settingKeys.contains("datafusion.indexed.max_collector_parallelism"));
            assertTrue(settingKeys.contains("datafusion.indexed.query_strategy"));
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    public void testGetSettingsReturnsTotalExpectedCount() {
        try (DataFusionPlugin plugin = new DataFusionPlugin()) {
            List<Setting<?>> settings = plugin.getSettings();
            assertEquals(28, settings.size());
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    // ── datafusion.spill_directory (task 1.1) ──

    public void testSpillDirectoryIsRegistered() {
        try (DataFusionPlugin plugin = new DataFusionPlugin()) {
            List<Setting<?>> settings = plugin.getSettings();
            assertTrue(
                "Plugin must register DATAFUSION_SPILL_DIRECTORY via getSettings()",
                settings.contains(DataFusionPlugin.DATAFUSION_SPILL_DIRECTORY)
            );
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    public void testSpillDirectoryIsFinalAndNodeScope() {
        // Final because DataFusion's DiskManagerMode::Directories is built once at runtime
        // startup; changing the directory at runtime would orphan in-flight spill files.
        assertFalse("datafusion.spill_directory must NOT be dynamic", DataFusionPlugin.DATAFUSION_SPILL_DIRECTORY.isDynamic());
        assertTrue("datafusion.spill_directory must have node scope", DataFusionPlugin.DATAFUSION_SPILL_DIRECTORY.hasNodeScope());
        assertTrue("datafusion.spill_directory must be Final", DataFusionPlugin.DATAFUSION_SPILL_DIRECTORY.isFinal());
    }

    public void testSpillDirectoryDefaultIsEmpty() {
        // Empty default is the sentinel for "spill disabled" — DataFusion will build the
        // runtime in DiskManagerMode::Disabled. Setting the value to a real path opts back
        // into spill. This preserves a sensible default for self-managed clusters that
        // never configured the setting.
        assertEquals("", DataFusionPlugin.DATAFUSION_SPILL_DIRECTORY.get(Settings.EMPTY));
    }

    public void testSpillDirectoryAcceptsEmptyValue() {
        // Explicitly setting the value to empty must also pass validation — same effect as
        // leaving it unset (spill disabled).
        Settings s = Settings.builder().put("datafusion.spill_directory", "").build();
        assertEquals("", DataFusionPlugin.DATAFUSION_SPILL_DIRECTORY.get(s));
    }

    public void testSpillDirectoryAcceptsValidExistingPath() throws Exception {
        Path tmp = createTempDir();
        Settings s = Settings.builder().put("datafusion.spill_directory", tmp.toString()).build();
        assertEquals(tmp.toString(), DataFusionPlugin.DATAFUSION_SPILL_DIRECTORY.get(s));
    }

    public void testSpillDirectoryAcceptsPathThatDoesNotYetExist() throws Exception {
        // The leaf directory may not exist yet at plugin-startup time (host-fleet boot script
        // could still be mounting the volume). Validator accepts on syntactic grounds only;
        // runtime spill writes surface any permission/mount issues.
        Path existingParent = createTempDir();
        Path nonExistentLeaf = existingParent.resolve("spill-not-yet-mounted");
        assertTrue(Files.notExists(nonExistentLeaf));
        Settings s = Settings.builder().put("datafusion.spill_directory", nonExistentLeaf.toString()).build();
        assertEquals(nonExistentLeaf.toString(), DataFusionPlugin.DATAFUSION_SPILL_DIRECTORY.get(s));
    }

    public void testSpillDirectoryRejectsInvalidPathSyntax() {
        // Nul-byte path is unparseable by Path.of; the validator must reject it with a
        // clear error message that references the setting key.
        Settings s = Settings.builder().put("datafusion.spill_directory", "\u0000bad\u0000path").build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> DataFusionPlugin.DATAFUSION_SPILL_DIRECTORY.get(s));
        assertTrue(
            "error message should reference the setting key, got: " + e.getMessage(),
            e.getMessage().contains("datafusion.spill_directory")
        );
    }

    public void testDatafusionSettingsIsNullBeforeCreateComponents() {
        try (DataFusionPlugin plugin = new DataFusionPlugin()) {
            assertNull(plugin.getDatafusionSettings());
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    public void testDeriveMemoryPoolLimitDefaultUnsetReturnsLongMaxValue() {
        // AC explicitly unconfigured — the default must be Long.MAX_VALUE (unbounded),
        // preserving pre-AC behaviour. The default for node.native_memory.limit is now
        // ram - heap, so to test the "unset" branch we must explicitly set it to 0.
        Settings s = Settings.builder().put("node.native_memory.limit", "0b").build();
        assertEquals(Long.toString(Long.MAX_VALUE), DataFusionPlugin.deriveMemoryPoolLimitDefault(s));
    }

    public void testDeriveMemoryPoolLimitDefaultUsesNativeMemoryLimit() {
        // 10 GiB native memory limit — default takes 75% straight from limit, not
        // from limit - buffer_percent (which is AC's throttle margin, not a framework
        // budget reduction). 75% of 10 GiB.
        Settings s = Settings.builder().put("node.native_memory.limit", "10gb").build();
        long expected = (10L * 1024 * 1024 * 1024) * 75 / 100;
        assertEquals(Long.toString(expected), DataFusionPlugin.deriveMemoryPoolLimitDefault(s));
    }

    public void testDeriveMemoryPoolLimitDefaultIgnoresBufferPercent() {
        // node.native_memory.buffer_percent is AC's throttle margin. The framework default
        // takes its fraction off node.native_memory.limit directly so the buffer can sit
        // between AC's throttle threshold and the framework's hard cap.
        // 1000 bytes limit, 20% buffer => pool max still 75% of 1000 = 750.
        Settings s = Settings.builder().put("node.native_memory.limit", "1000b").put("node.native_memory.buffer_percent", 20).build();
        assertEquals("750", DataFusionPlugin.deriveMemoryPoolLimitDefault(s));
    }

    public void testMemoryPoolLimitSettingExposesDerivedDefault() {
        Settings s = Settings.builder().put("node.native_memory.limit", "10gb").build();
        long expected = (10L * 1024 * 1024 * 1024) * 75 / 100;
        assertEquals(Long.valueOf(expected), DataFusionPlugin.DATAFUSION_MEMORY_POOL_LIMIT.get(s));
    }

    public void testMemoryPoolLimitSettingExplicitOverridesDerived() {
        Settings s = Settings.builder().put("node.native_memory.limit", "10gb").put("datafusion.memory_pool_limit_bytes", 1024L).build();
        assertEquals(Long.valueOf(1024L), DataFusionPlugin.DATAFUSION_MEMORY_POOL_LIMIT.get(s));
    }

    public void testMemoryPoolLimitRejectsNegative() {
        Settings s = Settings.builder().put("datafusion.memory_pool_limit_bytes", -1L).build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DataFusionPlugin.DATAFUSION_MEMORY_POOL_LIMIT.get(s)
        );
        assertTrue(e.getMessage().contains("must be >= 0"));
    }

    public void testDeriveSpillLimitDefaultWithEmptySpillDirReturnsZero() {
        Settings settings = Settings.builder().put("datafusion.spill_directory", "").build();
        String defaultValue = DataFusionPlugin.deriveSpillLimitDefault(settings);
        assertEquals("Empty spill_directory must yield default 0 (spill disabled)", "0", defaultValue);
    }

    public void testDeriveSpillLimitDefaultWithValidSpillDirReturnsFractionOfTotal() throws Exception {
        Path spillDir = createTempDir();
        Settings settings = Settings.builder().put("datafusion.spill_directory", spillDir.toString()).build();
        long total = Environment.getFileStore(spillDir).getTotalSpace();
        assertTrue("test host must report a non-zero total space for the temp dir", total > 0);
        long expected = (long) (total * 0.80);
        long actual = Long.parseLong(DataFusionPlugin.deriveSpillLimitDefault(settings));
        assertEquals("Default must be 80% of the spill volume's total space", expected, actual);
    }

    public void testDeriveSpillLimitDefaultWithMissingSpillDirReturnsFallback() {
        // Path that doesn't exist → getFileStore throws IOException → fallback applies.
        Settings settings = Settings.builder()
            .put("datafusion.spill_directory", "/nonexistent/path/that/does/not/exist/spill-test-12345")
            .build();
        long actual = Long.parseLong(DataFusionPlugin.deriveSpillLimitDefault(settings));
        assertEquals("Probe failure must fall back to 8 GiB", 8L * 1024 * 1024 * 1024, actual);
    }

    public void testValidateSpillLimitAcceptsValueAtOrBelowDiskCapacity() throws Exception {
        Path spillDir = createTempDir();
        long total = Environment.getFileStore(spillDir).getTotalSpace();
        Settings settings = Settings.builder()
            .put("datafusion.spill_directory", spillDir.toString())
            .put("datafusion.spill_memory_limit_bytes", total / 2)
            .build();
        // get() runs the parser AND the validator; no throw = pass.
        long parsed = DataFusionPlugin.DATAFUSION_SPILL_MEMORY_LIMIT.get(settings);
        assertEquals(total / 2, parsed);
    }

    public void testValidateSpillLimitRejectsValueExceedingDiskCapacity() throws Exception {
        Path spillDir = createTempDir();
        long total = Environment.getFileStore(spillDir).getTotalSpace();
        Settings settings = Settings.builder()
            .put("datafusion.spill_directory", spillDir.toString())
            .put("datafusion.spill_memory_limit_bytes", total + 1)
            .build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DataFusionPlugin.DATAFUSION_SPILL_MEMORY_LIMIT.get(settings)
        );
        assertTrue(
            "expected message to mention exceeding capacity, got: " + e.getMessage(),
            e.getMessage().contains("exceeds spill volume capacity")
        );
    }

    public void testValidateSpillLimitRejectsNonZeroWhenSpillDirUnset() {
        Settings settings = Settings.builder()
            .put("datafusion.spill_directory", "")
            .put("datafusion.spill_memory_limit_bytes", 1024L * 1024 * 1024)
            .build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DataFusionPlugin.DATAFUSION_SPILL_MEMORY_LIMIT.get(settings)
        );
        assertTrue(
            "expected message to mention spill_directory unset, got: " + e.getMessage(),
            e.getMessage().contains("datafusion.spill_directory is unset")
        );
    }

    public void testValidateSpillLimitAcceptsZeroWhenSpillDirUnset() {
        Settings settings = Settings.builder().put("datafusion.spill_directory", "").put("datafusion.spill_memory_limit_bytes", 0L).build();
        long parsed = DataFusionPlugin.DATAFUSION_SPILL_MEMORY_LIMIT.get(settings);
        assertEquals("Zero with spill disabled is valid", 0L, parsed);
    }

    public void testValidateSpillLimitSkipsCapacityCheckWhenProbeFails() {
        // When the spill directory cannot be probed (e.g. a path that doesn't exist),
        // the validator must fail open: skip the capacity check rather than block the
        // operator. Same fail-open behavior as deriveSpillLimitDefault uses for its 8 GiB fallback.
        Settings settings = Settings.builder()
            .put("datafusion.spill_directory", "/nonexistent/path/that/does/not/exist/spill-test-12345")
            .put("datafusion.spill_memory_limit_bytes", 1024L * 1024 * 1024 * 1024) // 1 TiB
            .build();
        // Even though 1 TiB would likely exceed any real disk, the probe fails so the check is skipped.
        long parsed = DataFusionPlugin.DATAFUSION_SPILL_MEMORY_LIMIT.get(settings);
        assertEquals("probe failure must skip the volume-capacity check", 1024L * 1024 * 1024 * 1024, parsed);
    }
}
