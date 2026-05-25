/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.indices.pollingingest.StreamPoller;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.opensearch.indices.pollingingest.IngestionErrorStrategy.ErrorStrategy.DROP;

public class IngestionSourceTests extends OpenSearchTestCase {

    private final IngestionSource.PointerInitReset pointerInitReset = new IngestionSource.PointerInitReset(
        StreamPoller.ResetState.RESET_BY_OFFSET,
        "1000"
    );

    public void testConstructorAndGetters() {
        Map<String, Object> params = new HashMap<>();
        params.put("key", "value");
        IngestionSource source = new IngestionSource.Builder("type").setParams(params)
            .setPointerInitReset(pointerInitReset)
            .setErrorStrategy(DROP)
            .setBlockingQueueSize(1000)
            .setPointerBasedLagUpdateInterval(TimeValue.timeValueSeconds(1))
            .build();

        assertEquals("type", source.getType());
        assertEquals(StreamPoller.ResetState.RESET_BY_OFFSET, source.getPointerInitReset().getType());
        assertEquals("1000", source.getPointerInitReset().getValue());
        assertEquals(DROP, source.getErrorStrategy());
        assertEquals(params, source.params());
        assertEquals(1000, source.getMaxPollSize());
        assertEquals(1000, source.getPollTimeout());
        assertEquals(1000, source.getBlockingQueueSize());
        assertEquals(1, source.getPointerBasedLagUpdateInterval().getSeconds());
    }

    public void testEquals() {
        Map<String, Object> params1 = new HashMap<>();
        params1.put("key", "value");
        IngestionSource source1 = new IngestionSource.Builder("type").setParams(params1)
            .setPointerInitReset(pointerInitReset)
            .setErrorStrategy(DROP)
            .setMaxPollSize(500)
            .setPollTimeout(500)
            .build();

        Map<String, Object> params2 = new HashMap<>();
        params2.put("key", "value");
        IngestionSource source2 = new IngestionSource.Builder("type").setParams(params2)
            .setPointerInitReset(pointerInitReset)
            .setErrorStrategy(DROP)
            .setMaxPollSize(500)
            .setPollTimeout(500)
            .build();
        assertTrue(source1.equals(source2));
        assertTrue(source2.equals(source1));

        IngestionSource source3 = new IngestionSource.Builder("differentType").setParams(params1)
            .setPointerInitReset(pointerInitReset)
            .setErrorStrategy(DROP)
            .build();
        assertFalse(source1.equals(source3));
    }

    public void testHashCode() {
        Map<String, Object> params1 = new HashMap<>();
        params1.put("key", "value");
        IngestionSource source1 = new IngestionSource.Builder("type").setParams(params1)
            .setPointerInitReset(pointerInitReset)
            .setErrorStrategy(DROP)
            .setMaxPollSize(500)
            .setPollTimeout(500)
            .build();

        Map<String, Object> params2 = new HashMap<>();
        params2.put("key", "value");
        IngestionSource source2 = new IngestionSource.Builder("type").setParams(params2)
            .setPointerInitReset(pointerInitReset)
            .setErrorStrategy(DROP)
            .setMaxPollSize(500)
            .setPollTimeout(500)
            .build();
        assertEquals(source1.hashCode(), source2.hashCode());

        IngestionSource source3 = new IngestionSource.Builder("differentType").setParams(params1)
            .setPointerInitReset(pointerInitReset)
            .setErrorStrategy(DROP)
            .build();
        assertNotEquals(source1.hashCode(), source3.hashCode());
    }

    public void testToString() {
        Map<String, Object> params = new HashMap<>();
        params.put("key", "value");
        IngestionSource source = new IngestionSource.Builder("type").setParams(params)
            .setPointerInitReset(pointerInitReset)
            .setErrorStrategy(DROP)
            .build();
        String expected =
            "IngestionSource{type='type',pointer_init_reset='PointerInitReset{type='RESET_BY_OFFSET', value=1000}',error_strategy='DROP', params={key=value}, maxPollSize=1000, pollTimeout=1000, numProcessorThreads=1, blockingQueueSize=100, allActiveIngestion=false, pointerBasedLagUpdateInterval=10s, mapperType='DEFAULT', mapperSettings={}, warmupConfig=WarmupConfig[timeout=-1, lagThreshold=100], sourcePartitionStrategy='simple'}";
        assertEquals(expected, source.toString());
    }

    public void testAllActiveIngestionConstructorAndGetter() {
        Map<String, Object> params = new HashMap<>();
        params.put("key", "value");

        // Test with all-active ingestion enabled
        IngestionSource sourceEnabled = new IngestionSource.Builder("type").setParams(params)
            .setPointerInitReset(pointerInitReset)
            .setErrorStrategy(DROP)
            .setAllActiveIngestion(true)
            .build();

        assertTrue("All-active ingestion should be enabled", sourceEnabled.isAllActiveIngestionEnabled());

        // Test with all-active ingestion disabled
        IngestionSource sourceDisabled = new IngestionSource.Builder("type").setParams(params)
            .setPointerInitReset(pointerInitReset)
            .setErrorStrategy(DROP)
            .setAllActiveIngestion(false)
            .build();

        assertFalse("All-active ingestion should be disabled", sourceDisabled.isAllActiveIngestionEnabled());

        IngestionSource ingestionSourceClone = new IngestionSource.Builder(sourceEnabled).build();
        assertTrue(ingestionSourceClone.isAllActiveIngestionEnabled());
    }

    public void testMapperSettings() {
        Map<String, Object> params = new HashMap<>();
        params.put("key", "value");
        Map<String, Object> mapperSettings = new HashMap<>();
        mapperSettings.put("id_field", "user_id");
        mapperSettings.put("version_field", "timestamp");
        mapperSettings.put("op_type_field", "is_deleted");

        IngestionSource source = new IngestionSource.Builder("type").setParams(params)
            .setPointerInitReset(pointerInitReset)
            .setErrorStrategy(DROP)
            .setMapperSettings(mapperSettings)
            .build();

        assertEquals(mapperSettings, source.getMapperSettings());
        assertEquals("user_id", source.getMapperSettings().get("id_field"));
        assertEquals("timestamp", source.getMapperSettings().get("version_field"));
        assertEquals("is_deleted", source.getMapperSettings().get("op_type_field"));

        // Test copy constructor preserves mapper settings
        IngestionSource copy = new IngestionSource.Builder(source).build();
        assertEquals(mapperSettings, copy.getMapperSettings());

        // Test equals with mapper settings
        IngestionSource source2 = new IngestionSource.Builder("type").setParams(params)
            .setPointerInitReset(pointerInitReset)
            .setErrorStrategy(DROP)
            .setMapperSettings(new HashMap<>(mapperSettings))
            .build();
        assertEquals(source, source2);
        assertEquals(source.hashCode(), source2.hashCode());

        // Test empty mapper settings by default
        IngestionSource sourceNoMapperSettings = new IngestionSource.Builder("type").setParams(params)
            .setPointerInitReset(pointerInitReset)
            .setErrorStrategy(DROP)
            .build();
        assertTrue(sourceNoMapperSettings.getMapperSettings().isEmpty());
    }

    public void testWarmupConfigurationConstructorAndGetters() {
        Map<String, Object> params = new HashMap<>();
        params.put("key", "value");

        // Test with warmup configuration (timeout >= 0 means enabled)
        IngestionSource source = new IngestionSource.Builder("type").setParams(params)
            .setPointerInitReset(pointerInitReset)
            .setErrorStrategy(DROP)
            .setWarmupTimeout(TimeValue.timeValueMinutes(10))
            .setWarmupLagThreshold(100)
            .build();

        assertTrue("Warmup should be enabled", source.getWarmupConfig().isEnabled());
        assertEquals(TimeValue.timeValueMinutes(10), source.getWarmupConfig().timeout());
        assertEquals(100, source.getWarmupConfig().lagThreshold());
    }

    public void testWarmupConfigurationDefaults() {
        // Test default warmup values (timeout=-1 means disabled)
        IngestionSource source = new IngestionSource.Builder("type").build();

        assertFalse("Warmup should be disabled by default", source.getWarmupConfig().isEnabled());
        assertEquals(TimeValue.timeValueMillis(-1), source.getWarmupConfig().timeout());
        assertEquals(100, source.getWarmupConfig().lagThreshold());
    }

    public void testWarmupConfigurationEquality() {
        Map<String, Object> params = new HashMap<>();
        params.put("key", "value");

        IngestionSource source1 = new IngestionSource.Builder("type").setParams(params)
            .setWarmupTimeout(TimeValue.timeValueMinutes(10))
            .setWarmupLagThreshold(100)
            .build();

        IngestionSource source2 = new IngestionSource.Builder("type").setParams(params)
            .setWarmupTimeout(TimeValue.timeValueMinutes(10))
            .setWarmupLagThreshold(100)
            .build();

        assertEquals(source1, source2);
        assertEquals(source1.hashCode(), source2.hashCode());

        // Test inequality with different warmup settings (disabled vs enabled)
        IngestionSource source3 = new IngestionSource.Builder("type").setParams(params)
            .setWarmupTimeout(TimeValue.timeValueMillis(-1))
            .setWarmupLagThreshold(100)
            .build();

        assertNotEquals(source1, source3);
    }

    public void testWarmupConfigurationCopiedByBuilder() {
        IngestionSource original = new IngestionSource.Builder("type").setWarmupTimeout(TimeValue.timeValueMinutes(10))
            .setWarmupLagThreshold(500)
            .build();

        // Create a copy using the copy constructor
        IngestionSource copy = new IngestionSource.Builder(original).build();

        assertEquals(original.getWarmupConfig().timeout(), copy.getWarmupConfig().timeout());
        assertEquals(original.getWarmupConfig().lagThreshold(), copy.getWarmupConfig().lagThreshold());
    }

    public void testWarmupConfigClass() {
        IngestionSource.WarmupConfig config1 = new IngestionSource.WarmupConfig(TimeValue.timeValueMinutes(10), 100);

        assertTrue(config1.isEnabled());
        assertEquals(TimeValue.timeValueMinutes(10), config1.timeout());
        assertEquals(100, config1.lagThreshold());

        // Test equals and hashCode
        IngestionSource.WarmupConfig config2 = new IngestionSource.WarmupConfig(TimeValue.timeValueMinutes(10), 100);
        assertEquals(config1, config2);
        assertEquals(config1.hashCode(), config2.hashCode());

        // Test inequality
        IngestionSource.WarmupConfig config3 = new IngestionSource.WarmupConfig(TimeValue.timeValueMillis(-1), 100);
        assertNotEquals(config1, config3);
        assertFalse(config3.isEnabled());

        // Test toString
        String expectedToString = "WarmupConfig[timeout=10m, lagThreshold=100]";
        assertEquals(expectedToString, config1.toString());
    }

    public void testSetWarmupConfig() {
        IngestionSource.WarmupConfig warmupConfig = new IngestionSource.WarmupConfig(TimeValue.timeValueMinutes(15), 200);

        IngestionSource source = new IngestionSource.Builder("type").setWarmupConfig(warmupConfig).build();

        assertEquals(warmupConfig, source.getWarmupConfig());
        assertTrue(source.getWarmupConfig().isEnabled());
        assertEquals(TimeValue.timeValueMinutes(15), source.getWarmupConfig().timeout());
        assertEquals(200, source.getWarmupConfig().lagThreshold());
    }

    // ---- SourcePartitionStrategy enum tests ----

    public void testSourcePartitionStrategyGetName() {
        assertEquals("simple", IngestionSource.SourcePartitionStrategy.SIMPLE.getName());
        assertEquals("modulo", IngestionSource.SourcePartitionStrategy.MODULO.getName());
    }

    public void testSourcePartitionStrategyToString() {
        // toString() should match getName()
        assertEquals("simple", IngestionSource.SourcePartitionStrategy.SIMPLE.toString());
        assertEquals("modulo", IngestionSource.SourcePartitionStrategy.MODULO.toString());
    }

    public void testSourcePartitionStrategyFromString() {
        assertEquals(IngestionSource.SourcePartitionStrategy.SIMPLE, IngestionSource.SourcePartitionStrategy.fromString("simple"));
        assertEquals(IngestionSource.SourcePartitionStrategy.MODULO, IngestionSource.SourcePartitionStrategy.fromString("modulo"));
    }

    public void testSourcePartitionStrategyFromStringIsCaseInsensitive() {
        assertEquals(IngestionSource.SourcePartitionStrategy.SIMPLE, IngestionSource.SourcePartitionStrategy.fromString("SIMPLE"));
        assertEquals(IngestionSource.SourcePartitionStrategy.SIMPLE, IngestionSource.SourcePartitionStrategy.fromString("Simple"));
        assertEquals(IngestionSource.SourcePartitionStrategy.MODULO, IngestionSource.SourcePartitionStrategy.fromString("MODULO"));
        assertEquals(IngestionSource.SourcePartitionStrategy.MODULO, IngestionSource.SourcePartitionStrategy.fromString("Modulo"));
    }

    public void testSourcePartitionStrategyFromStringInvalid() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> IngestionSource.SourcePartitionStrategy.fromString("unknown_strategy")
        );
        assertTrue(e.getMessage().contains("Unknown partition strategy"));
    }

    // ---- IngestionSource sourcePartitionStrategy field tests ----

    public void testSourcePartitionStrategyDefault() {
        // Default builder should produce SIMPLE strategy
        IngestionSource source = new IngestionSource.Builder("type").build();
        assertEquals(IngestionSource.SourcePartitionStrategy.SIMPLE, source.getSourcePartitionStrategy());
    }

    public void testSourcePartitionStrategySetAndGet() {
        IngestionSource source = new IngestionSource.Builder("type").setSourcePartitionStrategy(
            IngestionSource.SourcePartitionStrategy.MODULO
        ).build();
        assertEquals(IngestionSource.SourcePartitionStrategy.MODULO, source.getSourcePartitionStrategy());
    }

    public void testSourcePartitionStrategyAffectsEquals() {
        IngestionSource simpleSource = new IngestionSource.Builder("type").setSourcePartitionStrategy(
            IngestionSource.SourcePartitionStrategy.SIMPLE
        ).build();
        IngestionSource moduloSource = new IngestionSource.Builder("type").setSourcePartitionStrategy(
            IngestionSource.SourcePartitionStrategy.MODULO
        ).build();
        assertNotEquals(simpleSource, moduloSource);
        assertNotEquals(simpleSource.hashCode(), moduloSource.hashCode());

        IngestionSource moduloSource2 = new IngestionSource.Builder("type").setSourcePartitionStrategy(
            IngestionSource.SourcePartitionStrategy.MODULO
        ).build();
        assertEquals(moduloSource, moduloSource2);
        assertEquals(moduloSource.hashCode(), moduloSource2.hashCode());
    }
}
