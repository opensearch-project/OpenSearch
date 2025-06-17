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

package org.opensearch.script;

import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.admin.cluster.storedscripts.GetStoredScriptRequest;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.env.Environment;
import org.opensearch.search.lookup.FieldsLookup;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.opensearch.script.ScriptService.SCRIPT_CACHE_EXPIRE_SETTING;
import static org.opensearch.script.ScriptService.SCRIPT_CACHE_SIZE_SETTING;
import static org.opensearch.script.ScriptService.SCRIPT_GENERAL_CACHE_EXPIRE_SETTING;
import static org.opensearch.script.ScriptService.SCRIPT_GENERAL_CACHE_SIZE_SETTING;
import static org.opensearch.script.ScriptService.SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING;
import static org.opensearch.script.ScriptService.SCRIPT_MAX_COMPILATIONS_RATE_SETTING;
import static org.opensearch.search.lookup.SearchLookup.UNKNOWN_SHARD_ID;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;

public class ScriptServiceTests extends OpenSearchTestCase {

    private ScriptEngine scriptEngine;
    private Map<String, ScriptEngine> engines;
    private Map<String, ScriptContext<?>> contexts;
    private ScriptService scriptService;
    private Settings baseSettings;
    private ClusterSettings clusterSettings;

    @Before
    public void setup() throws IOException {
        baseSettings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();
        for (int i = 0; i < 20; ++i) {
            scripts.put(i + "+" + i, p -> null); // only care about compilation, not execution
        }
        scripts.put("script", p -> null);
        scriptEngine = new MockScriptEngine(Script.DEFAULT_SCRIPT_LANG, scripts, Collections.emptyMap());
        // prevent duplicates using map
        contexts = new HashMap<>(ScriptModule.CORE_CONTEXTS);
        engines = new HashMap<>();
        engines.put(scriptEngine.getType(), scriptEngine);
        engines.put("test", new MockScriptEngine("test", scripts, Collections.emptyMap()));
        logger.info("--> setup script service");
    }

    private void buildScriptService(Settings additionalSettings) throws IOException {
        Settings finalSettings = Settings.builder().put(baseSettings).put(additionalSettings).build();
        scriptService = new ScriptService(finalSettings, engines, contexts) {
            @Override
            Map<String, StoredScriptSource> getScriptsFromClusterState() {
                Map<String, StoredScriptSource> scripts = new HashMap<>();
                scripts.put("test1", new StoredScriptSource("test", "1+1", Collections.emptyMap()));
                scripts.put("test2", new StoredScriptSource("test", "1", Collections.emptyMap()));
                return scripts;
            }

            @Override
            protected StoredScriptSource getScriptFromClusterState(String id) {
                // mock the script that gets retrieved from an index
                return new StoredScriptSource("test", "1+1", Collections.emptyMap());
            }
        };
        clusterSettings = new ClusterSettings(finalSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        scriptService.registerClusterSettingsListeners(clusterSettings);
    }

    public void testMaxCompilationRateSetting() throws Exception {
        assertThat(new ScriptCache.CompilationRate("10/1m"), is(new ScriptCache.CompilationRate(10, TimeValue.timeValueMinutes(1))));
        assertThat(new ScriptCache.CompilationRate("10/60s"), is(new ScriptCache.CompilationRate(10, TimeValue.timeValueMinutes(1))));
        assertException("10/m", IllegalArgumentException.class, "failed to parse [m]");
        assertException("6/1.6m", IllegalArgumentException.class, "failed to parse [1.6m], fractional time values are not supported");
        assertException("foo/bar", IllegalArgumentException.class, "could not parse [foo] as integer in value [foo/bar]");
        assertException("6.0/1m", IllegalArgumentException.class, "could not parse [6.0] as integer in value [6.0/1m]");
        assertException("6/-1m", IllegalArgumentException.class, "time value [-1m] must be positive");
        assertException("6/0m", IllegalArgumentException.class, "time value [0m] must be positive");
        assertException(
            "10",
            IllegalArgumentException.class,
            "parameter must contain a positive integer and a timevalue, i.e. 10/1m, but was [10]"
        );
        assertException(
            "anything",
            IllegalArgumentException.class,
            "parameter must contain a positive integer and a timevalue, i.e. 10/1m, but was [anything]"
        );
        assertException(
            "/1m",
            IllegalArgumentException.class,
            "parameter must contain a positive integer and a timevalue, i.e. 10/1m, but was [/1m]"
        );
        assertException(
            "10/",
            IllegalArgumentException.class,
            "parameter must contain a positive integer and a timevalue, i.e. 10/1m, but was [10/]"
        );
        assertException("-1/1m", IllegalArgumentException.class, "rate [-1] must be positive");
        assertException("10/5s", IllegalArgumentException.class, "time value [5s] must be at least on a one minute resolution");
    }

    private void assertException(String rate, Class<? extends Exception> clazz, String message) {
        Exception e = expectThrows(clazz, () -> new ScriptCache.CompilationRate(rate));
        assertThat(e.getMessage(), is(message));
    }

    public void testNotSupportedDisableDynamicSetting() throws IOException {
        try {
            buildScriptService(
                Settings.builder()
                    .put(ScriptService.DISABLE_DYNAMIC_SCRIPTING_SETTING, randomUnicodeOfLength(randomIntBetween(1, 10)))
                    .build()
            );
            fail("script service should have thrown exception due to non supported script.disable_dynamic setting");
        } catch (IllegalArgumentException e) {
            assertThat(
                e.getMessage(),
                containsString(
                    ScriptService.DISABLE_DYNAMIC_SCRIPTING_SETTING
                        + " is not a supported setting, replace with fine-grained script settings"
                )
            );
        }
    }

    public void testInlineScriptCompiledOnceCache() throws IOException {
        buildScriptService(Settings.EMPTY);
        Script script = new Script(ScriptType.INLINE, "test", "1+1", Collections.emptyMap());
        FieldScript.Factory factoryScript1 = scriptService.compile(script, FieldScript.CONTEXT);
        FieldScript.Factory factoryScript2 = scriptService.compile(script, FieldScript.CONTEXT);
        assertThat(factoryScript1, sameInstance(factoryScript2));
    }

    public void testScriptsUseCachedSourceLookup() throws IOException {
        buildScriptService(Settings.EMPTY);
        Script script = new Script(ScriptType.INLINE, "test", "1+1", Collections.emptyMap());
        FieldScript.Factory factoryScript = scriptService.compile(script, FieldScript.CONTEXT);

        FieldScript.LeafFactory leafFactory = factoryScript.newFactory(
            new HashMap<>(),
            new SearchLookup(null, null, UNKNOWN_SHARD_ID, mock(FieldsLookup.class))
        );

        FieldScript script1 = leafFactory.newInstance(null);
        FieldScript script2 = leafFactory.newInstance(null);

        assertThat(script1.getLeafLookup().source(), sameInstance(script2.getLeafLookup().source()));
    }

    public void testAllowAllScriptTypeSettings() throws IOException {
        buildScriptService(Settings.EMPTY);

        assertCompileAccepted("painless", "script", ScriptType.INLINE, FieldScript.CONTEXT);
        assertCompileAccepted(null, "script", ScriptType.STORED, FieldScript.CONTEXT);
    }

    public void testAllowAllScriptContextSettings() throws IOException {
        buildScriptService(Settings.EMPTY);

        assertCompileAccepted("painless", "script", ScriptType.INLINE, FieldScript.CONTEXT);
        assertCompileAccepted("painless", "script", ScriptType.INLINE, AggregationScript.CONTEXT);
        assertCompileAccepted("painless", "script", ScriptType.INLINE, UpdateScript.CONTEXT);
        assertCompileAccepted("painless", "script", ScriptType.INLINE, IngestScript.CONTEXT);
    }

    public void testAllowSomeScriptTypeSettings() throws IOException {
        Settings.Builder builder = Settings.builder();
        builder.put("script.allowed_types", "inline");
        buildScriptService(builder.build());

        assertCompileAccepted("painless", "script", ScriptType.INLINE, FieldScript.CONTEXT);
        assertCompileRejected(null, "script", ScriptType.STORED, FieldScript.CONTEXT);
    }

    public void testAllowSomeScriptContextSettings() throws IOException {
        Settings.Builder builder = Settings.builder();
        builder.put("script.allowed_contexts", "field, aggs");
        buildScriptService(builder.build());

        assertCompileAccepted("painless", "script", ScriptType.INLINE, FieldScript.CONTEXT);
        assertCompileAccepted("painless", "script", ScriptType.INLINE, AggregationScript.CONTEXT);
        assertCompileRejected("painless", "script", ScriptType.INLINE, UpdateScript.CONTEXT);
    }

    public void testAllowNoScriptTypeSettings() throws IOException {
        Settings.Builder builder = Settings.builder();
        builder.put("script.allowed_types", "none");
        buildScriptService(builder.build());

        assertCompileRejected("painless", "script", ScriptType.INLINE, FieldScript.CONTEXT);
        assertCompileRejected(null, "script", ScriptType.STORED, FieldScript.CONTEXT);
    }

    public void testAllowNoScriptContextSettings() throws IOException {
        Settings.Builder builder = Settings.builder();
        builder.put("script.allowed_contexts", "none");
        buildScriptService(builder.build());

        assertCompileRejected("painless", "script", ScriptType.INLINE, FieldScript.CONTEXT);
        assertCompileRejected("painless", "script", ScriptType.INLINE, AggregationScript.CONTEXT);
    }

    public void testCompileNonRegisteredContext() throws IOException {
        contexts.remove(IngestScript.CONTEXT.name);
        buildScriptService(Settings.EMPTY);

        String type = scriptEngine.getType();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> scriptService.compile(new Script(ScriptType.INLINE, type, "test", Collections.emptyMap()), IngestScript.CONTEXT)
        );
        assertThat(e.getMessage(), containsString("script context [" + IngestScript.CONTEXT.name + "] not supported"));
    }

    public void testCompileCountedInCompilationStats() throws IOException {
        buildScriptService(Settings.EMPTY);
        scriptService.compile(new Script(ScriptType.INLINE, "test", "1+1", Collections.emptyMap()), randomFrom(contexts.values()));
        assertEquals(1L, scriptService.stats().getCompilations());
    }

    public void testMultipleCompilationsCountedInCompilationStats() throws IOException {
        buildScriptService(Settings.EMPTY);
        int numberOfCompilations = randomIntBetween(1, 20);
        for (int i = 0; i < numberOfCompilations; i++) {
            scriptService.compile(
                new Script(ScriptType.INLINE, "test", i + "+" + i, Collections.emptyMap()),
                randomFrom(contexts.values())
            );
        }
        assertEquals(numberOfCompilations, scriptService.stats().getCompilations());
    }

    public void testCompilationStatsOnCacheHit() throws IOException {
        Settings.Builder builder = Settings.builder()
            .put(SCRIPT_GENERAL_CACHE_SIZE_SETTING.getKey(), 1)
            .put(SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), "2/1m");
        buildScriptService(builder.build());
        Script script = new Script(ScriptType.INLINE, "test", "1+1", Collections.emptyMap());
        ScriptContext<?> context = randomFrom(contexts.values());
        scriptService.compile(script, context);
        scriptService.compile(script, context);
        assertEquals(1L, scriptService.stats().getCompilations());
        assertSettingDeprecationsAndWarnings(
            new Setting<?>[] { SCRIPT_GENERAL_CACHE_SIZE_SETTING, SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING }
        );
    }

    public void testIndexedScriptCountedInCompilationStats() throws IOException {
        buildScriptService(Settings.EMPTY);
        ScriptContext<?> ctx = randomFrom(contexts.values());
        scriptService.compile(new Script(ScriptType.STORED, null, "script", Collections.emptyMap()), ctx);
        assertEquals(1L, scriptService.stats().getCompilations());
        assertEquals(1L, scriptService.cacheStats().getContextStats().get(ctx.name).getCompilations());
    }

    public void testCacheEvictionCountedInCacheEvictionsStats() throws IOException {
        Settings.Builder builder = Settings.builder();
        builder.put(SCRIPT_GENERAL_CACHE_SIZE_SETTING.getKey(), 1);
        builder.put(SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), "10/1m");
        buildScriptService(builder.build());
        scriptService.compile(new Script(ScriptType.INLINE, "test", "1+1", Collections.emptyMap()), randomFrom(contexts.values()));
        scriptService.compile(new Script(ScriptType.INLINE, "test", "2+2", Collections.emptyMap()), randomFrom(contexts.values()));
        assertEquals(2L, scriptService.stats().getCompilations());
        assertEquals(2L, scriptService.cacheStats().getGeneralStats().getCompilations());
        assertEquals(1L, scriptService.stats().getCacheEvictions());
        assertEquals(1L, scriptService.cacheStats().getGeneralStats().getCacheEvictions());
        assertSettingDeprecationsAndWarnings(
            new Setting<?>[] { SCRIPT_GENERAL_CACHE_SIZE_SETTING, SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING }
        );
    }

    public void testContextCacheStats() throws IOException {
        ScriptContext<?> contextA = randomFrom(contexts.values());
        String aRate = "2/10m";
        ScriptContext<?> contextB = randomValueOtherThan(contextA, () -> randomFrom(contexts.values()));
        String bRate = "3/10m";
        BiFunction<String, String, String> msg = (rate, ctx) -> ("[script] Too many dynamic script compilations within, max: ["
            + rate
            + "]; please use indexed, or scripts with parameters instead; this limit can be changed by the [script.context."
            + ctx
            + ".max_compilations_rate] setting");
        buildScriptService(
            Settings.builder()
                .put(SCRIPT_CACHE_SIZE_SETTING.getConcreteSettingForNamespace(contextA.name).getKey(), 1)
                .put(SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace(contextA.name).getKey(), aRate)
                .put(SCRIPT_CACHE_SIZE_SETTING.getConcreteSettingForNamespace(contextB.name).getKey(), 2)
                .put(SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace(contextB.name).getKey(), bRate)
                .build()
        );

        // Context A
        scriptService.compile(new Script(ScriptType.INLINE, "test", "1+1", Collections.emptyMap()), contextA);
        scriptService.compile(new Script(ScriptType.INLINE, "test", "2+2", Collections.emptyMap()), contextA);
        GeneralScriptException gse = expectThrows(
            GeneralScriptException.class,
            () -> scriptService.compile(new Script(ScriptType.INLINE, "test", "3+3", Collections.emptyMap()), contextA)
        );
        assertEquals(msg.apply(aRate, contextA.name), gse.getRootCause().getMessage());
        assertEquals(CircuitBreakingException.class, gse.getRootCause().getClass());

        // Context B
        scriptService.compile(new Script(ScriptType.INLINE, "test", "4+4", Collections.emptyMap()), contextB);
        scriptService.compile(new Script(ScriptType.INLINE, "test", "5+5", Collections.emptyMap()), contextB);
        scriptService.compile(new Script(ScriptType.INLINE, "test", "6+6", Collections.emptyMap()), contextB);
        gse = expectThrows(
            GeneralScriptException.class,
            () -> scriptService.compile(new Script(ScriptType.INLINE, "test", "7+7", Collections.emptyMap()), contextB)
        );
        assertEquals(msg.apply(bRate, contextB.name), gse.getRootCause().getMessage());
        gse = expectThrows(
            GeneralScriptException.class,
            () -> scriptService.compile(new Script(ScriptType.INLINE, "test", "8+8", Collections.emptyMap()), contextB)
        );
        assertEquals(msg.apply(bRate, contextB.name), gse.getRootCause().getMessage());
        assertEquals(CircuitBreakingException.class, gse.getRootCause().getClass());

        // Context specific
        ScriptCacheStats stats = scriptService.cacheStats();
        assertEquals(2L, stats.getContextStats().get(contextA.name).getCompilations());
        assertEquals(1L, stats.getContextStats().get(contextA.name).getCacheEvictions());
        assertEquals(1L, stats.getContextStats().get(contextA.name).getCompilationLimitTriggered());

        assertEquals(3L, stats.getContextStats().get(contextB.name).getCompilations());
        assertEquals(1L, stats.getContextStats().get(contextB.name).getCacheEvictions());
        assertEquals(2L, stats.getContextStats().get(contextB.name).getCompilationLimitTriggered());
        assertNull(scriptService.cacheStats().getGeneralStats());

        // Summed up
        assertEquals(5L, scriptService.stats().getCompilations());
        assertEquals(2L, scriptService.stats().getCacheEvictions());
        assertEquals(3L, scriptService.stats().getCompilationLimitTriggered());
    }

    public void testStoreScript() throws Exception {
        BytesReference script = BytesReference.bytes(
            XContentFactory.jsonBuilder()
                .startObject()
                .field("script")
                .startObject()
                .field("lang", "_lang")
                .field("source", "abc")
                .endObject()
                .endObject()
        );
        ScriptMetadata scriptMetadata = ScriptMetadata.putStoredScript(
            null,
            "_id",
            StoredScriptSource.parse(script, MediaTypeRegistry.JSON)
        );
        assertNotNull(scriptMetadata);
        assertEquals("abc", scriptMetadata.getStoredScript("_id").getSource());
    }

    public void testDeleteScript() throws Exception {
        ScriptMetadata scriptMetadata = ScriptMetadata.putStoredScript(
            null,
            "_id",
            StoredScriptSource.parse(new BytesArray("{\"script\": {\"lang\": \"_lang\", \"source\": \"abc\"} }"), MediaTypeRegistry.JSON)
        );
        scriptMetadata = ScriptMetadata.deleteStoredScript(scriptMetadata, "_id");
        assertNotNull(scriptMetadata);
        assertNull(scriptMetadata.getStoredScript("_id"));

        ScriptMetadata errorMetadata = scriptMetadata;
        ResourceNotFoundException e = expectThrows(ResourceNotFoundException.class, () -> {
            ScriptMetadata.deleteStoredScript(errorMetadata, "_id");
        });
        assertEquals("stored script [_id] does not exist and cannot be deleted", e.getMessage());
    }

    public void testGetStoredScript() throws Exception {
        buildScriptService(Settings.EMPTY);
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
            .metadata(
                Metadata.builder()
                    .putCustom(
                        ScriptMetadata.TYPE,
                        new ScriptMetadata.Builder(null).storeScript(
                            "_id",
                            StoredScriptSource.parse(
                                new BytesArray("{\"script\": {\"lang\": \"_lang\", \"source\": \"abc\"} }"),
                                MediaTypeRegistry.JSON
                            )
                        ).build()
                    )
            )
            .build();

        assertEquals("abc", scriptService.getStoredScript(cs, new GetStoredScriptRequest("_id")).getSource());

        cs = ClusterState.builder(new ClusterName("_name")).build();
        assertNull(scriptService.getStoredScript(cs, new GetStoredScriptRequest("_id")));
    }

    public void testMaxSizeLimit() throws Exception {
        buildScriptService(Settings.builder().put(ScriptService.SCRIPT_MAX_SIZE_IN_BYTES.getKey(), 4).build());
        scriptService.compile(new Script(ScriptType.INLINE, "test", "1+1", Collections.emptyMap()), randomFrom(contexts.values()));
        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> {
            scriptService.compile(new Script(ScriptType.INLINE, "test", "10+10", Collections.emptyMap()), randomFrom(contexts.values()));
        });
        assertEquals("exceeded max allowed inline script size in bytes [4] with size [5] for script [10+10]", iae.getMessage());
        clusterSettings.applySettings(Settings.builder().put(ScriptService.SCRIPT_MAX_SIZE_IN_BYTES.getKey(), 6).build());
        scriptService.compile(new Script(ScriptType.INLINE, "test", "10+10", Collections.emptyMap()), randomFrom(contexts.values()));
        clusterSettings.applySettings(Settings.builder().put(ScriptService.SCRIPT_MAX_SIZE_IN_BYTES.getKey(), 5).build());
        scriptService.compile(new Script(ScriptType.INLINE, "test", "10+10", Collections.emptyMap()), randomFrom(contexts.values()));
        iae = expectThrows(IllegalArgumentException.class, () -> {
            clusterSettings.applySettings(Settings.builder().put(ScriptService.SCRIPT_MAX_SIZE_IN_BYTES.getKey(), 2).build());
        });
        assertEquals(
            "script.max_size_in_bytes cannot be set to [2], stored script [test1] exceeds the new value with a size of [3]",
            iae.getMessage()
        );
    }

    public void testConflictContextSettings() throws IOException {
        IllegalArgumentException illegal = expectThrows(IllegalArgumentException.class, () -> {
            buildScriptService(
                Settings.builder()
                    .put(SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), "10/1m")
                    .put(ScriptService.SCRIPT_CACHE_SIZE_SETTING.getConcreteSettingForNamespace("field").getKey(), 123)
                    .build()
            );
        });
        assertEquals(
            "Context cache settings [script.context.field.cache_max_size] requires " + "[script.max_compilations_rate] to be [use-context]",
            illegal.getMessage()
        );

        illegal = expectThrows(IllegalArgumentException.class, () -> {
            buildScriptService(
                Settings.builder()
                    .put(SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), "10/1m")
                    .put(ScriptService.SCRIPT_CACHE_EXPIRE_SETTING.getConcreteSettingForNamespace("ingest").getKey(), "5m")
                    .build()
            );
        });

        assertEquals(
            "Context cache settings [script.context.ingest.cache_expire] requires " + "[script.max_compilations_rate] to be [use-context]",
            illegal.getMessage()
        );

        illegal = expectThrows(IllegalArgumentException.class, () -> {
            buildScriptService(
                Settings.builder()
                    .put(SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), "10/1m")
                    .put(ScriptService.SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace("score").getKey(), "50/5m")
                    .build()
            );
        });

        assertEquals(
            "Context cache settings [script.context.score.max_compilations_rate] requires "
                + "[script.max_compilations_rate] to be [use-context]",
            illegal.getMessage()
        );

        buildScriptService(
            Settings.builder()
                .put(ScriptService.SCRIPT_CACHE_EXPIRE_SETTING.getConcreteSettingForNamespace("ingest").getKey(), "5m")
                .put(ScriptService.SCRIPT_CACHE_SIZE_SETTING.getConcreteSettingForNamespace("field").getKey(), 123)
                .put(ScriptService.SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace("score").getKey(), "50/5m")
                .build()
        );
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING });
    }

    public void testFallbackContextSettings() {
        int cacheSizeBackup = randomIntBetween(0, 1024);
        int cacheSizeFoo = randomValueOtherThan(cacheSizeBackup, () -> randomIntBetween(0, 1024));

        String cacheExpireBackup = randomTimeValue(1, 1000, "h");
        TimeValue cacheExpireBackupParsed = TimeValue.parseTimeValue(cacheExpireBackup, "");
        String cacheExpireFoo = randomValueOtherThan(cacheExpireBackup, () -> randomTimeValue(1, 1000, "h"));
        TimeValue cacheExpireFooParsed = TimeValue.parseTimeValue(cacheExpireFoo, "");

        Settings s = Settings.builder()
            .put(SCRIPT_GENERAL_CACHE_SIZE_SETTING.getKey(), cacheSizeBackup)
            .put(ScriptService.SCRIPT_CACHE_SIZE_SETTING.getConcreteSettingForNamespace("foo").getKey(), cacheSizeFoo)
            .put(SCRIPT_GENERAL_CACHE_EXPIRE_SETTING.getKey(), cacheExpireBackup)
            .put(ScriptService.SCRIPT_CACHE_EXPIRE_SETTING.getConcreteSettingForNamespace("foo").getKey(), cacheExpireFoo)
            .build();

        assertEquals(cacheSizeFoo, ScriptService.SCRIPT_CACHE_SIZE_SETTING.getConcreteSettingForNamespace("foo").get(s).intValue());
        assertEquals(cacheSizeBackup, ScriptService.SCRIPT_CACHE_SIZE_SETTING.getConcreteSettingForNamespace("bar").get(s).intValue());

        assertEquals(cacheExpireFooParsed, ScriptService.SCRIPT_CACHE_EXPIRE_SETTING.getConcreteSettingForNamespace("foo").get(s));
        assertEquals(cacheExpireBackupParsed, ScriptService.SCRIPT_CACHE_EXPIRE_SETTING.getConcreteSettingForNamespace("bar").get(s));
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { SCRIPT_GENERAL_CACHE_SIZE_SETTING, SCRIPT_GENERAL_CACHE_EXPIRE_SETTING });
    }

    public void testUseContextSettingValue() {
        Settings s = Settings.builder()
            .put(ScriptService.SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), ScriptService.USE_CONTEXT_RATE_KEY)
            .put(
                ScriptService.SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace("foo").getKey(),
                ScriptService.USE_CONTEXT_RATE_KEY
            )
            .build();

        assertEquals(ScriptService.SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.get(s), ScriptService.USE_CONTEXT_RATE_VALUE);

        IllegalArgumentException illegal = expectThrows(IllegalArgumentException.class, () -> {
            ScriptService.SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getAsMap(s);
        });

        assertEquals("parameter must contain a positive integer and a timevalue, i.e. 10/1m, but was [use-context]", illegal.getMessage());
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING });
    }

    public void testCacheHolderGeneralConstructor() throws IOException {
        String compilationRate = "77/5m";
        buildScriptService(Settings.builder().put(SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), compilationRate).build());

        ScriptService.CacheHolder holder = scriptService.cacheHolder.get();

        assertNotNull(holder.general);
        assertNull(holder.contextCache);
        assertEquals(holder.general.rate, new ScriptCache.CompilationRate(compilationRate));
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING });
    }

    public void testCacheHolderContextConstructor() throws IOException {
        String a = randomFrom(contexts.keySet());
        String b = randomValueOtherThan(a, () -> randomFrom(contexts.keySet()));
        String aCompilationRate = "77/5m";
        String bCompilationRate = "78/6m";

        buildScriptService(
            Settings.builder()
                .put(SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace(a).getKey(), aCompilationRate)
                .put(SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace(b).getKey(), bCompilationRate)
                .build()
        );

        assertNull(scriptService.cacheHolder.get().general);
        assertNotNull(scriptService.cacheHolder.get().contextCache);
        assertEquals(contexts.keySet(), scriptService.cacheHolder.get().contextCache.keySet());

        assertEquals(new ScriptCache.CompilationRate(aCompilationRate), scriptService.cacheHolder.get().contextCache.get(a).get().rate);
        assertEquals(new ScriptCache.CompilationRate(bCompilationRate), scriptService.cacheHolder.get().contextCache.get(b).get().rate);
    }

    public void testCompilationRateUnlimitedContextOnly() throws IOException {
        IllegalArgumentException illegal = expectThrows(IllegalArgumentException.class, () -> {
            buildScriptService(
                Settings.builder()
                    .put(SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), ScriptService.UNLIMITED_COMPILATION_RATE_KEY)
                    .build()
            );
        });
        assertEquals("parameter must contain a positive integer and a timevalue, i.e. 10/1m, but was [unlimited]", illegal.getMessage());

        // Should not throw.
        buildScriptService(
            Settings.builder()
                .put(
                    SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace("ingest").getKey(),
                    ScriptService.UNLIMITED_COMPILATION_RATE_KEY
                )
                .put(
                    SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace("field").getKey(),
                    ScriptService.UNLIMITED_COMPILATION_RATE_KEY
                )
                .put(SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), ScriptService.USE_CONTEXT_RATE_KEY)
                .build()
        );
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING });
    }

    public void testDisableCompilationRateSetting() throws IOException {
        IllegalArgumentException illegal = expectThrows(IllegalArgumentException.class, () -> {
            buildScriptService(
                Settings.builder()
                    .put("script.context.ingest.max_compilations_rate", "76/10m")
                    .put("script.context.field.max_compilations_rate", "77/10m")
                    .put("script.disable_max_compilations_rate", true)
                    .build()
            );
        });
        assertEquals(
            "Cannot set custom context compilation rates [script.context.field.max_compilations_rate, "
                + "script.context.ingest.max_compilations_rate] if compile rates disabled via "
                + "[script.disable_max_compilations_rate]",
            illegal.getMessage()
        );

        illegal = expectThrows(IllegalArgumentException.class, () -> {
            buildScriptService(
                Settings.builder().put("script.disable_max_compilations_rate", true).put("script.max_compilations_rate", "76/10m").build()
            );
        });
        assertEquals(
            "Cannot set custom general compilation rates [script.max_compilations_rate] "
                + "to [76/10m] if compile rates disabled via [script.disable_max_compilations_rate]",
            illegal.getMessage()
        );

        buildScriptService(Settings.builder().put("script.disable_max_compilations_rate", true).build());
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING });
    }

    public void testCacheHolderChangeSettings() throws IOException {
        Set<String> contextNames = contexts.keySet();
        String a = randomFrom(contextNames);
        String aRate = "77/5m";
        String b = randomValueOtherThan(a, () -> randomFrom(contextNames));
        String bRate = "78/6m";
        String c = randomValueOtherThanMany(s -> a.equals(s) || b.equals(s), () -> randomFrom(contextNames));
        String compilationRate = "77/5m";
        ScriptCache.CompilationRate generalRate = new ScriptCache.CompilationRate(compilationRate);

        Settings s = Settings.builder().put(SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), compilationRate).build();

        buildScriptService(s);

        assertNotNull(scriptService.cacheHolder.get().general);
        // Set should not throw when using general cache
        scriptService.cacheHolder.get().set(c, scriptService.contextCache(s, contexts.get(c)));
        assertNull(scriptService.cacheHolder.get().contextCache);
        assertEquals(generalRate, scriptService.cacheHolder.get().general.rate);

        scriptService.setCacheHolder(
            Settings.builder()
                .put(SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace(a).getKey(), aRate)
                .put(SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace(b).getKey(), bRate)
                .put(
                    SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace(c).getKey(),
                    ScriptService.UNLIMITED_COMPILATION_RATE_KEY
                )
                .build()
        );

        assertNull(scriptService.cacheHolder.get().general);
        assertNotNull(scriptService.cacheHolder.get().contextCache);
        // get of missing context should be null
        assertNull(
            scriptService.cacheHolder.get().get(randomValueOtherThanMany(contexts.keySet()::contains, () -> randomAlphaOfLength(8)))
        );
        assertEquals(contexts.keySet(), scriptService.cacheHolder.get().contextCache.keySet());

        assertEquals(new ScriptCache.CompilationRate(aRate), scriptService.cacheHolder.get().contextCache.get(a).get().rate);
        assertEquals(new ScriptCache.CompilationRate(bRate), scriptService.cacheHolder.get().contextCache.get(b).get().rate);
        assertEquals(ScriptCache.UNLIMITED_COMPILATION_RATE, scriptService.cacheHolder.get().contextCache.get(c).get().rate);

        scriptService.cacheHolder.get()
            .set(
                b,
                scriptService.contextCache(
                    Settings.builder().put(SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace(b).getKey(), aRate).build(),
                    contexts.get(b)
                )
            );
        assertEquals(new ScriptCache.CompilationRate(aRate), scriptService.cacheHolder.get().contextCache.get(b).get().rate);

        scriptService.setCacheHolder(s);
        assertNotNull(scriptService.cacheHolder.get().general);
        assertNull(scriptService.cacheHolder.get().contextCache);
        assertEquals(generalRate, scriptService.cacheHolder.get().general.rate);

        scriptService.setCacheHolder(Settings.builder().put(SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), bRate).build());

        assertNotNull(scriptService.cacheHolder.get().general);
        assertNull(scriptService.cacheHolder.get().contextCache);
        assertEquals(new ScriptCache.CompilationRate(bRate), scriptService.cacheHolder.get().general.rate);

        ScriptService.CacheHolder holder = scriptService.cacheHolder.get();
        scriptService.setCacheHolder(Settings.builder().put(SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), bRate).build());
        assertEquals(holder, scriptService.cacheHolder.get());

        assertSettingDeprecationsAndWarnings(new Setting<?>[] { SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING });
    }

    public void testFallbackToContextDefaults() throws IOException {
        String contextRateStr = randomIntBetween(10, 1024) + "/" + randomIntBetween(10, 200) + "m";
        ScriptCache.CompilationRate contextRate = new ScriptCache.CompilationRate(contextRateStr);
        int contextCacheSize = randomIntBetween(1, 1024);
        TimeValue contextExpire = TimeValue.timeValueMinutes(randomIntBetween(10, 200));

        buildScriptService(Settings.builder().put(SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(), "75/5m").build());

        String name = "ingest";

        // Use context specific
        scriptService.setCacheHolder(
            Settings.builder()
                .put(SCRIPT_CACHE_SIZE_SETTING.getConcreteSettingForNamespace(name).getKey(), contextCacheSize)
                .put(SCRIPT_CACHE_EXPIRE_SETTING.getConcreteSettingForNamespace(name).getKey(), contextExpire)
                .put(SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace(name).getKey(), contextRateStr)
                .build()
        );

        ScriptService.CacheHolder holder = scriptService.cacheHolder.get();
        assertNotNull(holder.contextCache);
        assertNotNull(holder.contextCache.get(name));
        assertNotNull(holder.contextCache.get(name).get());

        assertEquals(contextRate, holder.contextCache.get(name).get().rate);
        assertEquals(contextCacheSize, holder.contextCache.get(name).get().cacheSize);
        assertEquals(contextExpire, holder.contextCache.get(name).get().cacheExpire);

        ScriptContext<?> ingest = contexts.get(name);
        // Fallback to context defaults
        buildScriptService(Settings.EMPTY);

        holder = scriptService.cacheHolder.get();
        assertNotNull(holder.contextCache);
        assertNotNull(holder.contextCache.get(name));
        assertNotNull(holder.contextCache.get(name).get());

        assertEquals(ingest.maxCompilationRateDefault, holder.contextCache.get(name).get().rate.asTuple());
        assertEquals(ingest.cacheSizeDefault, holder.contextCache.get(name).get().cacheSize);
        assertEquals(ingest.cacheExpireDefault, holder.contextCache.get(name).get().cacheExpire);

        assertSettingDeprecationsAndWarnings(new Setting<?>[] { SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING });
    }

    private void assertCompileRejected(String lang, String script, ScriptType scriptType, ScriptContext scriptContext) {
        try {
            scriptService.compile(new Script(scriptType, lang, script, Collections.emptyMap()), scriptContext);
            fail(
                "compile should have been rejected for lang ["
                    + lang
                    + "], "
                    + "script_type ["
                    + scriptType
                    + "], scripted_op ["
                    + scriptContext
                    + "]"
            );
        } catch (IllegalArgumentException | IllegalStateException e) {
            // pass
        }
    }

    private void assertCompileAccepted(String lang, String script, ScriptType scriptType, ScriptContext scriptContext) {
        assertThat(scriptService.compile(new Script(scriptType, lang, script, Collections.emptyMap()), scriptContext), notNullValue());
    }
}
