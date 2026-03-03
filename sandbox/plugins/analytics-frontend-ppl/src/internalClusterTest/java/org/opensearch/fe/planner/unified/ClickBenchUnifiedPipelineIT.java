/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.fe.planner.unified;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.engine.AnalyticsExecutorPlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.fe.PPLFrontEndPlugin;
import org.opensearch.fe.action.UnifiedPPLExecuteAction;
import org.opensearch.fe.action.UnifiedPPLRequest;
import org.opensearch.fe.action.UnifiedPPLResponse;
import org.opensearch.analytics.BaseAnalyticsPlugin;
import org.opensearch.Version;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.BeforeClass;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Internal cluster integration tests that run the ClickBench PPL workload
 * through the full unified pipeline against a real OpenSearch cluster.
 *
 * <p>Spawns a real cluster with PPLFrontEndPlugin + the real BaseAnalyticsPlugin
 * from sandbox/modules/query-engine, creates the ClickBench 'hits' index
 * with the full mapping, and issues each PPL query via the transport action
 * using client().execute().
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class ClickBenchUnifiedPipelineIT extends OpenSearchIntegTestCase {

    private static final Logger logger = LogManager.getLogger(ClickBenchUnifiedPipelineIT.class);
    private static final String HITS_INDEX = "hits";

    @BeforeClass
    public static void setupNettyProperties() {
        System.setProperty("io.netty.allocator.numDirectArenas", "1");
        System.setProperty("io.netty.noUnsafe", "false");
        System.setProperty("io.netty.tryUnsafe", "true");
        System.setProperty("io.netty.tryReflectionSetAccessible", "true");
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(FlightStreamPlugin.class, BaseAnalyticsPlugin.class);
    }

    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        String analyticsEngineName = BaseAnalyticsPlugin.class.getName();
        return List.of(
            new PluginInfo(
                AnalyticsExecutorPlugin.class.getName(),
                "classpath plugin",
                "NA",
                Version.CURRENT,
                "1.8",
                AnalyticsExecutorPlugin.class.getName(),
                null,
                List.of(analyticsEngineName),
                false
            ),
            new PluginInfo(
                PPLFrontEndPlugin.class.getName(),
                "classpath plugin",
                "NA",
                Version.CURRENT,
                "1.8",
                PPLFrontEndPlugin.class.getName(),
                null,
                List.of(analyticsEngineName),
                false
            )
        );
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        createHitsIndex();
        ensureGreen();
    }

    private void createHitsIndex() throws Exception {
        if (indexExists(HITS_INDEX)) {
            return;
        }
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        mapping.startObject("properties");
        addField(mapping, "AdvEngineID", "short");
        addField(mapping, "Age", "short");
        addField(mapping, "BrowserCountry", "keyword");
        addField(mapping, "BrowserLanguage", "keyword");
        addField(mapping, "CLID", "integer");
        addDateField(mapping, "ClientEventTime");
        addField(mapping, "ClientIP", "integer");
        addField(mapping, "ClientTimeZone", "short");
        addField(mapping, "CodeVersion", "integer");
        addField(mapping, "ConnectTiming", "integer");
        addField(mapping, "CookieEnable", "short");
        addField(mapping, "CounterClass", "short");
        addField(mapping, "CounterID", "integer");
        addField(mapping, "DNSTiming", "integer");
        addField(mapping, "DontCountHits", "short");
        addDateField(mapping, "EventDate");
        addDateField(mapping, "EventTime");
        addField(mapping, "FUniqID", "long");
        addField(mapping, "FetchTiming", "integer");
        addField(mapping, "FlashMajor", "short");
        addField(mapping, "FlashMinor", "short");
        addField(mapping, "FlashMinor2", "short");
        addField(mapping, "FromTag", "keyword");
        addField(mapping, "GoodEvent", "short");
        addField(mapping, "HID", "integer");
        addField(mapping, "HTTPError", "short");
        addField(mapping, "HasGCLID", "short");
        addField(mapping, "HistoryLength", "short");
        addField(mapping, "HitColor", "keyword");
        addField(mapping, "IPNetworkID", "integer");
        addField(mapping, "Income", "short");
        addField(mapping, "Interests", "short");
        addField(mapping, "IsArtifical", "short");
        addField(mapping, "IsDownload", "short");
        addField(mapping, "IsEvent", "short");
        addField(mapping, "IsLink", "short");
        addField(mapping, "IsMobile", "short");
        addField(mapping, "IsNotBounce", "short");
        addField(mapping, "IsOldCounter", "short");
        addField(mapping, "IsParameter", "short");
        addField(mapping, "IsRefresh", "short");
        addField(mapping, "JavaEnable", "short");
        addField(mapping, "JavascriptEnable", "short");
        addDateField(mapping, "LocalEventTime");
        addField(mapping, "MobilePhone", "short");
        addField(mapping, "MobilePhoneModel", "keyword");
        addField(mapping, "NetMajor", "short");
        addField(mapping, "NetMinor", "short");
        addField(mapping, "OS", "short");
        addField(mapping, "OpenerName", "integer");
        addField(mapping, "OpenstatAdID", "keyword");
        addField(mapping, "OpenstatCampaignID", "keyword");
        addField(mapping, "OpenstatServiceName", "keyword");
        addField(mapping, "OpenstatSourceID", "keyword");
        addField(mapping, "OriginalURL", "keyword");
        addField(mapping, "PageCharset", "keyword");
        addField(mapping, "ParamCurrency", "keyword");
        addField(mapping, "ParamCurrencyID", "short");
        addField(mapping, "ParamOrderID", "keyword");
        addField(mapping, "ParamPrice", "long");
        addField(mapping, "Params", "keyword");
        addField(mapping, "Referer", "keyword");
        addField(mapping, "RefererCategoryID", "short");
        addField(mapping, "RefererHash", "long");
        addField(mapping, "RefererRegionID", "integer");
        addField(mapping, "RegionID", "integer");
        addField(mapping, "RemoteIP", "integer");
        addField(mapping, "ResolutionDepth", "short");
        addField(mapping, "ResolutionHeight", "short");
        addField(mapping, "ResolutionWidth", "short");
        addField(mapping, "ResponseEndTiming", "integer");
        addField(mapping, "ResponseStartTiming", "integer");
        addField(mapping, "Robotness", "short");
        addField(mapping, "SearchEngineID", "short");
        addField(mapping, "SearchPhrase", "keyword");
        addField(mapping, "SendTiming", "integer");
        addField(mapping, "Sex", "short");
        addField(mapping, "SilverlightVersion1", "short");
        addField(mapping, "SilverlightVersion2", "short");
        addField(mapping, "SilverlightVersion3", "integer");
        addField(mapping, "SilverlightVersion4", "short");
        addField(mapping, "SocialSourceNetworkID", "short");
        addField(mapping, "SocialSourcePage", "keyword");
        addField(mapping, "Title", "keyword");
        addField(mapping, "TraficSourceID", "short");
        addField(mapping, "URL", "keyword");
        addField(mapping, "URLCategoryID", "short");
        addField(mapping, "URLHash", "long");
        addField(mapping, "URLRegionID", "integer");
        addField(mapping, "UTMCampaign", "keyword");
        addField(mapping, "UTMContent", "keyword");
        addField(mapping, "UTMMedium", "keyword");
        addField(mapping, "UTMSource", "keyword");
        addField(mapping, "UTMTerm", "keyword");
        addField(mapping, "UserAgent", "short");
        addField(mapping, "UserAgentMajor", "short");
        addField(mapping, "UserAgentMinor", "keyword");
        addField(mapping, "UserID", "long");
        addField(mapping, "WatchID", "long");
        addField(mapping, "WindowClientHeight", "short");
        addField(mapping, "WindowClientWidth", "short");
        addField(mapping, "WindowName", "integer");
        addField(mapping, "WithHash", "short");
        mapping.endObject(); // properties
        mapping.endObject();

        prepareCreate(HITS_INDEX).setSettings(
            Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).build()
        ).setMapping(mapping).get();
    }

    private static void addField(XContentBuilder builder, String name, String type) throws Exception {
        builder.startObject(name).field("type", type).endObject();
    }

    private static void addDateField(XContentBuilder builder, String name) throws Exception {
        builder.startObject(name)
            .field("type", "date")
            .field("format", "yyyy-MM-dd HH:mm:ss||strict_date_optional_time||epoch_millis")
            .endObject();
    }

    // --- ClickBench query tests ---

    public void testClickBenchQ1() throws Exception {
        runClickBenchQuery("q1");
    }

    public void testClickBenchQ2() throws Exception {
        runClickBenchQuery("q2");
    }

    public void testClickBenchQ3() throws Exception {
        runClickBenchQuery("q3");
    }

    public void testClickBenchQ4() throws Exception {
        runClickBenchQuery("q4");
    }

    public void testClickBenchQ5() throws Exception {
        runClickBenchQuery("q5");
    }

    public void testClickBenchQ6() throws Exception {
        runClickBenchQuery("q6");
    }

    public void testClickBenchQ7() throws Exception {
        runClickBenchQuery("q7");
    }

    public void testClickBenchQ8() throws Exception {
        runClickBenchQuery("q8");
    }

    public void testClickBenchQ9() throws Exception {
        runClickBenchQuery("q9");
    }

    public void testClickBenchQ10() throws Exception {
        runClickBenchQuery("q10");
    }

    public void testClickBenchQ11() throws Exception {
        runClickBenchQuery("q11");
    }

    public void testClickBenchQ12() throws Exception {
        runClickBenchQuery("q12");
    }

    public void testClickBenchQ13() throws Exception {
        runClickBenchQuery("q13");
    }

    public void testClickBenchQ14() throws Exception {
        runClickBenchQuery("q14");
    }

    public void testClickBenchQ15() throws Exception {
        runClickBenchQuery("q15");
    }

    public void testClickBenchQ16() throws Exception {
        runClickBenchQuery("q16");
    }

    public void testClickBenchQ17() throws Exception {
        runClickBenchQuery("q17");
    }

    public void testClickBenchQ18() throws Exception {
        runClickBenchQuery("q18");
    }

    public void testClickBenchQ19() throws Exception {
        runClickBenchQuery("q19");
    }

    public void testClickBenchQ20() throws Exception {
        runClickBenchQuery("q20");
    }

    public void testClickBenchQ21() throws Exception {
        runClickBenchQuery("q21");
    }

    public void testClickBenchQ22() throws Exception {
        runClickBenchQuery("q22");
    }

    public void testClickBenchQ23() throws Exception {
        runClickBenchQuery("q23");
    }

    public void testClickBenchQ24() throws Exception {
        runClickBenchQuery("q24");
    }

    public void testClickBenchQ25() throws Exception {
        runClickBenchQuery("q25");
    }

    public void testClickBenchQ26() throws Exception {
        runClickBenchQuery("q26");
    }

    public void testClickBenchQ27() throws Exception {
        runClickBenchQuery("q27");
    }

    public void testClickBenchQ28() throws Exception {
        runClickBenchQuery("q28");
    }

    public void testClickBenchQ29() throws Exception {
        runClickBenchQuery("q29");
    }

    public void testClickBenchQ30() throws Exception {
        runClickBenchQuery("q30");
    }

    public void testClickBenchQ31() throws Exception {
        runClickBenchQuery("q31");
    }

    public void testClickBenchQ32() throws Exception {
        runClickBenchQuery("q32");
    }

    public void testClickBenchQ33() throws Exception {
        runClickBenchQuery("q33");
    }

    public void testClickBenchQ34() throws Exception {
        runClickBenchQuery("q34");
    }

    public void testClickBenchQ35() throws Exception {
        runClickBenchQuery("q35");
    }

    public void testClickBenchQ36() throws Exception {
        runClickBenchQuery("q36");
    }

    public void testClickBenchQ37() throws Exception {
        runClickBenchQuery("q37");
    }

    public void testClickBenchQ38() throws Exception {
        runClickBenchQuery("q38");
    }

    public void testClickBenchQ39() throws Exception {
        runClickBenchQuery("q39");
    }

    public void testClickBenchQ40() throws Exception {
        runClickBenchQuery("q40");
    }

    public void testClickBenchQ41() throws Exception {
        runClickBenchQuery("q41");
    }

    public void testClickBenchQ42() throws Exception {
        runClickBenchQuery("q42");
    }

    public void testClickBenchQ43() throws Exception {
        runClickBenchQuery("q43");
    }

    // --- Core test runner ---

    private void runClickBenchQuery(String queryId) throws Exception {
        String rawPpl = loadQuery(queryId);
        String ppl = rawPpl.replace("source=hits", "source=opensearch.hits")
            .replace("source =hits", "source =opensearch.hits")
            .replace("source= hits", "source= opensearch.hits")
            .replace("source = hits", "source = opensearch.hits");

        logger.info("=== ClickBench {} (Unified Pipeline IT) ===\nPPL: {}", queryId, ppl);

        UnifiedPPLRequest request = new UnifiedPPLRequest(ppl);
        UnifiedPPLResponse response = client().execute(UnifiedPPLExecuteAction.INSTANCE, request).actionGet();
        assertNotNull("Response should not be null for " + queryId, response);
        assertNotNull("Columns should not be null for " + queryId, response.getColumns());
        assertFalse("Columns should not be empty for " + queryId, response.getColumns().isEmpty());
        logger.info("SUCCESS {}: {} columns, {} rows", queryId, response.getColumns().size(), response.getRows().size());
    }

    private String loadQuery(String queryId) throws Exception {
        String resourcePath = "clickbench/queries/" + queryId + ".ppl";
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
            assertNotNull("Resource not found: " + resourcePath, is);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                String content = reader.lines().collect(Collectors.joining("\n"));
                content = content.replaceAll("/\\*[\\s\\S]*?\\*/", "");
                content = content.replaceAll("\\n", " ").replaceAll("\\s+", " ").trim();
                return content;
            }
        }
    }

}
