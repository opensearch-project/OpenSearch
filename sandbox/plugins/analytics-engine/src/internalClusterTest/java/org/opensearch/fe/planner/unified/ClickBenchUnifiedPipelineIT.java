/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.fe.planner.unified;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugins.Plugin;
import org.opensearch.ppl.TestPPLPlugin;
import org.opensearch.ppl.action.PPLRequest;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.ppl.action.UnifiedPPLExecuteAction;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Internal cluster integration tests that run the ClickBench PPL workload
 * through the full unified pipeline against a real OpenSearch cluster.
 *
 * <p>Spawns a real cluster with PPLFrontEndPlugin + the real AnalyticsPlugin
 * from sandbox/modules/query-engine, creates the ClickBench 'hits' index
 * with the full mapping, and issues each PPL query via the transport action
 * using client().execute().
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class ClickBenchUnifiedPipelineIT extends OpenSearchIntegTestCase {

    private static final Logger logger = LogManager.getLogger(ClickBenchUnifiedPipelineIT.class);
    private static final String HITS_INDEX = "hits";

    private final String queryId;

    public ClickBenchUnifiedPipelineIT(String queryId) {
        this.queryId = queryId;
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        List<Object[]> params = new ArrayList<>();
        for (int i = 1; i <= 43; i++) {
            params.add(new Object[] { "q" + i });
        }
        return params;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestPPLPlugin.class, FlightStreamPlugin.class, AnalyticsPlugin.class);
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

    public void testClickBenchQuery() throws Exception {
        runClickBenchQuery(queryId);
    }

    private void runClickBenchQuery(String queryId) throws Exception {
        String rawPpl = loadQuery(queryId);
        String ppl = rawPpl.replace("source=hits", "source=opensearch.hits")
            .replace("source =hits", "source =opensearch.hits")
            .replace("source= hits", "source= opensearch.hits")
            .replace("source = hits", "source = opensearch.hits");

        logger.info("=== ClickBench {} (Unified Pipeline IT) ===\nPPL: {}", queryId, ppl);

        PPLRequest request = new PPLRequest(ppl);
        PPLResponse response = client().execute(UnifiedPPLExecuteAction.INSTANCE, request).actionGet();
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
