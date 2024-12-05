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

package org.opensearch.test;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.Listeners;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;
import com.carrotsearch.randomizedtesting.generators.CodepointSetGenerator;
import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import com.carrotsearch.randomizedtesting.rules.TestRuleAdapter;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.status.StatusConsoleListener;
import org.apache.logging.log4j.status.StatusData;
import org.apache.logging.log4j.status.StatusLogger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.tests.util.TestRuleMarkFailure;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.tests.util.TimeUnits;
import org.opensearch.Version;
import org.opensearch.bootstrap.BootstrapForTesting;
import org.opensearch.client.Client;
import org.opensearch.client.Requests;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.coordination.PersistedStateRegistry;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.Numbers;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.io.PathUtils;
import org.opensearch.common.io.PathUtilsForTesting;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.joda.JodaDeprecationPatterns;
import org.opensearch.common.logging.DeprecatedMessage;
import org.opensearch.common.logging.HeaderWarning;
import org.opensearch.common.logging.HeaderWarningAppender;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.time.DateUtils;
import org.opensearch.common.time.FormatNames;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.MockBigArrays;
import org.opensearch.common.util.MockPageCacheRecycler;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.util.set.Sets;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.NamedWriteable;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentHelper;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParser.Token;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.index.analysis.AnalyzerScope;
import org.opensearch.index.analysis.CharFilterFactory;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.analysis.TokenFilterFactory;
import org.opensearch.index.analysis.TokenizerFactory;
import org.opensearch.index.remote.RemoteStoreEnums;
import org.opensearch.index.remote.RemoteStorePathStrategy;
import org.opensearch.indices.analysis.AnalysisModule;
import org.opensearch.monitor.jvm.JvmInfo;
import org.opensearch.plugins.AnalysisPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.script.MockScriptEngine;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.MockSearchService;
import org.opensearch.test.junit.listeners.LoggingListener;
import org.opensearch.test.junit.listeners.ReproduceInfoPrinter;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.nio.MockNioTransportPlugin;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.internal.AssumptionViolatedException;
import org.junit.rules.RuleChain;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import reactor.core.scheduler.Schedulers;

import static java.util.Collections.emptyMap;
import static org.opensearch.core.common.util.CollectionUtils.arrayAsArrayList;
import static org.opensearch.index.store.remote.filecache.FileCacheSettings.DATA_TO_FILE_CACHE_SIZE_RATIO_SETTING;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

/**
 * Base testcase for randomized unit testing with OpenSearch
 */
@Listeners({ ReproduceInfoPrinter.class, LoggingListener.class })
@ThreadLeakScope(Scope.SUITE)
@ThreadLeakLingering(linger = 5000) // 5 sec lingering
@TimeoutSuite(millis = 20 * TimeUnits.MINUTE)
@LuceneTestCase.SuppressSysoutChecks(bugUrl = "we log a lot on purpose")
// we suppress pretty much all the lucene codecs for now, except asserting
// assertingcodec is the winner for a codec here: it finds bugs and gives clear exceptions.
@SuppressCodecs({
    "SimpleText",
    "Memory",
    "CheapBastard",
    "Direct",
    "Compressing",
    "FST50",
    "FSTOrd50",
    "TestBloomFilteredLucenePostings",
    "MockRandom",
    "BlockTreeOrds",
    "LuceneFixedGap",
    "LuceneVarGapFixedInterval",
    "LuceneVarGapDocFreqInterval",
    "Lucene50",
    "Lucene90",
    "Lucene94",
    "Lucene90",
    "Lucene95",
    "Lucene99" })
@LuceneTestCase.SuppressReproduceLine
public abstract class OpenSearchTestCase extends LuceneTestCase {

    protected static final List<String> JODA_TIMEZONE_IDS;
    protected static final List<String> JAVA_TIMEZONE_IDS;
    protected static final List<String> JAVA_ZONE_IDS;

    private static final AtomicInteger portGenerator = new AtomicInteger();

    private static final Collection<String> nettyLoggedLeaks = new ArrayList<>();
    private HeaderWarningAppender headerWarningAppender;

    @AfterClass
    public static void resetPortCounter() {
        portGenerator.set(0);
    }

    @Override
    public void tearDown() throws Exception {
        Schedulers.shutdownNow();
        FeatureFlagSetter.clear();
        super.tearDown();
    }

    // Allows distinguishing between parallel test processes
    public static final String TEST_WORKER_VM_ID;

    public static final String TEST_WORKER_SYS_PROPERTY = "org.gradle.test.worker";

    public static final String DEFAULT_TEST_WORKER_ID = "--not-gradle--";

    public static final String FIPS_SYSPROP = "tests.fips.enabled";

    static {
        TEST_WORKER_VM_ID = System.getProperty(TEST_WORKER_SYS_PROPERTY, DEFAULT_TEST_WORKER_ID);
        setTestSysProps();

        String leakLoggerName = "io.netty.util.ResourceLeakDetector";
        Logger leakLogger = LogManager.getLogger(leakLoggerName);
        Appender leakAppender = new AbstractAppender(leakLoggerName, null, PatternLayout.newBuilder().withPattern("%m").build()) {
            @Override
            public void append(LogEvent event) {
                String message = event.getMessage().getFormattedMessage();
                if (Level.ERROR.equals(event.getLevel()) && message.contains("LEAK:")) {
                    synchronized (nettyLoggedLeaks) {
                        nettyLoggedLeaks.add(message);
                    }
                }
            }
        };
        leakAppender.start();
        Loggers.addAppender(leakLogger, leakAppender);

        // shutdown hook so that when the test JVM exits, logging is shutdown too
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            leakAppender.stop();
            LoggerContext context = (LoggerContext) LogManager.getContext(false);
            Configurator.shutdown(context);
        }));

        BootstrapForTesting.ensureInitialized();
        TransportService.ensureClassloaded(); // ensure server streamables are registered

        // filter out joda timezones that are deprecated for the java time migration
        List<String> jodaTZIds = DateTimeZone.getAvailableIDs()
            .stream()
            .filter(s -> DateUtils.DEPRECATED_SHORT_TZ_IDS.contains(s) == false)
            .sorted()
            .collect(Collectors.toList());
        JODA_TIMEZONE_IDS = Collections.unmodifiableList(jodaTZIds);

        List<String> javaTZIds = Arrays.asList(TimeZone.getAvailableIDs());
        Collections.sort(javaTZIds);
        JAVA_TIMEZONE_IDS = Collections.unmodifiableList(javaTZIds);

        List<String> javaZoneIds = new ArrayList<>(ZoneId.getAvailableZoneIds());
        Collections.sort(javaZoneIds);
        JAVA_ZONE_IDS = Collections.unmodifiableList(javaZoneIds);
    }

    @SuppressForbidden(reason = "force log4j and netty sysprops")
    private static void setTestSysProps() {
        System.setProperty("log4j.shutdownHookEnabled", "false");
        System.setProperty("log4j2.disable.jmx", "true");

        // Enable Netty leak detection and monitor logger for logged leak errors
        System.setProperty("io.netty.leakDetection.level", "paranoid");
    }

    protected final Logger logger = LogManager.getLogger(getClass());
    private ThreadContext threadContext;

    // -----------------------------------------------------------------
    // Suite and test case setup/cleanup.
    // -----------------------------------------------------------------

    @Rule
    public RuleChain failureAndSuccessEvents = RuleChain.outerRule(new TestRuleAdapter() {
        @Override
        protected void afterIfSuccessful() throws Throwable {
            OpenSearchTestCase.this.afterIfSuccessful();
        }

        @Override
        protected void afterAlways(List<Throwable> errors) throws Throwable {
            if (errors != null && errors.isEmpty() == false) {
                boolean allAssumption = true;
                for (Throwable error : errors) {
                    if (false == error instanceof AssumptionViolatedException) {
                        allAssumption = false;
                        break;
                    }
                }
                if (false == allAssumption) {
                    OpenSearchTestCase.this.afterIfFailed(errors);
                }
            }
            super.afterAlways(errors);
        }
    });

    /**
     * Generates a new transport address using {@link TransportAddress#META_ADDRESS} with an incrementing port number.
     * The port number starts at 0 and is reset after each test suite run.
     */
    public static TransportAddress buildNewFakeTransportAddress() {
        return new TransportAddress(TransportAddress.META_ADDRESS, portGenerator.incrementAndGet());
    }

    /**
     * Called when a test fails, supplying the errors it generated. Not called when the test fails because assumptions are violated.
     */
    protected void afterIfFailed(List<Throwable> errors) {}

    /** called after a test is finished, but only if successful */
    protected void afterIfSuccessful() throws Exception {}

    // setup mock filesystems for this test run. we change PathUtils
    // so that all accesses are plumbed thru any mock wrappers

    @BeforeClass
    public static void setFileSystem() throws Exception {
        PathUtilsForTesting.setup();
    }

    @AfterClass
    public static void restoreFileSystem() throws Exception {
        PathUtilsForTesting.teardown();
    }

    // randomize content type for request builders

    @BeforeClass
    public static void setContentType() throws Exception {
        Requests.CONTENT_TYPE = randomFrom(XContentType.values());
        Requests.INDEX_CONTENT_TYPE = randomFrom(XContentType.values());
    }

    @AfterClass
    public static void restoreContentType() {
        Requests.CONTENT_TYPE = XContentType.SMILE;
        Requests.INDEX_CONTENT_TYPE = MediaTypeRegistry.JSON;
    }

    @BeforeClass
    public static void ensureSupportedLocale() {
        if (isUnusableLocale()) {
            Logger logger = LogManager.getLogger(OpenSearchTestCase.class);
            logger.warn(
                "Attempting to run tests in an unusable locale in a FIPS JVM. Certificate expiration validation will fail, "
                    + "switching to English. See: https://github.com/bcgit/bc-java/issues/405"
            );
            Locale.setDefault(Locale.ENGLISH);
        }
    }

    @Before
    public void setHeaderWarningAppender() {
        this.headerWarningAppender = HeaderWarningAppender.createAppender("header_warning", null);
        this.headerWarningAppender.start();
        Loggers.addAppender(LogManager.getLogger("org.opensearch.deprecation"), this.headerWarningAppender);
    }

    @After
    public void removeHeaderWarningAppender() {
        if (this.headerWarningAppender != null) {
            Loggers.removeAppender(LogManager.getLogger("org.opensearch.deprecation"), this.headerWarningAppender);
            this.headerWarningAppender = null;
        }
    }

    @Before
    public final void before() {
        logger.info("{}before test", getTestParamsForLogging());
        assertNull("Thread context initialized twice", threadContext);
        if (enableWarningsCheck()) {
            this.threadContext = new ThreadContext(Settings.EMPTY);
            HeaderWarning.setThreadContext(threadContext);
        }
    }

    @AfterClass
    public static void clearAdditionalRoles() {
        DiscoveryNode.setAdditionalRoles(Collections.emptySet());
    }

    /**
     * Whether or not we check after each test whether it has left warnings behind. That happens if any deprecated feature or syntax
     * was used by the test and the test didn't assert on it using {@link #assertWarnings(String...)}.
     */
    protected boolean enableWarningsCheck() {
        return true;
    }

    protected boolean enableJodaDeprecationWarningsCheck() {
        return false;
    }

    @After
    public final void after() throws Exception {
        checkStaticState(false);
        // We check threadContext != null rather than enableWarningsCheck()
        // because after methods are still called in the event that before
        // methods failed, in which case threadContext might not have been
        // initialized
        if (threadContext != null) {
            ensureNoWarnings();
            HeaderWarning.removeThreadContext(threadContext);
            threadContext = null;
        }
        ensureAllSearchContextsReleased();
        ensureCheckIndexPassed();
        // "clear" the deprecated message set for the next tests to run independently.
        DeprecatedMessage.resetDeprecatedMessageForTests();
        logger.info("{}after test", getTestParamsForLogging());
    }

    private String getTestParamsForLogging() {
        String name = getTestName();
        int start = name.indexOf('{');
        if (start < 0) return "";
        int end = name.lastIndexOf('}');
        if (end < 0) return "";
        return "[" + name.substring(start + 1, end) + "] ";
    }

    private void ensureNoWarnings() {
        // Check that there are no unaccounted warning headers. These should be checked with {@link #assertWarnings(String...)} in the
        // appropriate test
        try {
            final List<String> warnings = threadContext.getResponseHeaders().get("Warning");
            if (warnings != null) {
                List<String> filteredWarnings = new ArrayList<>(warnings);
                if (enableJodaDeprecationWarningsCheck() == false) {
                    filteredWarnings = filterJodaDeprecationWarnings(filteredWarnings);
                }
                if (JvmInfo.jvmInfo().getBundledJdk() == false) {
                    // unit tests do not run with the bundled JDK, if there are warnings we need to filter the no-jdk deprecation warning
                    filteredWarnings = filteredWarnings.stream()
                        .filter(
                            k -> k.contains(
                                "no-jdk distributions that do not bundle a JDK are deprecated and will be removed in a future release"
                            ) == false
                        )
                        .collect(Collectors.toList());
                }
                assertThat("unexpected warning headers", filteredWarnings, empty());
            } else {
                assertNull("unexpected warning headers", warnings);
            }
        } finally {
            resetDeprecationLogger();
        }
    }

    /**
     * Convenience method to assert warnings for settings deprecations and general deprecation warnings.
     *
     * @param settings the settings that are expected to be deprecated
     * @param warnings other expected general deprecation warnings
     */
    protected final void assertSettingDeprecationsAndWarnings(final Setting<?>[] settings, final String... warnings) {
        assertSettingDeprecationsAndWarnings(Arrays.stream(settings).map(Setting::getKey).toArray(String[]::new), warnings);
    }

    protected final void assertSettingDeprecationsAndWarnings(final String[] settings, final String... warnings) {
        assertWarnings(
            Stream.concat(
                Arrays.stream(settings)
                    .map(
                        k -> "["
                            + k
                            + "] setting was deprecated in OpenSearch and will be removed in a future release! "
                            + "See the breaking changes documentation for the next major version."
                    ),
                Arrays.stream(warnings)
            ).toArray(String[]::new)
        );
    }

    /**
     * Convenience method to assert same warnings for settings deprecations and general deprecation warnings
     * are not logged again.
     */
    protected final void assertNoDeprecationWarnings() {
        final List<String> actualWarnings = threadContext.getResponseHeaders().get("Warning");
        assertTrue("Found duplicate warnings logged", actualWarnings == null);
    }

    protected final void assertWarnings(String... expectedWarnings) {
        assertWarnings(true, expectedWarnings);
    }

    /**
     * Allow the given warnings, but don't require their presence.
     */
    protected final void allowedWarnings(String... allowedWarnings) {
        if (enableWarningsCheck() == false) {
            throw new IllegalStateException("unable to check warning headers if the test is not set to do so");
        }
        try {
            final List<String> actualWarnings = threadContext.getResponseHeaders().get("Warning");
            if (actualWarnings == null) {
                return;
            }
            final Set<String> actualWarningValues = actualWarnings.stream()
                .map(s -> HeaderWarning.extractWarningValueFromWarningHeader(s, true))
                .map(HeaderWarning::escapeAndEncode)
                .collect(Collectors.toSet());
            Set<String> expectedWarnings = new HashSet<>(Arrays.asList(allowedWarnings));
            final Set<String> warningsNotExpected = Sets.difference(actualWarningValues, expectedWarnings);
            assertThat(
                "Found "
                    + warningsNotExpected.size()
                    + " unexpected warnings\nExpected: "
                    + expectedWarnings
                    + "\nActual: "
                    + actualWarningValues,
                warningsNotExpected.size(),
                equalTo(0)
            );
        } finally {
            resetDeprecationLogger();
        }
    }

    protected final void assertWarnings(boolean stripXContentPosition, String... expectedWarnings) {
        if (enableWarningsCheck() == false) {
            throw new IllegalStateException("unable to check warning headers if the test is not set to do so");
        }
        try {
            final List<String> actualWarnings = threadContext.getResponseHeaders().get("Warning");
            if (actualWarnings != null && enableJodaDeprecationWarningsCheck() == false) {
                List<String> filteredWarnings = filterJodaDeprecationWarnings(actualWarnings);
                assertWarnings(stripXContentPosition, filteredWarnings, expectedWarnings);
            } else {
                assertWarnings(stripXContentPosition, actualWarnings, expectedWarnings);
            }
        } finally {
            resetDeprecationLogger();
        }
    }

    private List<String> filterJodaDeprecationWarnings(List<String> actualWarnings) {
        return actualWarnings.stream()
            .filter(m -> m.contains(JodaDeprecationPatterns.USE_NEW_FORMAT_SPECIFIERS) == false)
            .collect(Collectors.toList());
    }

    private void assertWarnings(boolean stripXContentPosition, List<String> actualWarnings, String[] expectedWarnings) {
        assertNotNull("no warnings, expected: " + Arrays.asList(expectedWarnings), actualWarnings);
        final Set<String> actualWarningValues = actualWarnings.stream()
            .map(s -> HeaderWarning.extractWarningValueFromWarningHeader(s, stripXContentPosition))
            .collect(Collectors.toSet());
        for (String msg : expectedWarnings) {
            assertThat(actualWarningValues, hasItem(HeaderWarning.escapeAndEncode(msg)));
        }
        assertEquals(
            "Expected "
                + expectedWarnings.length
                + " warnings but found "
                + actualWarnings.size()
                + "\nExpected: "
                + Arrays.asList(expectedWarnings)
                + "\nActual: "
                + actualWarnings,
            expectedWarnings.length,
            actualWarnings.size()
        );
    }

    /**
     * Reset the deprecation logger by clearing the current thread context.
     */
    private void resetDeprecationLogger() {
        // "clear" context by stashing current values and dropping the returned StoredContext
        threadContext.stashContext();
    }

    private static final List<StatusData> statusData = new ArrayList<>();
    static {
        // ensure that the status logger is set to the warn level so we do not miss any warnings with our Log4j usage
        StatusLogger.getLogger().setLevel(Level.WARN);
        // Log4j will write out status messages indicating problems with the Log4j usage to the status logger; we hook into this logger and
        // assert that no such messages were written out as these would indicate a problem with our logging configuration
        StatusLogger.getLogger().registerListener(new StatusConsoleListener(Level.WARN) {

            @Override
            public void log(StatusData data) {
                synchronized (statusData) {
                    statusData.add(data);
                }
            }

        });
    }

    // separate method so that this can be checked again after suite scoped cluster is shut down
    protected static void checkStaticState(boolean afterClass) throws Exception {
        if (afterClass) {
            MockPageCacheRecycler.ensureAllPagesAreReleased();
        }
        MockBigArrays.ensureAllArraysAreReleased();

        // ensure no one changed the status logger level on us
        assertThat(StatusLogger.getLogger().getLevel(), equalTo(Level.WARN));
        synchronized (statusData) {
            try {
                // ensure that there are no status logger messages which would indicate a problem with our Log4j usage; we map the
                // StatusData instances to Strings as otherwise their toString output is useless

                final Function<StatusData, String> statusToString = (statusData) -> {
                    try (final StringWriter sw = new StringWriter(); final PrintWriter pw = new PrintWriter(sw)) {

                        pw.print(statusData.getLevel());
                        pw.print(":");
                        pw.print(statusData.getMessage().getFormattedMessage());

                        if (statusData.getStackTraceElement() != null) {
                            final var messageSource = statusData.getStackTraceElement();
                            pw.println("Source:");
                            pw.println(messageSource.getFileName() + "@" + messageSource.getLineNumber());
                        }

                        if (statusData.getThrowable() != null) {
                            pw.println("Throwable:");
                            statusData.getThrowable().printStackTrace(pw);
                        }
                        return sw.toString();
                    } catch (IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                };

                assertThat(
                    statusData.stream().map(statusToString::apply).collect(Collectors.joining("\r\n")),
                    statusData.stream().map(status -> status.getMessage().getFormattedMessage()).collect(Collectors.toList()),
                    empty()
                );
            } finally {
                // we clear the list so that status data from other tests do not interfere with tests within the same JVM
                statusData.clear();
            }
        }
        synchronized (nettyLoggedLeaks) {
            try {
                assertThat(nettyLoggedLeaks, empty());
            } finally {
                nettyLoggedLeaks.clear();
            }
        }
    }

    // this must be a separate method from other ensure checks above so suite scoped integ tests can call...TODO: fix that
    public final void ensureAllSearchContextsReleased() throws Exception {
        assertBusy(() -> MockSearchService.assertNoInFlightContext());
    }

    // mockdirectorywrappers currently set this boolean if checkindex fails
    // TODO: can we do this cleaner???

    /** MockFSDirectoryService sets this: */
    public static final List<Exception> checkIndexFailures = new CopyOnWriteArrayList<>();

    @Before
    public final void resetCheckIndexStatus() throws Exception {
        checkIndexFailures.clear();
    }

    public final void ensureCheckIndexPassed() {
        if (checkIndexFailures.isEmpty() == false) {
            final AssertionError e = new AssertionError("at least one shard failed CheckIndex");
            for (Exception failure : checkIndexFailures) {
                e.addSuppressed(failure);
            }
            throw e;
        }
    }

    // -----------------------------------------------------------------
    // Test facilities and facades for subclasses.
    // -----------------------------------------------------------------

    // TODO: decide on one set of naming for between/scaledBetween and remove others
    // TODO: replace frequently() with usually()

    /**
     * Returns a "scaled" random number between min and max (inclusive).
     *
     * @see RandomizedTest#scaledRandomIntBetween(int, int)
     */
    public static int scaledRandomIntBetween(int min, int max) {
        return RandomizedTest.scaledRandomIntBetween(min, max);
    }

    /**
     * A random integer from <code>min</code> to <code>max</code> (inclusive).
     *
     * @see #scaledRandomIntBetween(int, int)
     */
    public static int randomIntBetween(int min, int max) {
        return RandomNumbers.randomIntBetween(random(), min, max);
    }

    /**
     * A random long number between min (inclusive) and max (inclusive).
     */
    public static long randomLongBetween(long min, long max) {
        return RandomNumbers.randomLongBetween(random(), min, max);
    }

    /**
     * Returns a "scaled" number of iterations for loops which can have a variable
     * iteration count. This method is effectively
     * an alias to {@link #scaledRandomIntBetween(int, int)}.
     */
    public static int iterations(int min, int max) {
        return scaledRandomIntBetween(min, max);
    }

    /**
     * An alias for {@link #randomIntBetween(int, int)}.
     *
     * @see #scaledRandomIntBetween(int, int)
     */
    public static int between(int min, int max) {
        return randomIntBetween(min, max);
    }

    /**
     * The exact opposite of {@link #rarely()}.
     */
    public static boolean frequently() {
        return !rarely();
    }

    public static boolean randomBoolean() {
        return random().nextBoolean();
    }

    public static byte randomByte() {
        return (byte) random().nextInt();
    }

    /**
     * Helper method to create a byte array of a given length populated with random byte values
     *
     * @see #randomByte()
     */
    public static byte[] randomByteArrayOfLength(int size) {
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++) {
            bytes[i] = randomByte();
        }
        return bytes;
    }

    public static short randomShort() {
        return (short) random().nextInt();
    }

    public static int randomInt() {
        return random().nextInt();
    }

    /**
     * @return a <code>long</code> between <code>0</code> and <code>Long.MAX_VALUE</code> (inclusive) chosen uniformly at random.
     */
    public static long randomNonNegativeLong() {
        long randomLong = randomLong();
        return randomLong == Long.MIN_VALUE ? 0 : Math.abs(randomLong);
    }

    /**
     * @return a <code>int</code> between <code>0</code> and <code>Integer.MAX_VALUE</code> (inclusive) chosen uniformly at random.
     */
    public static int randomNonNegativeInt() {
        int randomInt = randomInt();
        return randomInt == Integer.MIN_VALUE ? 0 : Math.abs(randomInt);
    }

    public static float randomFloat() {
        return random().nextFloat();
    }

    public static double randomDouble() {
        return random().nextDouble();
    }

    /**
     * Returns a double value in the interval [start, end) if lowerInclusive is
     * set to true, (start, end) otherwise.
     *
     * @param start          lower bound of interval to draw uniformly distributed random numbers from
     * @param end            upper bound
     * @param lowerInclusive whether or not to include lower end of the interval
     */
    public static double randomDoubleBetween(double start, double end, boolean lowerInclusive) {
        double result = 0.0;

        if (start == -Double.MAX_VALUE || end == Double.MAX_VALUE) {
            // formula below does not work with very large doubles
            result = Double.longBitsToDouble(randomLong());
            while (result < start || result > end || Double.isNaN(result)) {
                result = Double.longBitsToDouble(randomLong());
            }
        } else {
            result = randomDouble();
            if (lowerInclusive == false) {
                while (result <= 0.0) {
                    result = randomDouble();
                }
            }
            result = result * end + (1.0 - result) * start;
        }
        return result;
    }

    public static long randomLong() {
        return random().nextLong();
    }

    /**
     * Returns a random BigInteger uniformly distributed over the range 0 to (2^64 - 1) inclusive
     * Currently BigIntegers are only used for unsigned_long field type, where the max value is 2^64 - 1.
     * Modify this random generator if a wider range for BigIntegers is necessary.
     * @return a random bigInteger in the range [0 ; 2^64 - 1]
     */
    public static BigInteger randomUnsignedLong() {
        BigInteger value = randomBigInteger().abs();

        while (value.compareTo(Numbers.MAX_UNSIGNED_LONG_VALUE) == 1) {
            value = value.subtract(Numbers.MAX_UNSIGNED_LONG_VALUE);
        }

        return value;
    }

    /**
     * Returns a random BigInteger uniformly distributed over the range 0 to (2^64 - 1) inclusive
     * Currently BigIntegers are only used for unsigned_long field type, where the max value is 2^64 - 1.
     * Modify this random generator if a wider range for BigIntegers is necessary.
     * @return a random bigInteger in the range [0 ; 2^64 - 1]
     */
    public static BigInteger randomBigInteger() {
        return new BigInteger(64, random());
    }

    /** A random integer from 0..max (inclusive). */
    public static int randomInt(int max) {
        return RandomizedTest.randomInt(max);
    }

    /** Pick a random object from the given array. The array must not be empty. */
    public static <T> T randomFrom(T... array) {
        return randomFrom(random(), array);
    }

    /** Pick a random object from the given array. The array must not be empty. */
    public static <T> T randomFrom(Random random, T... array) {
        return RandomPicks.randomFrom(random, array);
    }

    /** Pick a random object from the given list. */
    public static <T> T randomFrom(List<T> list) {
        return RandomPicks.randomFrom(random(), list);
    }

    /** Pick a random object from the given collection. */
    public static <T> T randomFrom(Collection<T> collection) {
        return randomFrom(random(), collection);
    }

    /** Pick a random object from the given collection. */
    public static <T> T randomFrom(Random random, Collection<T> collection) {
        return RandomPicks.randomFrom(random, collection);
    }

    public static String randomAlphaOfLengthBetween(int minCodeUnits, int maxCodeUnits) {
        return RandomizedTest.randomAsciiOfLengthBetween(minCodeUnits, maxCodeUnits);
    }

    public static String randomAlphaOfLength(int codeUnits) {
        return RandomizedTest.randomAsciiOfLength(codeUnits);
    }

    public static String randomUnicodeOfLengthBetween(int minCodeUnits, int maxCodeUnits) {
        return RandomizedTest.randomUnicodeOfLengthBetween(minCodeUnits, maxCodeUnits);
    }

    public static String randomUnicodeOfLength(int codeUnits) {
        return RandomizedTest.randomUnicodeOfLength(codeUnits);
    }

    public static String randomUnicodeOfCodepointLengthBetween(int minCodePoints, int maxCodePoints) {
        return RandomizedTest.randomUnicodeOfCodepointLengthBetween(minCodePoints, maxCodePoints);
    }

    public static String randomUnicodeOfCodepointLength(int codePoints) {
        return RandomizedTest.randomUnicodeOfCodepointLength(codePoints);
    }

    public static String randomRealisticUnicodeOfLengthBetween(int minCodeUnits, int maxCodeUnits) {
        return RandomizedTest.randomRealisticUnicodeOfLengthBetween(minCodeUnits, maxCodeUnits);
    }

    public static String randomRealisticUnicodeOfLength(int codeUnits) {
        return RandomizedTest.randomRealisticUnicodeOfLength(codeUnits);
    }

    public static String randomRealisticUnicodeOfCodepointLengthBetween(int minCodePoints, int maxCodePoints) {
        return RandomizedTest.randomRealisticUnicodeOfCodepointLengthBetween(minCodePoints, maxCodePoints);
    }

    public static String randomRealisticUnicodeOfCodepointLength(int codePoints) {
        return RandomizedTest.randomRealisticUnicodeOfCodepointLength(codePoints);
    }

    public static String replaceUnicodeControlCharacters(String uniCodeStr, String toReplaceWith) {
        // replace control characters (https://stackoverflow.com/questions/3438854/replace-unicode-control-characters/)
        return uniCodeStr.replaceAll("\\p{Cc}", toReplaceWith);
    }

    public static String replaceUnicodeControlCharacters(String uniCodeStr) {
        return replaceUnicodeControlCharacters(uniCodeStr, " ");
    }

    /**
     * @param maxArraySize The maximum number of elements in the random array
     * @param stringSize The length of each String in the array
     * @param allowNull Whether the returned array may be null
     * @param allowEmpty Whether the returned array may be empty (have zero elements)
     */
    public static String[] generateRandomStringArray(int maxArraySize, int stringSize, boolean allowNull, boolean allowEmpty) {
        if (allowNull && random().nextBoolean()) {
            return null;
        }
        int arraySize = randomIntBetween(allowEmpty ? 0 : 1, maxArraySize);
        String[] array = new String[arraySize];
        for (int i = 0; i < arraySize; i++) {
            array[i] = RandomStrings.randomAsciiOfLength(random(), stringSize);
        }
        return array;
    }

    public static String[] generateRandomStringArray(int maxArraySize, int stringSize, boolean allowNull) {
        return generateRandomStringArray(maxArraySize, stringSize, allowNull, true);
    }

    public static <T> T[] randomArray(int maxArraySize, IntFunction<T[]> arrayConstructor, Supplier<T> valueConstructor) {
        return randomArray(0, maxArraySize, arrayConstructor, valueConstructor);
    }

    public static <T> T[] randomArray(int minArraySize, int maxArraySize, IntFunction<T[]> arrayConstructor, Supplier<T> valueConstructor) {
        final int size = randomIntBetween(minArraySize, maxArraySize);
        final T[] array = arrayConstructor.apply(size);
        for (int i = 0; i < array.length; i++) {
            array[i] = valueConstructor.get();
        }
        return array;
    }

    public static <T> List<T> randomList(int maxListSize, Supplier<T> valueConstructor) {
        return randomList(0, maxListSize, valueConstructor);
    }

    public static <T> List<T> randomList(int minListSize, int maxListSize, Supplier<T> valueConstructor) {
        final int size = randomIntBetween(minListSize, maxListSize);
        List<T> list = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            list.add(valueConstructor.get());
        }
        return list;
    }

    private static final String[] TIME_SUFFIXES = new String[] { "d", "h", "ms", "s", "m", "micros", "nanos" };

    public static String randomTimeValue(int lower, int upper, String... suffixes) {
        return randomIntBetween(lower, upper) + randomFrom(suffixes);
    }

    public static String randomTimeValue(int lower, int upper) {
        return randomTimeValue(lower, upper, TIME_SUFFIXES);
    }

    public static String randomTimeValue() {
        return randomTimeValue(0, 1000);
    }

    public static String randomPositiveTimeValue() {
        return randomTimeValue(1, 1000);
    }

    /**
     * generate a random DateTimeZone from the ones available in joda library
     */
    public static DateTimeZone randomDateTimeZone() {
        return DateTimeZone.forID(randomFrom(JODA_TIMEZONE_IDS));
    }

    /**
     * generate a random TimeZone from the ones available in java.util
     */
    public static TimeZone randomTimeZone() {
        return TimeZone.getTimeZone(randomJodaAndJavaSupportedTimezone(JAVA_TIMEZONE_IDS));
    }

    /**
     * generate a random TimeZone from the ones available in java.time
     */
    public static ZoneId randomZone() {
        return ZoneId.of(randomJodaAndJavaSupportedTimezone(JAVA_ZONE_IDS));
    }

    /**
     * We need to exclude time zones not supported by joda (like SystemV* timezones)
     * because they cannot be converted back to DateTimeZone which we currently
     * still need to do internally e.g. in bwc serialization and in the extract() method
     * //TODO remove once joda is not supported
     */
    private static String randomJodaAndJavaSupportedTimezone(List<String> zoneIds) {
        return randomValueOtherThanMany(id -> JODA_TIMEZONE_IDS.contains(id) == false, () -> randomFrom(zoneIds));
    }

    /**
     * Generate a random valid date formatter pattern.
     */
    public static String randomDateFormatterPattern() {
        // WEEKYEAR should be used instead of WEEK_YEAR
        EnumSet<FormatNames> formatNames = EnumSet.complementOf(EnumSet.of(FormatNames.WEEK_YEAR));
        return randomFrom(formatNames).getSnakeCaseName();
    }

    /**
     * helper to randomly perform on <code>consumer</code> with <code>value</code>
     */
    public static <T> void maybeSet(Consumer<T> consumer, T value) {
        if (randomBoolean()) {
            consumer.accept(value);
        }
    }

    /**
     * helper to get a random value in a certain range that's different from the input
     */
    public static <T> T randomValueOtherThan(T input, Supplier<T> randomSupplier) {
        return randomValueOtherThanMany(v -> Objects.equals(input, v), randomSupplier);
    }

    /**
     * helper to get a random value in a certain range that's different from the input
     */
    public static <T> T randomValueOtherThanMany(Predicate<T> input, Supplier<T> randomSupplier) {
        T randomValue = null;
        do {
            randomValue = randomSupplier.get();
        } while (input.test(randomValue));
        return randomValue;
    }

    /**
     * Runs the code block for 10 seconds waiting for no assertion to trip.
     */
    public static void assertBusy(CheckedRunnable<Exception> codeBlock) throws Exception {
        assertBusy(codeBlock, 10, TimeUnit.SECONDS);
    }

    /**
     * Runs the code block for the provided interval, waiting for no assertions to trip.
     */
    public static void assertBusy(CheckedRunnable<Exception> codeBlock, long maxWaitTime, TimeUnit unit) throws Exception {
        long maxTimeInMillis = TimeUnit.MILLISECONDS.convert(maxWaitTime, unit);
        // In case you've forgotten your high-school studies, log10(x) / log10(y) == log y(x)
        long iterations = Math.max(Math.round(Math.log10(maxTimeInMillis) / Math.log10(2)), 1);
        long timeInMillis = 1;
        long sum = 0;
        List<AssertionError> failures = new ArrayList<>();
        for (int i = 0; i < iterations; i++) {
            try {
                codeBlock.run();
                return;
            } catch (AssertionError e) {
                failures.add(e);
            }
            sum += timeInMillis;
            Thread.sleep(timeInMillis);
            timeInMillis *= 2;
        }
        timeInMillis = maxTimeInMillis - sum;
        Thread.sleep(Math.max(timeInMillis, 0));
        try {
            codeBlock.run();
        } catch (AssertionError e) {
            for (AssertionError failure : failures) {
                e.addSuppressed(failure);
            }
            throw e;
        }
    }

    /**
     * Runs the code block for the provided max wait time and sleeping for fixed sleep time, waiting for no assertions to trip.
     */
    public static void assertBusyWithFixedSleepTime(CheckedRunnable<Exception> codeBlock, TimeValue maxWaitTime, TimeValue sleepTime)
        throws Exception {
        long maxTimeInMillis = maxWaitTime.millis();
        long sleepTimeInMillis = sleepTime.millis();
        if (sleepTimeInMillis > maxTimeInMillis) {
            throw new IllegalArgumentException("sleepTime is more than the maxWaitTime");
        }
        long sum = 0;
        List<AssertionError> failures = new ArrayList<>();
        while (sum <= maxTimeInMillis) {
            try {
                codeBlock.run();
                return;
            } catch (AssertionError e) {
                failures.add(e);
            }
            sum += sleepTimeInMillis;
            Thread.sleep(sleepTimeInMillis);
        }
        try {
            codeBlock.run();
        } catch (AssertionError e) {
            for (AssertionError failure : failures) {
                e.addSuppressed(failure);
            }
            throw e;
        }
    }

    /**
     * Periodically execute the supplied function until it returns true, or a timeout
     * is reached. This version uses a timeout of 10 seconds. If at all possible,
     * use {@link OpenSearchTestCase#assertBusy(CheckedRunnable)} instead.
     *
     * @param breakSupplier determines whether to return immediately or continue waiting.
     * @return the last value returned by <code>breakSupplier</code>
     * @throws InterruptedException if any sleep calls were interrupted.
     */
    public static boolean waitUntil(BooleanSupplier breakSupplier) throws InterruptedException {
        return waitUntil(breakSupplier, 10, TimeUnit.SECONDS);
    }

    // After 1s, we stop growing the sleep interval exponentially and just sleep 1s until maxWaitTime
    private static final long AWAIT_BUSY_THRESHOLD = 1000L;

    /**
     * Periodically execute the supplied function until it returns true, or until the
     * specified maximum wait time has elapsed. If at all possible, use
     * {@link OpenSearchTestCase#assertBusy(CheckedRunnable)} instead.
     *
     * @param breakSupplier determines whether to return immediately or continue waiting.
     * @param maxWaitTime the maximum amount of time to wait
     * @param unit the unit of tie for <code>maxWaitTime</code>
     * @return the last value returned by <code>breakSupplier</code>
     * @throws InterruptedException if any sleep calls were interrupted.
     */
    public static boolean waitUntil(BooleanSupplier breakSupplier, long maxWaitTime, TimeUnit unit) throws InterruptedException {
        long maxTimeInMillis = TimeUnit.MILLISECONDS.convert(maxWaitTime, unit);
        long timeInMillis = 1;
        long sum = 0;
        while (sum + timeInMillis < maxTimeInMillis) {
            if (breakSupplier.getAsBoolean()) {
                return true;
            }
            Thread.sleep(timeInMillis);
            sum += timeInMillis;
            timeInMillis = Math.min(AWAIT_BUSY_THRESHOLD, timeInMillis * 2);
        }
        timeInMillis = maxTimeInMillis - sum;
        Thread.sleep(Math.max(timeInMillis, 0));
        return breakSupplier.getAsBoolean();
    }

    public static boolean terminate(ExecutorService... services) {
        boolean terminated = true;
        for (ExecutorService service : services) {
            if (service != null) {
                terminated &= ThreadPool.terminate(service, 10, TimeUnit.SECONDS);
            }
        }
        return terminated;
    }

    public static boolean terminate(ThreadPool threadPool) {
        return ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    /**
     * Returns a {@link java.nio.file.Path} pointing to the class path relative resource given
     * as the first argument. In contrast to
     * <code>getClass().getResource(...).getFile()</code> this method will not
     * return URL encoded paths if the parent path contains spaces or other
     * non-standard characters.
     */
    @Override
    public Path getDataPath(String relativePath) {
        // we override LTC behavior here: wrap even resources with mockfilesystems,
        // because some code is buggy when it comes to multiple nio.2 filesystems
        // (e.g. FileSystemUtils, and likely some tests)
        try {
            return PathUtils.get(getClass().getResource(relativePath).toURI()).toAbsolutePath().normalize();
        } catch (Exception e) {
            throw new RuntimeException("resource not found: " + relativePath, e);
        }
    }

    /** Returns a random number of temporary paths. */
    public String[] tmpPaths() {
        final int numPaths = TestUtil.nextInt(random(), 1, 3);
        final String[] absPaths = new String[numPaths];
        for (int i = 0; i < numPaths; i++) {
            absPaths[i] = createTempDir().toAbsolutePath().toString();
        }
        return absPaths;
    }

    public NodeEnvironment newNodeEnvironment() throws IOException {
        return newNodeEnvironment(Settings.EMPTY);
    }

    public Settings buildEnvSettings(Settings settings) {
        return Settings.builder()
            .put(settings)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath())
            .putList(Environment.PATH_DATA_SETTING.getKey(), tmpPaths())
            .build();
    }

    public NodeEnvironment newNodeEnvironment(Settings settings) throws IOException {
        Settings build = buildEnvSettings(settings);
        return new NodeEnvironment(build, TestEnvironment.newEnvironment(build));
    }

    /** Return consistent index settings for the provided index version. */
    public static Settings.Builder settings(Version version) {
        Settings.Builder builder = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version);
        return builder;
    }

    public static Settings.Builder remoteIndexSettings(Version version) {
        Settings.Builder builder = Settings.builder()
            .put(DATA_TO_FILE_CACHE_SIZE_RATIO_SETTING.getKey(), 5)
            .put(IndexMetadata.SETTING_VERSION_CREATED, version)
            .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.REMOTE_SNAPSHOT.getSettingsKey());
        return builder;
    }

    /**
     * Returns size random values
     */
    public static <T> List<T> randomSubsetOf(int size, T... values) {
        List<T> list = arrayAsArrayList(values);
        return randomSubsetOf(size, list);
    }

    /**
     * Returns a random subset of values (including a potential empty list, or the full original list)
     */
    public static <T> List<T> randomSubsetOf(Collection<T> collection) {
        return randomSubsetOf(randomInt(collection.size()), collection);
    }

    /**
     * Returns size random values
     */
    public static <T> List<T> randomSubsetOf(int size, Collection<T> collection) {
        if (size > collection.size()) {
            throw new IllegalArgumentException(
                "Can\'t pick " + size + " random objects from a collection of " + collection.size() + " objects"
            );
        }
        List<T> tempList = new ArrayList<>(collection);
        Collections.shuffle(tempList, random());
        return tempList.subList(0, size);
    }

    /**
     * Builds a set of unique items. Usually you'll get the requested count but you might get less than that number if the supplier returns
     * lots of repeats. Make sure that the items properly implement equals and hashcode.
     */
    public static <T> Set<T> randomUnique(Supplier<T> supplier, int targetCount) {
        Set<T> things = new HashSet<>();
        int maxTries = targetCount * 10;
        for (int t = 0; t < maxTries; t++) {
            if (things.size() == targetCount) {
                return things;
            }
            things.add(supplier.get());
        }
        // Oh well, we didn't get enough unique things. It'll be ok.
        return things;
    }

    public static String randomGeohash(int minPrecision, int maxPrecision) {
        return geohashGenerator.ofStringLength(random(), minPrecision, maxPrecision);
    }

    public static String getTestTransportType() {
        return MockNioTransportPlugin.MOCK_NIO_TRANSPORT_NAME;
    }

    public static Class<? extends Plugin> getTestTransportPlugin() {
        return MockNioTransportPlugin.class;
    }

    private static final GeohashGenerator geohashGenerator = new GeohashGenerator();

    public static class GeohashGenerator extends CodepointSetGenerator {
        private static final char[] ASCII_SET = "0123456789bcdefghjkmnpqrstuvwxyz".toCharArray();

        public GeohashGenerator() {
            super(ASCII_SET);
        }
    }

    /**
     * Returns the bytes that represent the XContent output of the provided {@link ToXContent} object, using the provided
     * {@link XContentType}. Wraps the output into a new anonymous object according to the value returned
     * by the {@link ToXContent#isFragment()} method returns. Shuffles the keys to make sure that parsing never relies on keys ordering.
     *
     * @deprecated use {@link #toShuffledXContent(ToXContent, MediaType, ToXContent.Params, boolean, String...)} instead
     */
    @Deprecated
    protected final BytesReference toShuffledXContent(
        ToXContent toXContent,
        XContentType xContentType,
        ToXContent.Params params,
        boolean humanReadable,
        String... exceptFieldNames
    ) throws IOException {
        BytesReference bytes = XContentHelper.toXContent(toXContent, xContentType, params, humanReadable);
        try (XContentParser parser = createParser(xContentType.xContent(), bytes)) {
            try (XContentBuilder builder = shuffleXContent(parser, rarely(), exceptFieldNames)) {
                return BytesReference.bytes(builder);
            }
        }
    }

    /**
     * Returns the bytes that represent the XContent output of the provided {@link ToXContent} object, using the provided
     * {@link XContentType}. Wraps the output into a new anonymous object according to the value returned
     * by the {@link ToXContent#isFragment()} method returns. Shuffles the keys to make sure that parsing never relies on keys ordering.
     */
    protected final BytesReference toShuffledXContent(
        ToXContent toXContent,
        MediaType mediaType,
        ToXContent.Params params,
        boolean humanReadable,
        String... exceptFieldNames
    ) throws IOException {
        BytesReference bytes = org.opensearch.core.xcontent.XContentHelper.toXContent(toXContent, mediaType, params, humanReadable);
        try (XContentParser parser = createParser(mediaType.xContent(), bytes)) {
            try (XContentBuilder builder = shuffleXContent(parser, rarely(), exceptFieldNames)) {
                return BytesReference.bytes(builder);
            }
        }
    }

    /**
     * Randomly shuffles the fields inside objects in the {@link XContentBuilder} passed in.
     * Recursively goes through inner objects and also shuffles them. Exceptions for this
     * recursive shuffling behavior can be made by passing in the names of fields which
     * internally should stay untouched.
     */
    protected final XContentBuilder shuffleXContent(XContentBuilder builder, String... exceptFieldNames) throws IOException {
        try (XContentParser parser = createParser(builder)) {
            return shuffleXContent(parser, builder.isPrettyPrint(), exceptFieldNames);
        }
    }

    /**
     * Randomly shuffles the fields inside objects parsed using the {@link XContentParser} passed in.
     * Recursively goes through inner objects and also shuffles them. Exceptions for this
     * recursive shuffling behavior can be made by passing in the names of fields which
     * internally should stay untouched.
     */
    public static XContentBuilder shuffleXContent(XContentParser parser, boolean prettyPrint, String... exceptFieldNames)
        throws IOException {
        XContentBuilder xContentBuilder = MediaTypeRegistry.contentBuilder(parser.contentType());
        if (prettyPrint) {
            xContentBuilder.prettyPrint();
        }
        Token token = parser.currentToken() == null ? parser.nextToken() : parser.currentToken();
        if (token == Token.START_ARRAY) {
            List<Object> shuffledList = shuffleList(parser.listOrderedMap(), new HashSet<>(Arrays.asList(exceptFieldNames)));
            return xContentBuilder.value(shuffledList);
        }
        // we need a sorted map for reproducibility, as we are going to shuffle its keys and write XContent back
        Map<String, Object> shuffledMap = shuffleMap(
            (LinkedHashMap<String, Object>) parser.mapOrdered(),
            new HashSet<>(Arrays.asList(exceptFieldNames))
        );
        return xContentBuilder.map(shuffledMap);
    }

    // shuffle fields of objects in the list, but not the list itself
    @SuppressWarnings("unchecked")
    private static List<Object> shuffleList(List<Object> list, Set<String> exceptFields) {
        List<Object> targetList = new ArrayList<>();
        for (Object value : list) {
            if (value instanceof Map) {
                LinkedHashMap<String, Object> valueMap = (LinkedHashMap<String, Object>) value;
                targetList.add(shuffleMap(valueMap, exceptFields));
            } else if (value instanceof List) {
                targetList.add(shuffleList((List) value, exceptFields));
            } else {
                targetList.add(value);
            }
        }
        return targetList;
    }

    @SuppressWarnings("unchecked")
    public static LinkedHashMap<String, Object> shuffleMap(LinkedHashMap<String, Object> map, Set<String> exceptFields) {
        List<String> keys = new ArrayList<>(map.keySet());
        LinkedHashMap<String, Object> targetMap = new LinkedHashMap<>();
        Collections.shuffle(keys, random());
        for (String key : keys) {
            Object value = map.get(key);
            if (value instanceof Map && exceptFields.contains(key) == false) {
                LinkedHashMap<String, Object> valueMap = (LinkedHashMap<String, Object>) value;
                targetMap.put(key, shuffleMap(valueMap, exceptFields));
            } else if (value instanceof List && exceptFields.contains(key) == false) {
                targetMap.put(key, shuffleList((List) value, exceptFields));
            } else {
                targetMap.put(key, value);
            }
        }
        return targetMap;
    }

    /**
     * Create a copy of an original {@link Writeable} object by running it through a {@link BytesStreamOutput} and
     * reading it in again using a provided {@link Writeable.Reader}. The stream that is wrapped around the {@link StreamInput}
     * potentially need to use a {@link NamedWriteableRegistry}, so this needs to be provided too (although it can be
     * empty if the object that is streamed doesn't contain any {@link NamedWriteable} objects itself.
     */
    public static <T extends Writeable> T copyWriteable(
        T original,
        NamedWriteableRegistry namedWriteableRegistry,
        Writeable.Reader<T> reader
    ) throws IOException {
        return copyWriteable(original, namedWriteableRegistry, reader, Version.CURRENT);
    }

    /**
     * Same as {@link #copyWriteable(Writeable, NamedWriteableRegistry, Writeable.Reader)} but also allows to provide
     * a {@link Version} argument which will be used to write and read back the object.
     */
    public static <T extends Writeable> T copyWriteable(
        T original,
        NamedWriteableRegistry namedWriteableRegistry,
        Writeable.Reader<T> reader,
        Version version
    ) throws IOException {
        return copyInstance(original, namedWriteableRegistry, (out, value) -> value.writeTo(out), reader, version);
    }

    /**
     * Create a copy of an original {@link NamedWriteable} object by running it through a {@link BytesStreamOutput} and
     * reading it in again using a provided {@link Writeable.Reader}.
     */
    public static <T extends NamedWriteable> T copyNamedWriteable(
        T original,
        NamedWriteableRegistry namedWriteableRegistry,
        Class<T> categoryClass
    ) throws IOException {
        return copyNamedWriteable(original, namedWriteableRegistry, categoryClass, Version.CURRENT);
    }

    /**
     * Same as {@link #copyNamedWriteable(NamedWriteable, NamedWriteableRegistry, Class)} but also allows to provide
     * a {@link Version} argument which will be used to write and read back the object.
     */
    public static <T extends NamedWriteable> T copyNamedWriteable(
        T original,
        NamedWriteableRegistry namedWriteableRegistry,
        Class<T> categoryClass,
        Version version
    ) throws IOException {
        return copyInstance(
            original,
            namedWriteableRegistry,
            (out, value) -> out.writeNamedWriteable(value),
            in -> in.readNamedWriteable(categoryClass),
            version
        );
    }

    protected static <T> T copyInstance(
        T original,
        NamedWriteableRegistry namedWriteableRegistry,
        Writeable.Writer<T> writer,
        Writeable.Reader<T> reader,
        Version version
    ) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setVersion(version);
            writer.write(output, original);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry)) {
                in.setVersion(version);
                return reader.read(in);
            }
        }
    }

    /**
     * Create a new {@link XContentParser}.
     */
    protected final XContentParser createParser(XContentBuilder builder) throws IOException {
        return createParser(builder.contentType().xContent(), BytesReference.bytes(builder));
    }

    /**
     * Create a new {@link XContentParser}.
     */
    protected final XContentParser createParser(XContent xContent, String data) throws IOException {
        return xContent.createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, data);
    }

    /**
     * Create a new {@link XContentParser}.
     */
    protected final XContentParser createParser(XContent xContent, InputStream data) throws IOException {
        return xContent.createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, data);
    }

    /**
     * Create a new {@link XContentParser}.
     */
    protected final XContentParser createParser(XContent xContent, byte[] data) throws IOException {
        return xContent.createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, data);
    }

    /**
     * Create a new {@link XContentParser}.
     */
    protected final XContentParser createParser(XContent xContent, BytesReference data) throws IOException {
        return createParser(xContentRegistry(), xContent, data);
    }

    /**
     * Create a new {@link XContentParser}.
     */
    protected final XContentParser createParser(NamedXContentRegistry namedXContentRegistry, XContent xContent, BytesReference data)
        throws IOException {
        if (data instanceof BytesArray) {
            final BytesArray array = (BytesArray) data;
            return xContent.createParser(
                namedXContentRegistry,
                LoggingDeprecationHandler.INSTANCE,
                array.array(),
                array.offset(),
                array.length()
            );
        }
        return xContent.createParser(namedXContentRegistry, LoggingDeprecationHandler.INSTANCE, data.streamInput());
    }

    private static final NamedXContentRegistry DEFAULT_NAMED_X_CONTENT_REGISTRY = new NamedXContentRegistry(
        ClusterModule.getNamedXWriteables()
    );

    protected static final NamedWriteableRegistry DEFAULT_NAMED_WRITABLE_REGISTRY = new NamedWriteableRegistry(
        ClusterModule.getNamedWriteables()
    );

    /**
     * The {@link NamedXContentRegistry} to use for this test. Subclasses should override and use liberally.
     */
    protected NamedXContentRegistry xContentRegistry() {
        return DEFAULT_NAMED_X_CONTENT_REGISTRY;
    }

    /**
     * The {@link NamedWriteableRegistry} to use for this test. Subclasses should override and use liberally.
     */
    protected NamedWriteableRegistry writableRegistry() {
        return new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
    }

    /**
     * The {@link PersistedStateRegistry} to use for this test. Subclasses should override and use liberally.
     */
    protected PersistedStateRegistry persistedStateRegistry() {
        return new PersistedStateRegistry();
    }

    /**
     * Create a "mock" script for use either with {@link MockScriptEngine} or anywhere where you need a script but don't really care about
     * its contents.
     */
    public static Script mockScript(String id) {
        return new Script(ScriptType.INLINE, MockScriptEngine.NAME, id, emptyMap());
    }

    /** Returns the suite failure marker: internal use only! */
    public static TestRuleMarkFailure getSuiteFailureMarker() {
        return suiteFailureMarker;
    }

    /** Compares two stack traces, ignoring module (which is not yet serialized) */
    public static void assertArrayEquals(StackTraceElement expected[], StackTraceElement actual[]) {
        assertEquals(expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], actual[i]);
        }
    }

    /** Compares two stack trace elements, ignoring module (which is not yet serialized) */
    public static void assertEquals(StackTraceElement expected, StackTraceElement actual) {
        assertEquals(expected.getClassName(), actual.getClassName());
        assertEquals(expected.getMethodName(), actual.getMethodName());
        assertEquals(expected.getFileName(), actual.getFileName());
        assertEquals(expected.getLineNumber(), actual.getLineNumber());
        assertEquals(expected.isNativeMethod(), actual.isNativeMethod());
    }

    protected static long spinForAtLeastOneMillisecond() {
        return spinForAtLeastNMilliseconds(1);
    }

    protected static long spinForAtLeastNMilliseconds(final long ms) {
        long nanosecondsInMillisecond = TimeUnit.NANOSECONDS.convert(ms, TimeUnit.MILLISECONDS);
        /*
         * Force at least ms milliseconds to elapse, but ensure the clock has enough resolution to
         * observe the passage of time.
         */
        long start = System.nanoTime();
        long elapsed;
        while ((elapsed = (System.nanoTime() - start)) < nanosecondsInMillisecond) {
            // busy spin
        }
        return elapsed;
    }

    /**
     * Creates an IndexAnalyzers with a single default analyzer
     */
    protected IndexAnalyzers createDefaultIndexAnalyzers() {
        return new IndexAnalyzers(
            Collections.singletonMap("default", new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer())),
            Collections.emptyMap(),
            Collections.emptyMap()
        );
    }

    /**
     * Creates an TestAnalysis with all the default analyzers configured.
     */
    public static TestAnalysis createTestAnalysis(Index index, Settings settings, AnalysisPlugin... analysisPlugins) throws IOException {
        Settings nodeSettings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        return createTestAnalysis(index, nodeSettings, settings, analysisPlugins);
    }

    /**
     * Creates an TestAnalysis with all the default analyzers configured.
     */
    public static TestAnalysis createTestAnalysis(Index index, Settings nodeSettings, Settings settings, AnalysisPlugin... analysisPlugins)
        throws IOException {
        Settings indexSettings = Settings.builder().put(settings).put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build();
        return createTestAnalysis(IndexSettingsModule.newIndexSettings(index, indexSettings), nodeSettings, analysisPlugins);
    }

    /**
     * Creates an TestAnalysis with all the default analyzers configured.
     */
    public static TestAnalysis createTestAnalysis(IndexSettings indexSettings, Settings nodeSettings, AnalysisPlugin... analysisPlugins)
        throws IOException {
        Environment env = TestEnvironment.newEnvironment(nodeSettings);
        AnalysisModule analysisModule = new AnalysisModule(env, Arrays.asList(analysisPlugins));
        AnalysisRegistry analysisRegistry = analysisModule.getAnalysisRegistry();
        return new TestAnalysis(
            analysisRegistry.build(indexSettings),
            analysisRegistry.buildTokenFilterFactories(indexSettings),
            analysisRegistry.buildTokenizerFactories(indexSettings),
            analysisRegistry.buildCharFilterFactories(indexSettings)
        );
    }

    /**
     * This cute helper class just holds all analysis building blocks that are used
     * to build IndexAnalyzers. This is only for testing since in production we only need the
     * result and we don't even expose it there.
     */
    public static final class TestAnalysis {

        public final IndexAnalyzers indexAnalyzers;
        public final Map<String, TokenFilterFactory> tokenFilter;
        public final Map<String, TokenizerFactory> tokenizer;
        public final Map<String, CharFilterFactory> charFilter;

        public TestAnalysis(
            IndexAnalyzers indexAnalyzers,
            Map<String, TokenFilterFactory> tokenFilter,
            Map<String, TokenizerFactory> tokenizer,
            Map<String, CharFilterFactory> charFilter
        ) {
            this.indexAnalyzers = indexAnalyzers;
            this.tokenFilter = tokenFilter;
            this.tokenizer = tokenizer;
            this.charFilter = charFilter;
        }
    }

    private static boolean isUnusableLocale() {
        return inFipsJvm()
            && (Locale.getDefault().toLanguageTag().equals("th-TH")
                || Locale.getDefault().toLanguageTag().equals("ja-JP-u-ca-japanese-x-lvariant-JP")
                || Locale.getDefault().toLanguageTag().equals("th-TH-u-nu-thai-x-lvariant-TH"));
    }

    public static boolean inFipsJvm() {
        return Boolean.parseBoolean(System.getProperty(FIPS_SYSPROP));
    }

    /**
     * Returns a unique port range for this JVM starting from the computed base port
     */
    public static String getPortRange() {
        return getBasePort() + "-" + (getBasePort() + 99); // upper bound is inclusive
    }

    protected static int getBasePort() {
        // some tests use MockTransportService to do network based testing. Yet, we run tests in multiple JVMs that means
        // concurrent tests could claim port that another JVM just released and if that test tries to simulate a disconnect it might
        // be smart enough to re-connect depending on what is tested. To reduce the risk, since this is very hard to debug we use
        // a different default port range per JVM unless the incoming settings override it
        // use a non-default base port otherwise some cluster in this JVM might reuse a port

        // We rely on Gradle implementation details here, the worker IDs are long values incremented by one for the
        // lifespan of the daemon this means that they can get larger than the allowed port range.
        // Ephemeral ports on Linux start at 32768 so we modulo to make sure that we don't exceed that.
        // This is safe as long as we have fewer than 224 Gradle workers running in parallel
        // See also: https://github.com/elastic/elasticsearch/issues/44134
        final String workerIdStr = System.getProperty(OpenSearchTestCase.TEST_WORKER_SYS_PROPERTY);
        final int startAt;
        if (workerIdStr == null) {
            startAt = 0; // IDE
        } else {
            // we adjust the gradle worker id with mod so as to not go over the ephemoral port ranges, but gradle continually
            // increases this value, so the mod can eventually become zero, thus we shift on both sides by 1
            final long workerId = Long.valueOf(workerIdStr);
            assert workerId >= 1 : "Non positive gradle worker id: " + workerIdStr;
            startAt = (int) Math.floorMod(workerId - 1, 223L) + 1;
        }
        assert startAt >= 0 : "Unexpected test worker Id, resulting port range would be negative";
        return 10300 + (startAt * 100);
    }

    protected static InetAddress randomIp(boolean v4) {
        try {
            if (v4) {
                byte[] ipv4 = new byte[4];
                random().nextBytes(ipv4);
                return InetAddress.getByAddress(ipv4);
            } else {
                byte[] ipv6 = new byte[16];
                random().nextBytes(ipv6);
                return InetAddress.getByAddress(ipv6);
            }
        } catch (UnknownHostException e) {
            throw new AssertionError();
        }
    }

    public static BlobPath getShardLevelBlobPath(
        Client client,
        String remoteStoreIndex,
        BlobPath basePath,
        String shardId,
        RemoteStoreEnums.DataCategory dataCategory,
        RemoteStoreEnums.DataType dataType,
        String fixedPrefix
    ) {
        String indexUUID = client.admin()
            .indices()
            .prepareGetSettings(remoteStoreIndex)
            .get()
            .getSetting(remoteStoreIndex, IndexMetadata.SETTING_INDEX_UUID);
        ClusterState state = client.admin().cluster().prepareState().execute().actionGet().getState();
        Map<String, String> remoteCustomData = state.metadata()
            .index(remoteStoreIndex)
            .getCustomData(IndexMetadata.REMOTE_STORE_CUSTOM_KEY);
        RemoteStoreEnums.PathType type = Objects.isNull(remoteCustomData)
            ? RemoteStoreEnums.PathType.FIXED
            : RemoteStoreEnums.PathType.valueOf(remoteCustomData.get(RemoteStoreEnums.PathType.NAME));
        RemoteStoreEnums.PathHashAlgorithm hashAlgorithm = Objects.nonNull(remoteCustomData)
            ? remoteCustomData.containsKey(RemoteStoreEnums.PathHashAlgorithm.NAME)
                ? RemoteStoreEnums.PathHashAlgorithm.valueOf(remoteCustomData.get(RemoteStoreEnums.PathHashAlgorithm.NAME))
                : null
            : null;
        RemoteStorePathStrategy.ShardDataPathInput pathInput = RemoteStorePathStrategy.ShardDataPathInput.builder()
            .basePath(basePath)
            .indexUUID(indexUUID)
            .shardId(shardId)
            .dataCategory(dataCategory)
            .dataType(dataType)
            .fixedPrefix(fixedPrefix)
            .build();
        return type.path(pathInput, hashAlgorithm);
    }
}
