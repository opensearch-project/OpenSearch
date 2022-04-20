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

package org.opensearch.test.junit.listeners;

import com.carrotsearch.randomizedtesting.ReproduceErrorMessageBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Constants;
import org.opensearch.common.Strings;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.yaml.OpenSearchClientYamlSuiteTestCase;
import org.junit.internal.AssumptionViolatedException;
import org.junit.runner.Description;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

import java.util.Locale;
import java.util.TimeZone;

import static com.carrotsearch.randomizedtesting.SysGlobals.SYSPROP_ITERATIONS;
import static com.carrotsearch.randomizedtesting.SysGlobals.SYSPROP_PREFIX;
import static com.carrotsearch.randomizedtesting.SysGlobals.SYSPROP_TESTCLASS;
import static com.carrotsearch.randomizedtesting.SysGlobals.SYSPROP_TESTMETHOD;

/**
 * A {@link RunListener} that emits a command you can use to re-run a failing test with the failing random seed to
 * {@link System#err}.
 */
public class ReproduceInfoPrinter extends RunListener {

    protected final Logger logger = LogManager.getLogger(OpenSearchTestCase.class);

    @Override
    public void testStarted(Description description) throws Exception {
        logger.trace("Test {} started", description.getDisplayName());
    }

    @Override
    public void testFinished(Description description) throws Exception {
        logger.trace("Test {} finished", description.getDisplayName());
    }

    /**
     * Are we in the integ test phase?
     */
    static boolean inVerifyPhase() {
        return Boolean.parseBoolean(System.getProperty("tests.verify.phase"));
    }

    @Override
    public void testFailure(Failure failure) throws Exception {
        // Ignore assumptions.
        if (failure.getException() instanceof AssumptionViolatedException) {
            return;
        }

        final String gradlew = Constants.WINDOWS ? "gradlew" : "./gradlew";
        final StringBuilder b = new StringBuilder("REPRODUCE WITH: " + gradlew + " ");
        String task = System.getProperty("tests.task");

        // append Gradle test runner test filter string
        b.append("'" + task + "'");
        b.append(" --tests \"");
        b.append(failure.getDescription().getClassName());
        final String methodName = failure.getDescription().getMethodName();
        if (methodName != null) {
            // fallback to system property filter when tests contain "."
            if (methodName.contains(".")) {
                b.append("\" -Dtests.method=\"");
                b.append(methodName);
            } else {
                b.append(".");
                b.append(methodName);
            }
        }
        b.append("\"");

        GradleMessageBuilder gradleMessageBuilder = new GradleMessageBuilder(b);
        gradleMessageBuilder.appendAllOpts(failure.getDescription());

        // Client yaml suite tests are a special case as they allow for additional parameters
        if (OpenSearchClientYamlSuiteTestCase.class.isAssignableFrom(failure.getDescription().getTestClass())) {
            gradleMessageBuilder.appendClientYamlSuiteProperties();
        }

        printToErr(b.toString());
    }

    @SuppressForbidden(reason = "printing repro info")
    private static void printToErr(String s) {
        System.err.println(s);
    }

    protected static class GradleMessageBuilder extends ReproduceErrorMessageBuilder {

        public GradleMessageBuilder(StringBuilder b) {
            super(b);
        }

        @Override
        public ReproduceErrorMessageBuilder appendAllOpts(Description description) {
            super.appendAllOpts(description);

            return appendESProperties();
        }

        @Override
        public ReproduceErrorMessageBuilder appendEnvironmentSettings() {
            // we handle our own environment settings
            return this;
        }

        /**
         * Append a single VM option.
         */
        @Override
        public ReproduceErrorMessageBuilder appendOpt(String sysPropName, String value) {
            if (sysPropName.equals(SYSPROP_ITERATIONS())) { // we don't want the iters to be in there!
                return this;
            }
            if (sysPropName.equals(SYSPROP_TESTCLASS())) {
                // don't print out the test class, we print it ourselves in appendAllOpts
                // without filtering out the parameters (needed for REST tests)
                return this;
            }
            if (sysPropName.equals(SYSPROP_TESTMETHOD())) {
                // don't print out the test method, we print it ourselves in appendAllOpts
                // without filtering out the parameters (needed for REST tests)
                return this;
            }
            if (sysPropName.equals(SYSPROP_PREFIX())) {
                // we always use the default prefix
                return this;
            }
            if (Strings.hasLength(value)) {
                return super.appendOpt(sysPropName, value);
            }
            return this;
        }

        private ReproduceErrorMessageBuilder appendESProperties() {
            appendProperties("tests.opensearch.logger.level");
            if (inVerifyPhase()) {
                // these properties only make sense for integration tests
                appendProperties(OpenSearchIntegTestCase.TESTS_ENABLE_MOCK_MODULES);
            }
            appendProperties(
                "tests.assertion.disabled",
                "tests.security.manager",
                "tests.nightly",
                "tests.jvms",
                "tests.client.ratio",
                "tests.heap.size",
                "tests.bwc",
                "tests.bwc.version",
                "build.snapshot"
            );
            if (System.getProperty("tests.jvm.argline") != null && !System.getProperty("tests.jvm.argline").isEmpty()) {
                appendOpt("tests.jvm.argline", "\"" + System.getProperty("tests.jvm.argline") + "\"");
            }
            appendOpt("tests.locale", Locale.getDefault().toLanguageTag());
            appendOpt("tests.timezone", TimeZone.getDefault().getID());
            appendOpt("runtime.java", Integer.toString(Runtime.version().version().get(0)));
            appendOpt(OpenSearchTestCase.FIPS_SYSPROP, System.getProperty(OpenSearchTestCase.FIPS_SYSPROP));
            return this;
        }

        public ReproduceErrorMessageBuilder appendClientYamlSuiteProperties() {
            return appendProperties(
                OpenSearchClientYamlSuiteTestCase.REST_TESTS_SUITE,
                OpenSearchClientYamlSuiteTestCase.REST_TESTS_DENYLIST
            );
        }

        protected ReproduceErrorMessageBuilder appendProperties(String... properties) {
            for (String sysPropName : properties) {
                if (Strings.hasLength(System.getProperty(sysPropName))) {
                    appendOpt(sysPropName, System.getProperty(sysPropName));
                }
            }
            return this;
        }

    }
}
