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

package org.opensearch.common.settings;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.opensearch.common.Strings;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.rest.FakeRestRequest;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.equalTo;

public class SettingsFilterTests extends OpenSearchTestCase {
    public void testAddingAndRemovingFilters() {
        HashSet<String> hashSet = new HashSet<>(Arrays.asList("foo", "bar", "baz"));
        SettingsFilter settingsFilter = new SettingsFilter(hashSet);
        assertEquals(settingsFilter.getPatterns(), hashSet);
    }

    public void testSettingsFiltering() throws IOException {

        testFiltering(
            Settings.builder()
                .put("foo", "foo_test")
                .put("foo1", "foo1_test")
                .put("bar", "bar_test")
                .put("bar1", "bar1_test")
                .put("bar.2", "bar2_test")
                .build(),
            Settings.builder().put("foo1", "foo1_test").build(),
            "foo",
            "bar*"
        );

        testFiltering(
            Settings.builder()
                .put("foo", "foo_test")
                .put("foo1", "foo1_test")
                .put("bar", "bar_test")
                .put("bar1", "bar1_test")
                .put("bar.2", "bar2_test")
                .build(),
            Settings.builder().put("foo", "foo_test").put("foo1", "foo1_test").build(),
            "bar*"
        );

        testFiltering(
            Settings.builder()
                .put("foo", "foo_test")
                .put("foo1", "foo1_test")
                .put("bar", "bar_test")
                .put("bar1", "bar1_test")
                .put("bar.2", "bar2_test")
                .build(),
            Settings.builder().build(),
            "foo",
            "bar*",
            "foo*"
        );

        testFiltering(
            Settings.builder().put("foo", "foo_test").put("bar", "bar_test").put("baz", "baz_test").build(),
            Settings.builder().put("foo", "foo_test").put("bar", "bar_test").put("baz", "baz_test").build()
        );

        testFiltering(
            Settings.builder().put("a.b.something.d", "foo_test").put("a.b.something.c", "foo1_test").build(),
            Settings.builder().put("a.b.something.c", "foo1_test").build(),
            "a.b.*.d"
        );
    }

    public void testFilteredSettingIsNotLogged() throws Exception {
        Settings oldSettings = Settings.builder().put("key", "old").build();
        Settings newSettings = Settings.builder().put("key", "new").build();

        Setting<String> filteredSetting = Setting.simpleString("key", Property.Filtered);
        assertExpectedLogMessages(
            (testLogger) -> Setting.logSettingUpdate(filteredSetting, newSettings, oldSettings, testLogger),
            new MockLogAppender.SeenEventExpectation("secure logging", "org.opensearch.test", Level.INFO, "updating [key]"),
            new MockLogAppender.UnseenEventExpectation("unwanted old setting name", "org.opensearch.test", Level.INFO, "*old*"),
            new MockLogAppender.UnseenEventExpectation("unwanted new setting name", "org.opensearch.test", Level.INFO, "*new*")
        );
    }

    public void testRegularSettingUpdateIsFullyLogged() throws Exception {
        Settings oldSettings = Settings.builder().put("key", "old").build();
        Settings newSettings = Settings.builder().put("key", "new").build();

        Setting<String> regularSetting = Setting.simpleString("key");
        assertExpectedLogMessages(
            (testLogger) -> Setting.logSettingUpdate(regularSetting, newSettings, oldSettings, testLogger),
            new MockLogAppender.SeenEventExpectation(
                "regular logging",
                "org.opensearch.test",
                Level.INFO,
                "updating [key] from [old] to [new]"
            )
        );
    }

    private void assertExpectedLogMessages(Consumer<Logger> consumer, MockLogAppender.LoggingExpectation... expectations)
        throws IllegalAccessException {
        Logger testLogger = LogManager.getLogger("org.opensearch.test");
        try (MockLogAppender appender = MockLogAppender.createForLoggers(testLogger)) {
            Arrays.stream(expectations).forEach(appender::addExpectation);
            consumer.accept(testLogger);
            appender.assertAllExpectationsMatched();
        }
    }

    private void testFiltering(Settings source, Settings filtered, String... patterns) throws IOException {
        SettingsFilter settingsFilter = new SettingsFilter(Arrays.asList(patterns));

        // Test using direct filtering
        Settings filteredSettings = settingsFilter.filter(source);
        assertThat(filteredSettings, equalTo(filtered));

        // Test using toXContent filtering
        RestRequest request = new FakeRestRequest();
        settingsFilter.addFilterSettingParams(request);
        XContentBuilder xContentBuilder = XContentBuilder.builder(JsonXContent.jsonXContent);
        xContentBuilder.startObject();
        source.toXContent(xContentBuilder, request);
        xContentBuilder.endObject();
        String filteredSettingsString = Strings.toString(xContentBuilder);
        filteredSettings = Settings.builder().loadFromSource(filteredSettingsString, xContentBuilder.contentType()).build();
        assertThat(filteredSettings, equalTo(filtered));
    }
}
