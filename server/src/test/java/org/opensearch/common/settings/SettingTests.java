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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.AbstractScopedSettings.SettingUpdater;
import org.opensearch.common.settings.Setting.ByteSizeValueParser;
import org.opensearch.common.settings.Setting.DoubleParser;
import org.opensearch.common.settings.Setting.FloatParser;
import org.opensearch.common.settings.Setting.IntegerParser;
import org.opensearch.common.settings.Setting.LongParser;
import org.opensearch.common.settings.Setting.MemorySizeValueParser;
import org.opensearch.common.settings.Setting.MinMaxTimeValueParser;
import org.opensearch.common.settings.Setting.MinTimeValueParser;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Setting.RegexValidator;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.index.IndexSettings;
import org.opensearch.monitor.jvm.JvmInfo;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.junit.annotations.TestLogging;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.index.IndexSettingsTests.newIndexMeta;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class SettingTests extends OpenSearchTestCase {

    public void testGet() {
        Setting<Boolean> booleanSetting = Setting.boolSetting("foo.bar", false, Property.Dynamic, Property.NodeScope);
        assertFalse(booleanSetting.get(Settings.EMPTY));
        assertFalse(booleanSetting.get(Settings.builder().put("foo.bar", false).build()));
        assertTrue(booleanSetting.get(Settings.builder().put("foo.bar", true).build()));
    }

    public void testByteSizeSetting() {
        final Setting<ByteSizeValue> byteSizeValueSetting = Setting.byteSizeSetting(
            "a.byte.size",
            new ByteSizeValue(1024),
            Property.Dynamic,
            Property.NodeScope
        );
        assertFalse(byteSizeValueSetting.isGroupSetting());
        final ByteSizeValue byteSizeValue = byteSizeValueSetting.get(Settings.EMPTY);
        assertThat(byteSizeValue.getBytes(), equalTo(1024L));
    }

    public void testByteSizeSettingMinValue() {
        final Setting<ByteSizeValue> byteSizeValueSetting = Setting.byteSizeSetting(
            "a.byte.size",
            new ByteSizeValue(100, ByteSizeUnit.MB),
            new ByteSizeValue(20_000_000, ByteSizeUnit.BYTES),
            new ByteSizeValue(Integer.MAX_VALUE, ByteSizeUnit.BYTES)
        );
        final long value = 20_000_000 - randomIntBetween(1, 1024);
        final Settings settings = Settings.builder().put("a.byte.size", value + "b").build();
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> byteSizeValueSetting.get(settings));
        final String expectedMessage = "failed to parse value [" + value + "b] for setting [a.byte.size], must be >= [20000000b]";
        assertThat(e, hasToString(containsString(expectedMessage)));
    }

    public void testByteSizeSettingMaxValue() {
        final Setting<ByteSizeValue> byteSizeValueSetting = Setting.byteSizeSetting(
            "a.byte.size",
            new ByteSizeValue(100, ByteSizeUnit.MB),
            new ByteSizeValue(16, ByteSizeUnit.MB),
            new ByteSizeValue(Integer.MAX_VALUE, ByteSizeUnit.BYTES)
        );
        final long value = (1L << 31) - 1 + randomIntBetween(1, 1024);
        final Settings settings = Settings.builder().put("a.byte.size", value + "b").build();
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> byteSizeValueSetting.get(settings));
        final String expectedMessage = "failed to parse value [" + value + "b] for setting [a.byte.size], must be <= [2147483647b]";
        assertThat(e, hasToString(containsString(expectedMessage)));
    }

    public void testByteSizeSettingValidation() {
        final Setting<ByteSizeValue> byteSizeValueSetting = Setting.byteSizeSetting(
            "a.byte.size",
            s -> "2048b",
            Property.Dynamic,
            Property.NodeScope
        );
        final ByteSizeValue byteSizeValue = byteSizeValueSetting.get(Settings.EMPTY);
        assertThat(byteSizeValue.getBytes(), equalTo(2048L));
        AtomicReference<ByteSizeValue> value = new AtomicReference<>(null);
        ClusterSettings.SettingUpdater<ByteSizeValue> settingUpdater = byteSizeValueSetting.newUpdater(value::set, logger);

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> settingUpdater.apply(Settings.builder().put("a.byte.size", 12).build(), Settings.EMPTY)
        );
        assertThat(e, hasToString(containsString("illegal value can't update [a.byte.size] from [2048b] to [12]")));
        assertNotNull(e.getCause());
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        final IllegalArgumentException cause = (IllegalArgumentException) e.getCause();
        final String expected = "failed to parse setting [a.byte.size] with value [12] as a size in bytes: unit is missing or unrecognized";
        assertThat(cause, hasToString(containsString(expected)));
        assertTrue(settingUpdater.apply(Settings.builder().put("a.byte.size", "12b").build(), Settings.EMPTY));
        assertThat(value.get(), equalTo(new ByteSizeValue(12)));
    }

    public void testMemorySize() {
        Setting<ByteSizeValue> memorySizeValueSetting = Setting.memorySizeSetting(
            "a.byte.size",
            new ByteSizeValue(1024),
            Property.Dynamic,
            Property.NodeScope
        );

        assertFalse(memorySizeValueSetting.isGroupSetting());
        ByteSizeValue memorySizeValue = memorySizeValueSetting.get(Settings.EMPTY);
        assertEquals(memorySizeValue.getBytes(), 1024);

        memorySizeValueSetting = Setting.memorySizeSetting("a.byte.size", s -> "2048b", Property.Dynamic, Property.NodeScope);
        memorySizeValue = memorySizeValueSetting.get(Settings.EMPTY);
        assertEquals(memorySizeValue.getBytes(), 2048);

        memorySizeValueSetting = Setting.memorySizeSetting("a.byte.size", "50%", Property.Dynamic, Property.NodeScope);
        assertFalse(memorySizeValueSetting.isGroupSetting());
        memorySizeValue = memorySizeValueSetting.get(Settings.EMPTY);
        assertEquals(memorySizeValue.getBytes(), JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.5, 1.0);

        memorySizeValueSetting = Setting.memorySizeSetting("a.byte.size", s -> "25%", Property.Dynamic, Property.NodeScope);
        memorySizeValue = memorySizeValueSetting.get(Settings.EMPTY);
        assertEquals(memorySizeValue.getBytes(), JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.25, 1.0);

        AtomicReference<ByteSizeValue> value = new AtomicReference<>(null);
        ClusterSettings.SettingUpdater<ByteSizeValue> settingUpdater = memorySizeValueSetting.newUpdater(value::set, logger);
        try {
            settingUpdater.apply(Settings.builder().put("a.byte.size", 12).build(), Settings.EMPTY);
            fail("no unit");
        } catch (IllegalArgumentException ex) {
            assertThat(ex, hasToString(containsString("illegal value can't update [a.byte.size] from [25%] to [12]")));
            assertNotNull(ex.getCause());
            assertThat(ex.getCause(), instanceOf(IllegalArgumentException.class));
            final IllegalArgumentException cause = (IllegalArgumentException) ex.getCause();
            final String expected =
                "failed to parse setting [a.byte.size] with value [12] as a size in bytes: unit is missing or unrecognized";
            assertThat(cause, hasToString(containsString(expected)));
        }

        assertTrue(settingUpdater.apply(Settings.builder().put("a.byte.size", "12b").build(), Settings.EMPTY));
        assertEquals(new ByteSizeValue(12), value.get());

        assertTrue(settingUpdater.apply(Settings.builder().put("a.byte.size", "20%").build(), Settings.EMPTY));
        assertEquals(new ByteSizeValue((long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.2)), value.get());
    }

    public void testMemorySizeWithFallbackValue() {
        Setting<ByteSizeValue> fallbackSetting = Setting.memorySizeSetting("a.byte.size", "20%", Property.Dynamic, Property.NodeScope);
        Setting<ByteSizeValue> memorySizeSetting = Setting.memorySizeSetting(
            "b.byte.size",
            fallbackSetting,
            Property.Dynamic,
            Property.NodeScope
        );
        AtomicReference<ByteSizeValue> value = new AtomicReference<>(null);
        ClusterSettings.SettingUpdater<ByteSizeValue> settingUpdater = memorySizeSetting.newUpdater(value::set, logger);

        ByteSizeValue memorySizeValue = memorySizeSetting.get(Settings.EMPTY);
        assertEquals(memorySizeValue.getBytes(), JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.2, 1.0);

        assertTrue(settingUpdater.apply(Settings.builder().put("a.byte.size", "30%").build(), Settings.EMPTY));
        // If value=getHeapMax()*0.3 is bigger than 2gb, and is bigger than Integer.MAX_VALUE,
        // then (long)((int) value) will lose precision.
        assertEquals(new ByteSizeValue((long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.3)), value.get());

        assertTrue(settingUpdater.apply(Settings.builder().put("b.byte.size", "40%").build(), Settings.EMPTY));
        assertEquals(new ByteSizeValue((long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.4)), value.get());
    }

    public void testSimpleUpdate() {
        Setting<Boolean> booleanSetting = Setting.boolSetting("foo.bar", false, Property.Dynamic, Property.NodeScope);
        AtomicReference<Boolean> atomicBoolean = new AtomicReference<>(null);
        ClusterSettings.SettingUpdater<Boolean> settingUpdater = booleanSetting.newUpdater(atomicBoolean::set, logger);
        Settings build = Settings.builder().put("foo.bar", false).build();
        settingUpdater.apply(build, Settings.EMPTY);
        assertNull(atomicBoolean.get());
        build = Settings.builder().put("foo.bar", true).build();
        settingUpdater.apply(build, Settings.EMPTY);
        assertTrue(atomicBoolean.get());

        // try update bogus value
        build = Settings.builder().put("foo.bar", "I am not a boolean").build();
        try {
            settingUpdater.apply(build, Settings.EMPTY);
            fail("not a boolean");
        } catch (IllegalArgumentException ex) {
            assertThat(ex, hasToString(containsString("illegal value can't update [foo.bar] from [false] to [I am not a boolean]")));
            assertNotNull(ex.getCause());
            assertThat(ex.getCause(), instanceOf(IllegalArgumentException.class));
            final IllegalArgumentException cause = (IllegalArgumentException) ex.getCause();
            assertThat(
                cause,
                hasToString(containsString("Failed to parse value [I am not a boolean] as only [true] or [false] are allowed."))
            );
        }
    }

    public void testSimpleUpdateOfFilteredSetting() {
        Setting<Boolean> booleanSetting = Setting.boolSetting("foo.bar", false, Property.Dynamic, Property.Filtered);
        AtomicReference<Boolean> atomicBoolean = new AtomicReference<>(null);
        ClusterSettings.SettingUpdater<Boolean> settingUpdater = booleanSetting.newUpdater(atomicBoolean::set, logger);

        // try update bogus value
        Settings build = Settings.builder().put("foo.bar", "I am not a boolean").build();
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> settingUpdater.apply(build, Settings.EMPTY));
        assertThat(ex, hasToString(equalTo("java.lang.IllegalArgumentException: illegal value can't update [foo.bar]")));
        assertNull(ex.getCause());
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/33135")
    public void testValidateStringSetting() {
        Settings settings = Settings.builder().putList("foo.bar", Arrays.asList("bla-a", "bla-b")).build();
        Setting<String> stringSetting = Setting.simpleString("foo.bar", Property.NodeScope);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> stringSetting.get(settings));
        assertEquals("Found list type value for setting [foo.bar] but but did not expect a list for it.", e.getMessage());
    }

    private static final Setting<String> FOO_BAR_SETTING = new Setting<>(
        "foo.bar",
        "foobar",
        Function.identity(),
        new FooBarValidator(),
        Property.Dynamic,
        Property.NodeScope
    );

    private static final Setting<String> BAZ_QUX_SETTING = Setting.simpleString("baz.qux", Property.NodeScope);
    private static final Setting<String> QUUX_QUUZ_SETTING = Setting.simpleString("quux.quuz", Property.NodeScope);

    static class FooBarValidator implements Setting.Validator<String> {

        public static boolean invokedInIsolation;
        public static boolean invokedWithDependencies;

        @Override
        public void validate(final String value) {
            invokedInIsolation = true;
            assertThat(value, equalTo("foo.bar value"));
        }

        @Override
        public void validate(final String value, final Map<Setting<?>, Object> settings) {
            invokedWithDependencies = true;
            assertTrue(settings.keySet().contains(BAZ_QUX_SETTING));
            assertThat(settings.get(BAZ_QUX_SETTING), equalTo("baz.qux value"));
            assertTrue(settings.keySet().contains(QUUX_QUUZ_SETTING));
            assertThat(settings.get(QUUX_QUUZ_SETTING), equalTo("quux.quuz value"));
        }

        @Override
        public Iterator<Setting<?>> settings() {
            final List<Setting<?>> settings = Arrays.asList(BAZ_QUX_SETTING, QUUX_QUUZ_SETTING);
            return settings.iterator();
        }
    }

    // the purpose of this test is merely to ensure that a validator is invoked with the appropriate values
    public void testValidator() {
        final Settings settings = Settings.builder()
            .put("foo.bar", "foo.bar value")
            .put("baz.qux", "baz.qux value")
            .put("quux.quuz", "quux.quuz value")
            .build();
        FOO_BAR_SETTING.get(settings);
        assertTrue(FooBarValidator.invokedInIsolation);
        assertTrue(FooBarValidator.invokedWithDependencies);
    }

    public void testRegexValidator() throws Exception {
        // A regex that matches one or more digits
        String expectedRegex = "\\d+";
        Pattern expectedPattern = Pattern.compile(expectedRegex);
        RegexValidator regexValidator = new RegexValidator(expectedRegex);
        RegexValidator regexValidatorMatcherFalse = new RegexValidator(expectedRegex, false);

        // Test that the pattern is correctly initialized
        assertNotNull(expectedPattern);
        assertNotNull(regexValidator.getPattern());
        assertEquals(expectedPattern.pattern(), regexValidator.getPattern().pattern());

        // Test that checks the pattern and isMatching with the set value false parameters are working correctly during initialization
        assertNotNull(regexValidatorMatcherFalse);
        assertNotNull(regexValidatorMatcherFalse.getPattern());
        assertEquals(expectedPattern.pattern(), regexValidatorMatcherFalse.getPattern().pattern());

        // Test throw an exception when the value does not match
        final RegexValidator finalValidator = new RegexValidator(expectedRegex);
        assertThrows(IllegalArgumentException.class, () -> finalValidator.validate("foo"));
        try {
            regexValidator.validate("123");
        } catch (IllegalArgumentException e) {
            fail("Expected validate() to not throw an exception, but it threw " + e);
        }

        // Test throws an exception when the value matches
        final RegexValidator finalValidatorFalse = new RegexValidator(expectedRegex);
        assertThrows(IllegalArgumentException.class, () -> finalValidatorFalse.validate(expectedRegex));
        try {
            regexValidatorMatcherFalse.validate(expectedRegex);
        } catch (IllegalArgumentException e) {
            fail("Expected validate() to not throw an exception, but it threw " + e);
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            regexValidator.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                regexValidator = new RegexValidator(in);
                assertEquals(expectedPattern.pattern(), regexValidator.getPattern().pattern());

                // Test that validate() throws an exception for invalid input
                final RegexValidator newFinalValidator = new RegexValidator(expectedRegex);
                assertThrows(IllegalArgumentException.class, () -> newFinalValidator.validate("foo"));

                // Test that validate() does not throw an exception for valid input
                try {
                    regexValidator.validate("123");
                } catch (IllegalArgumentException e) {
                    fail("Expected validate() to not throw an exception, but it threw " + e);
                }
            }
        }
    }

    public void testValidatorForFilteredStringSetting() {
        final Setting<String> filteredStringSetting = new Setting<>("foo.bar", "foobar", Function.identity(), value -> {
            throw new SettingsException("validate always fails");
        }, Property.Filtered);

        final Settings settings = Settings.builder().put(filteredStringSetting.getKey(), filteredStringSetting.getKey() + " value").build();
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> filteredStringSetting.get(settings));
        assertThat(e, hasToString(containsString("Failed to parse value for setting [" + filteredStringSetting.getKey() + "]")));
        assertThat(e.getCause(), instanceOf(SettingsException.class));
        assertThat(e.getCause(), hasToString(containsString("validate always fails")));
    }

    public void testFilteredFloatSetting() {
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> Setting.floatSetting("foo", 42.0f, 43.0f, Property.Filtered)
        );
        assertThat(e, hasToString(containsString("Failed to parse value for setting [foo] must be >= 43.0")));
    }

    public void testFilteredDoubleSetting() {
        final IllegalArgumentException e1 = expectThrows(
            IllegalArgumentException.class,
            () -> Setting.doubleSetting("foo", 42.0, 43.0, Property.Filtered)
        );
        assertThat(e1, hasToString(containsString("Failed to parse value for setting [foo] must be >= 43.0")));

        final IllegalArgumentException e2 = expectThrows(
            IllegalArgumentException.class,
            () -> Setting.doubleSetting("foo", 45.0, 43.0, 44.0, Property.Filtered)
        );
        assertThat(e2, hasToString(containsString("Failed to parse value for setting [foo] must be <= 44.0")));
    }

    public void testFilteredIntSetting() {
        final IllegalArgumentException e1 = expectThrows(
            IllegalArgumentException.class,
            () -> Setting.intSetting("foo", 42, 43, 44, Property.Filtered)
        );
        assertThat(e1, hasToString(containsString("Failed to parse value for setting [foo] must be >= 43")));

        final IllegalArgumentException e2 = expectThrows(
            IllegalArgumentException.class,
            () -> Setting.intSetting("foo", 45, 43, 44, Property.Filtered)
        );
        assertThat(e2, hasToString(containsString("Failed to parse value for setting [foo] must be <= 44")));
    }

    public void testFilteredLongSetting() {
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> Setting.longSetting("foo", 42L, 43L, Property.Filtered)
        );
        assertThat(e, hasToString(containsString("Failed to parse value for setting [foo] must be >= 43")));
    }

    public void testFilteredTimeSetting() {
        final IllegalArgumentException e1 = expectThrows(
            IllegalArgumentException.class,
            () -> Setting.timeSetting("foo", TimeValue.timeValueHours(1), TimeValue.timeValueHours(2), Property.Filtered)
        );
        assertThat(e1, hasToString(containsString("failed to parse value for setting [foo], must be >= [2h]")));

        final IllegalArgumentException e2 = expectThrows(
            IllegalArgumentException.class,
            () -> Setting.timeSetting(
                "foo",
                TimeValue.timeValueHours(4),
                TimeValue.timeValueHours(2),
                TimeValue.timeValueHours(3),
                Property.Filtered
            )
        );
        assertThat(e2, hasToString(containsString("failed to parse value for setting [foo], must be <= [3h]")));

        final Setting minSetting = Setting.timeSetting("foo", TimeValue.timeValueHours(3), TimeValue.timeValueHours(2), Property.Filtered);
        final Settings minSettings = Settings.builder().put("foo", "not a time value").build();
        final IllegalArgumentException e3 = expectThrows(IllegalArgumentException.class, () -> minSetting.get(minSettings));
        assertThat(e3, hasToString(containsString("failed to parse value for setting [foo] as a time value")));
        assertNull(e3.getCause());

        final Setting maxSetting = Setting.timeSetting(
            "foo",
            TimeValue.timeValueHours(3),
            TimeValue.timeValueHours(2),
            TimeValue.timeValueHours(4),
            Property.Filtered
        );
        final Settings maxSettings = Settings.builder().put("foo", "not a time value").build();
        final IllegalArgumentException e4 = expectThrows(IllegalArgumentException.class, () -> maxSetting.get(maxSettings));
        assertThat(e4, hasToString(containsString("failed to parse value for setting [foo] as a time value")));
        assertNull(e4.getCause());
    }

    public void testFilteredBooleanSetting() {
        Setting setting = Setting.boolSetting("foo", false, Property.Filtered);
        final Settings settings = Settings.builder().put("foo", "not a boolean value").build();

        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> setting.get(settings));
        assertThat(e, hasToString(containsString("Failed to parse value for setting [foo]")));
        assertNull(e.getCause());
    }

    public void testUpdateNotDynamic() {
        Setting<Boolean> booleanSetting = Setting.boolSetting("foo.bar", false, Property.NodeScope);
        assertFalse(booleanSetting.isGroupSetting());
        AtomicReference<Boolean> atomicBoolean = new AtomicReference<>(null);
        try {
            booleanSetting.newUpdater(atomicBoolean::set, logger);
            fail("not dynamic");
        } catch (IllegalStateException ex) {
            assertEquals("setting [foo.bar] is not dynamic", ex.getMessage());
        }
    }

    public void testUpdaterIsIsolated() {
        Setting<Boolean> booleanSetting = Setting.boolSetting("foo.bar", false, Property.Dynamic, Property.NodeScope);
        AtomicReference<Boolean> ab1 = new AtomicReference<>(null);
        AtomicReference<Boolean> ab2 = new AtomicReference<>(null);
        ClusterSettings.SettingUpdater<Boolean> settingUpdater = booleanSetting.newUpdater(ab1::set, logger);
        settingUpdater.apply(Settings.builder().put("foo.bar", true).build(), Settings.EMPTY);
        assertTrue(ab1.get());
        assertNull(ab2.get());
    }

    public void testDefault() {
        TimeValue defaultValue = TimeValue.timeValueMillis(randomIntBetween(0, 1000000));
        Setting<TimeValue> setting = Setting.positiveTimeSetting("my.time.value", defaultValue, Property.NodeScope);
        assertFalse(setting.isGroupSetting());
        String aDefault = setting.getDefaultRaw(Settings.EMPTY);
        assertEquals(defaultValue.millis() + "ms", aDefault);
        assertEquals(defaultValue.millis(), setting.get(Settings.EMPTY).millis());
        assertEquals(defaultValue, setting.getDefault(Settings.EMPTY));

        Setting<String> secondaryDefault = new Setting<>(
            "foo.bar",
            (s) -> s.get("old.foo.bar", "some_default"),
            Function.identity(),
            Property.NodeScope
        );
        assertEquals("some_default", secondaryDefault.get(Settings.EMPTY));
        assertEquals("42", secondaryDefault.get(Settings.builder().put("old.foo.bar", 42).build()));

        Setting<String> secondaryDefaultViaSettings = new Setting<>("foo.bar", secondaryDefault, Function.identity(), Property.NodeScope);
        assertEquals("some_default", secondaryDefaultViaSettings.get(Settings.EMPTY));
        assertEquals("42", secondaryDefaultViaSettings.get(Settings.builder().put("old.foo.bar", 42).build()));

        // It gets more complicated when there are two settings objects....
        Settings hasFallback = Settings.builder().put("foo.bar", "o").build();
        Setting<String> fallsback = new Setting<>("foo.baz", secondaryDefault, Function.identity(), Property.NodeScope);
        assertEquals("o", fallsback.get(hasFallback));
        assertEquals("some_default", fallsback.get(Settings.EMPTY));
        assertEquals("some_default", fallsback.get(Settings.EMPTY, Settings.EMPTY));
        assertEquals("o", fallsback.get(Settings.EMPTY, hasFallback));
        assertEquals("o", fallsback.get(hasFallback, Settings.EMPTY));
        assertEquals("a", fallsback.get(Settings.builder().put("foo.bar", "a").build(), Settings.builder().put("foo.bar", "b").build()));
    }

    public void testComplexType() {
        AtomicReference<ComplexType> ref = new AtomicReference<>(null);
        Setting<ComplexType> setting = new Setting<>("foo.bar", (s) -> "", (s) -> new ComplexType(s), Property.Dynamic, Property.NodeScope);
        assertFalse(setting.isGroupSetting());
        ref.set(setting.get(Settings.EMPTY));
        ComplexType type = ref.get();
        ClusterSettings.SettingUpdater<ComplexType> settingUpdater = setting.newUpdater(ref::set, logger);
        assertFalse(settingUpdater.apply(Settings.EMPTY, Settings.EMPTY));
        assertSame("no update - type has not changed", type, ref.get());

        // change from default
        assertTrue(settingUpdater.apply(Settings.builder().put("foo.bar", "2").build(), Settings.EMPTY));
        assertNotSame("update - type has changed", type, ref.get());
        assertEquals("2", ref.get().foo);

        // change back to default...
        assertTrue(settingUpdater.apply(Settings.EMPTY, Settings.builder().put("foo.bar", "2").build()));
        assertNotSame("update - type has changed", type, ref.get());
        assertEquals("", ref.get().foo);
    }

    public void testType() {
        Setting<Integer> integerSetting = Setting.intSetting("foo.int.bar", 1, Property.Dynamic, Property.NodeScope);
        assertThat(integerSetting.hasNodeScope(), is(true));
        assertThat(integerSetting.hasIndexScope(), is(false));
        integerSetting = Setting.intSetting("foo.int.bar", 1, Property.Dynamic, Property.IndexScope);
        assertThat(integerSetting.hasIndexScope(), is(true));
        assertThat(integerSetting.hasNodeScope(), is(false));
    }

    public void testGroups() {
        AtomicReference<Settings> ref = new AtomicReference<>(null);
        Setting<Settings> setting = Setting.groupSetting("foo.bar.", Property.Dynamic, Property.NodeScope);
        assertTrue(setting.isGroupSetting());
        ClusterSettings.SettingUpdater<Settings> settingUpdater = setting.newUpdater(ref::set, logger);

        Settings currentInput = Settings.builder()
            .put("foo.bar.1.value", "1")
            .put("foo.bar.2.value", "2")
            .put("foo.bar.3.value", "3")
            .build();
        Settings previousInput = Settings.EMPTY;
        assertTrue(settingUpdater.apply(currentInput, previousInput));
        assertNotNull(ref.get());
        Settings settings = ref.get();
        Map<String, Settings> asMap = settings.getAsGroups();
        assertEquals(3, asMap.size());
        assertEquals(asMap.get("1").get("value"), "1");
        assertEquals(asMap.get("2").get("value"), "2");
        assertEquals(asMap.get("3").get("value"), "3");

        previousInput = currentInput;
        currentInput = Settings.builder().put("foo.bar.1.value", "1").put("foo.bar.2.value", "2").put("foo.bar.3.value", "3").build();
        Settings current = ref.get();
        assertFalse(settingUpdater.apply(currentInput, previousInput));
        assertSame(current, ref.get());

        previousInput = currentInput;
        currentInput = Settings.builder().put("foo.bar.1.value", "1").put("foo.bar.2.value", "2").build();
        // now update and check that we got it
        assertTrue(settingUpdater.apply(currentInput, previousInput));
        assertNotSame(current, ref.get());

        asMap = ref.get().getAsGroups();
        assertEquals(2, asMap.size());
        assertEquals(asMap.get("1").get("value"), "1");
        assertEquals(asMap.get("2").get("value"), "2");

        previousInput = currentInput;
        currentInput = Settings.builder().put("foo.bar.1.value", "1").put("foo.bar.2.value", "4").build();
        // now update and check that we got it
        assertTrue(settingUpdater.apply(currentInput, previousInput));
        assertNotSame(current, ref.get());

        asMap = ref.get().getAsGroups();
        assertEquals(2, asMap.size());
        assertEquals(asMap.get("1").get("value"), "1");
        assertEquals(asMap.get("2").get("value"), "4");

        assertTrue(setting.match("foo.bar.baz"));
        assertFalse(setting.match("foo.baz.bar"));

        ClusterSettings.SettingUpdater<Settings> predicateSettingUpdater = setting.newUpdater(ref::set, logger, (s) -> assertFalse(true));
        try {
            predicateSettingUpdater.apply(
                Settings.builder().put("foo.bar.1.value", "1").put("foo.bar.2.value", "2").build(),
                Settings.EMPTY
            );
            fail("not accepted");
        } catch (IllegalArgumentException ex) {
            assertEquals(ex.getMessage(), "illegal value can't update [foo.bar.] from [{}] to [{\"1.value\":\"1\",\"2.value\":\"2\"}]");
        }
    }

    public void testGroupSettingFallback() {
        Setting<Settings> fallbackSetting = Setting.groupSetting("old.bar.", Property.Dynamic, Property.NodeScope);
        assertFalse(fallbackSetting.exists(Settings.EMPTY));

        Setting<Settings> groupSetting = Setting.groupSetting("new.bar.", fallbackSetting, Property.Dynamic, Property.NodeScope);
        assertFalse(groupSetting.exists(Settings.EMPTY));

        Settings values = Settings.builder().put("old.bar.1.value", "value 1").put("old.bar.2.value", "value 2").build();

        assertTrue(groupSetting.exists(values));

        Map<String, Settings> asMap = groupSetting.get(values).getAsGroups();
        assertEquals(2, asMap.size());
        assertEquals("value 1", asMap.get("1").get("value"));
        assertEquals("value 2", asMap.get("2").get("value"));
    }

    public void testFilteredGroups() {
        AtomicReference<Settings> ref = new AtomicReference<>(null);
        Setting<Settings> setting = Setting.groupSetting("foo.bar.", Property.Filtered, Property.Dynamic);

        ClusterSettings.SettingUpdater<Settings> predicateSettingUpdater = setting.newUpdater(ref::set, logger, (s) -> assertFalse(true));
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> predicateSettingUpdater.apply(
                Settings.builder().put("foo.bar.1.value", "1").put("foo.bar.2.value", "2").build(),
                Settings.EMPTY
            )
        );
        assertEquals("illegal value can't update [foo.bar.]", ex.getMessage());
    }

    public static class ComplexType {

        final String foo;

        public ComplexType(String foo) {
            this.foo = foo;
        }
    }

    public static class Composite {

        private Integer b;
        private Integer a;

        public void set(Integer a, Integer b) {
            this.a = a;
            this.b = b;
        }

        public void validate(Integer a, Integer b) {
            if (Integer.signum(a) != Integer.signum(b)) {
                throw new IllegalArgumentException("boom");
            }
        }
    }

    public void testComposite() {
        Composite c = new Composite();
        Setting<Integer> a = Setting.intSetting("foo.int.bar.a", 1, Property.Dynamic, Property.NodeScope);
        Setting<Integer> b = Setting.intSetting("foo.int.bar.b", 1, Property.Dynamic, Property.NodeScope);
        ClusterSettings.SettingUpdater<Tuple<Integer, Integer>> settingUpdater = Setting.compoundUpdater(c::set, c::validate, a, b, logger);
        assertFalse(settingUpdater.apply(Settings.EMPTY, Settings.EMPTY));
        assertNull(c.a);
        assertNull(c.b);

        Settings build = Settings.builder().put("foo.int.bar.a", 2).build();
        assertTrue(settingUpdater.apply(build, Settings.EMPTY));
        assertEquals(2, c.a.intValue());
        assertEquals(1, c.b.intValue());

        Integer aValue = c.a;
        assertFalse(settingUpdater.apply(build, build));
        assertSame(aValue, c.a);
        Settings previous = build;
        build = Settings.builder().put("foo.int.bar.a", 2).put("foo.int.bar.b", 5).build();
        assertTrue(settingUpdater.apply(build, previous));
        assertEquals(2, c.a.intValue());
        assertEquals(5, c.b.intValue());

        // reset to default
        assertTrue(settingUpdater.apply(Settings.EMPTY, build));
        assertEquals(1, c.a.intValue());
        assertEquals(1, c.b.intValue());

    }

    public void testCompositeValidator() {
        Composite c = new Composite();
        Setting<Integer> a = Setting.intSetting("foo.int.bar.a", 1, Property.Dynamic, Property.NodeScope);
        Setting<Integer> b = Setting.intSetting("foo.int.bar.b", 1, Property.Dynamic, Property.NodeScope);
        ClusterSettings.SettingUpdater<Tuple<Integer, Integer>> settingUpdater = Setting.compoundUpdater(c::set, c::validate, a, b, logger);
        assertFalse(settingUpdater.apply(Settings.EMPTY, Settings.EMPTY));
        assertNull(c.a);
        assertNull(c.b);

        Settings build = Settings.builder().put("foo.int.bar.a", 2).build();
        assertTrue(settingUpdater.apply(build, Settings.EMPTY));
        assertEquals(2, c.a.intValue());
        assertEquals(1, c.b.intValue());

        Integer aValue = c.a;
        assertFalse(settingUpdater.apply(build, build));
        assertSame(aValue, c.a);
        Settings previous = build;
        build = Settings.builder().put("foo.int.bar.a", 2).put("foo.int.bar.b", 5).build();
        assertTrue(settingUpdater.apply(build, previous));
        assertEquals(2, c.a.intValue());
        assertEquals(5, c.b.intValue());

        Settings invalid = Settings.builder().put("foo.int.bar.a", -2).put("foo.int.bar.b", 5).build();
        IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> settingUpdater.apply(invalid, previous));
        assertThat(exc.getMessage(), equalTo("boom"));

        // reset to default
        assertTrue(settingUpdater.apply(Settings.EMPTY, build));
        assertEquals(1, c.a.intValue());
        assertEquals(1, c.b.intValue());

    }

    public void testListSettingsDeprecated() {
        final Setting<List<String>> deprecatedListSetting = Setting.listSetting(
            "foo.deprecated",
            Collections.singletonList("foo.deprecated"),
            Function.identity(),
            Property.Deprecated,
            Property.NodeScope
        );
        final Setting<List<String>> nonDeprecatedListSetting = Setting.listSetting(
            "foo.non_deprecated",
            Collections.singletonList("foo.non_deprecated"),
            Function.identity(),
            Property.NodeScope
        );
        final Settings settings = Settings.builder()
            .put("foo.deprecated", "foo.deprecated1,foo.deprecated2")
            .put("foo.deprecated", "foo.non_deprecated1,foo.non_deprecated2")
            .build();
        deprecatedListSetting.get(settings);
        nonDeprecatedListSetting.get(settings);
        assertSettingDeprecationsAndWarnings(new Setting[] { deprecatedListSetting });
    }

    public void testListSettings() {
        Setting<List<String>> listSetting = Setting.listSetting(
            "foo.bar",
            Arrays.asList("foo,bar"),
            (s) -> s.toString(),
            Property.Dynamic,
            Property.NodeScope
        );
        List<String> value = listSetting.get(Settings.EMPTY);
        assertFalse(listSetting.exists(Settings.EMPTY));
        assertEquals(1, value.size());
        assertEquals("foo,bar", value.get(0));

        List<String> input = Arrays.asList("test", "test1, test2", "test", ",,,,");
        Settings.Builder builder = Settings.builder().putList("foo.bar", input.toArray(new String[0]));
        assertTrue(listSetting.exists(builder.build()));
        value = listSetting.get(builder.build());
        assertEquals(input.size(), value.size());
        assertArrayEquals(value.toArray(new String[0]), input.toArray(new String[0]));

        // try to parse this really annoying format
        builder = Settings.builder();
        for (int i = 0; i < input.size(); i++) {
            builder.put("foo.bar." + i, input.get(i));
        }
        value = listSetting.get(builder.build());
        assertEquals(input.size(), value.size());
        assertArrayEquals(value.toArray(new String[0]), input.toArray(new String[0]));
        assertTrue(listSetting.exists(builder.build()));

        AtomicReference<List<String>> ref = new AtomicReference<>();
        AbstractScopedSettings.SettingUpdater<List<String>> settingUpdater = listSetting.newUpdater(ref::set, logger);
        assertTrue(settingUpdater.hasChanged(builder.build(), Settings.EMPTY));
        settingUpdater.apply(builder.build(), Settings.EMPTY);
        assertEquals(input.size(), ref.get().size());
        assertArrayEquals(ref.get().toArray(new String[0]), input.toArray(new String[0]));

        settingUpdater.apply(Settings.builder().putList("foo.bar", "123").build(), builder.build());
        assertEquals(1, ref.get().size());
        assertArrayEquals(ref.get().toArray(new String[0]), new String[] { "123" });

        settingUpdater.apply(Settings.builder().put("foo.bar", "1,2,3").build(), Settings.builder().putList("foo.bar", "123").build());
        assertEquals(3, ref.get().size());
        assertArrayEquals(ref.get().toArray(new String[0]), new String[] { "1", "2", "3" });

        settingUpdater.apply(Settings.EMPTY, Settings.builder().put("foo.bar", "1,2,3").build());
        assertEquals(1, ref.get().size());
        assertEquals("foo,bar", ref.get().get(0));

        Setting<List<Integer>> otherSettings = Setting.listSetting(
            "foo.bar",
            Collections.emptyList(),
            Integer::parseInt,
            Property.Dynamic,
            Property.NodeScope
        );
        List<Integer> defaultValue = otherSettings.get(Settings.EMPTY);
        assertEquals(0, defaultValue.size());
        List<Integer> intValues = otherSettings.get(Settings.builder().put("foo.bar", "0,1,2,3").build());
        assertEquals(4, intValues.size());
        for (int i = 0; i < intValues.size(); i++) {
            assertEquals(i, intValues.get(i).intValue());
        }

        Setting<List<String>> settingWithFallback = Setting.listSetting(
            "foo.baz",
            listSetting,
            Function.identity(),
            Property.Dynamic,
            Property.NodeScope
        );
        value = settingWithFallback.get(Settings.EMPTY);
        assertEquals(1, value.size());
        assertEquals("foo,bar", value.get(0));

        value = settingWithFallback.get(Settings.builder().putList("foo.bar", "1", "2").build());
        assertEquals(2, value.size());
        assertEquals("1", value.get(0));
        assertEquals("2", value.get(1));

        value = settingWithFallback.get(Settings.builder().putList("foo.baz", "3", "4").build());
        assertEquals(2, value.size());
        assertEquals("3", value.get(0));
        assertEquals("4", value.get(1));

        value = settingWithFallback.get(Settings.builder().putList("foo.baz", "3", "4").putList("foo.bar", "1", "2").build());
        assertEquals(2, value.size());
        assertEquals("3", value.get(0));
        assertEquals("4", value.get(1));
    }

    public void testListSettingAcceptsNumberSyntax() {
        Setting<List<String>> listSetting = Setting.listSetting(
            "foo.bar",
            Arrays.asList("foo,bar"),
            (s) -> s.toString(),
            Property.Dynamic,
            Property.NodeScope
        );
        List<String> input = Arrays.asList("test", "test1, test2", "test", ",,,,");
        Settings.Builder builder = Settings.builder().putList("foo.bar", input.toArray(new String[0]));
        // try to parse this really annoying format
        for (String key : builder.keys()) {
            assertTrue("key: " + key + " doesn't match", listSetting.match(key));
        }
        builder = Settings.builder().put("foo.bar", "1,2,3");
        for (String key : builder.keys()) {
            assertTrue("key: " + key + " doesn't match", listSetting.match(key));
        }
        assertFalse(listSetting.match("foo_bar"));
        assertFalse(listSetting.match("foo_bar.1"));
        assertTrue(listSetting.match("foo.bar"));
        assertTrue(listSetting.match("foo.bar." + randomIntBetween(0, 10000)));
    }

    public void testDynamicKeySetting() {
        Setting<Boolean> setting = Setting.prefixKeySetting("foo.", (key) -> Setting.boolSetting(key, false, Property.NodeScope));
        assertTrue(setting.hasComplexMatcher());
        assertTrue(setting.match("foo.bar"));
        assertFalse(setting.match("foo"));
        Setting<Boolean> concreteSetting = setting.getConcreteSetting("foo.bar");
        assertTrue(concreteSetting.get(Settings.builder().put("foo.bar", "true").build()));
        assertFalse(concreteSetting.get(Settings.builder().put("foo.baz", "true").build()));

        try {
            setting.getConcreteSetting("foo");
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("key [foo] must match [foo.] but didn't.", ex.getMessage());
        }
    }

    public void testAffixKeySettingWithDynamicPrefix() {
        Setting.AffixSetting<Boolean> setting = Setting.suffixKeySetting(
            "enable",
            (key) -> Setting.boolSetting(key, false, Property.NodeScope)
        );
        Setting<Boolean> concreteSetting = setting.getConcreteSettingForNamespace("foo.bar");
        assertEquals("foo.bar.enable", concreteSetting.getKey());

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> setting.getConcreteSettingForNamespace("foo."));
        assertEquals("key [foo..enable] must match [*.enable] but didn't.", ex.getMessage());
    }

    public void testAffixKeySetting() {
        Setting<Boolean> setting = Setting.affixKeySetting("foo.", "enable", (key) -> Setting.boolSetting(key, false, Property.NodeScope));
        assertTrue(setting.hasComplexMatcher());
        assertTrue(setting.match("foo.bar.enable"));
        assertTrue(setting.match("foo.baz.enable"));
        assertFalse(setting.match("foo.bar.baz.enable"));
        assertFalse(setting.match("foo.bar"));
        assertFalse(setting.match("foo.bar.baz.enabled"));
        assertFalse(setting.match("foo"));
        Setting<Boolean> concreteSetting = setting.getConcreteSetting("foo.bar.enable");
        assertTrue(concreteSetting.get(Settings.builder().put("foo.bar.enable", "true").build()));
        assertFalse(concreteSetting.get(Settings.builder().put("foo.baz.enable", "true").build()));

        IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> setting.getConcreteSetting("foo"));
        assertEquals("key [foo] must match [foo.*.enable] but didn't.", exc.getMessage());

        exc = expectThrows(
            IllegalArgumentException.class,
            () -> Setting.affixKeySetting("foo", "enable", (key) -> Setting.boolSetting(key, false, Property.NodeScope))
        );
        assertEquals("prefix must end with a '.'", exc.getMessage());

        Setting<List<String>> listAffixSetting = Setting.affixKeySetting(
            "foo.",
            "bar",
            (key) -> Setting.listSetting(key, Collections.emptyList(), Function.identity(), Property.NodeScope)
        );

        assertTrue(listAffixSetting.hasComplexMatcher());
        assertTrue(listAffixSetting.match("foo.test.bar"));
        assertTrue(listAffixSetting.match("foo.test_1.bar"));
        assertFalse(listAffixSetting.match("foo.buzz.baz.bar"));
        assertFalse(listAffixSetting.match("foo.bar"));
        assertFalse(listAffixSetting.match("foo.baz"));
        assertFalse(listAffixSetting.match("foo"));
    }

    public void testAffixSettingNamespaces() {
        Setting.AffixSetting<Boolean> setting = Setting.affixKeySetting(
            "foo.",
            "enable",
            (key) -> Setting.boolSetting(key, false, Property.NodeScope)
        );
        Settings build = Settings.builder()
            .put("foo.bar.enable", "true")
            .put("foo.baz.enable", "true")
            .put("foo.boom.enable", "true")
            .put("something.else", "true")
            .build();
        Set<String> namespaces = setting.getNamespaces(build);
        assertEquals(3, namespaces.size());
        assertTrue(namespaces.contains("bar"));
        assertTrue(namespaces.contains("baz"));
        assertTrue(namespaces.contains("boom"));
    }

    public void testAffixAsMap() {
        Setting.AffixSetting<String> setting = Setting.prefixKeySetting("foo.bar.", key -> Setting.simpleString(key, Property.NodeScope));
        Settings build = Settings.builder().put("foo.bar.baz", 2).put("foo.bar.foobar", 3).build();
        Map<String, String> asMap = setting.getAsMap(build);
        assertEquals(2, asMap.size());
        assertEquals("2", asMap.get("baz"));
        assertEquals("3", asMap.get("foobar"));

        setting = Setting.prefixKeySetting("foo.bar.", key -> Setting.simpleString(key, Property.NodeScope));
        build = Settings.builder().put("foo.bar.baz", 2).put("foo.bar.foobar", 3).put("foo.bar.baz.deep", 45).build();
        asMap = setting.getAsMap(build);
        assertEquals(3, asMap.size());
        assertEquals("2", asMap.get("baz"));
        assertEquals("3", asMap.get("foobar"));
        assertEquals("45", asMap.get("baz.deep"));
    }

    public void testGetAllConcreteSettings() {
        Setting.AffixSetting<List<String>> listAffixSetting = Setting.affixKeySetting(
            "foo.",
            "bar",
            (key) -> Setting.listSetting(key, Collections.emptyList(), Function.identity(), Property.NodeScope)
        );

        Settings settings = Settings.builder()
            .putList("foo.1.bar", "1", "2")
            .putList("foo.2.bar", "3", "4", "5")
            .putList("foo.bar", "6")
            .putList("some.other", "6")
            .putList("foo.3.bar", "6")
            .build();
        Stream<Setting<List<String>>> allConcreteSettings = listAffixSetting.getAllConcreteSettings(settings);
        Map<String, List<String>> collect = allConcreteSettings.collect(Collectors.toMap(Setting::getKey, (s) -> s.get(settings)));
        assertEquals(3, collect.size());
        assertEquals(Arrays.asList("1", "2"), collect.get("foo.1.bar"));
        assertEquals(Arrays.asList("3", "4", "5"), collect.get("foo.2.bar"));
        assertEquals(Arrays.asList("6"), collect.get("foo.3.bar"));
    }

    public void testAffixSettingsFailOnGet() {
        Setting.AffixSetting<List<String>> listAffixSetting = Setting.affixKeySetting(
            "foo.",
            "bar",
            (key) -> Setting.listSetting(key, Collections.singletonList("testelement"), Function.identity(), Property.NodeScope)
        );
        expectThrows(UnsupportedOperationException.class, () -> listAffixSetting.get(Settings.EMPTY));
        assertEquals(Collections.singletonList("testelement"), listAffixSetting.getDefault(Settings.EMPTY));
        assertEquals("[\"testelement\"]", listAffixSetting.getDefaultRaw(Settings.EMPTY));
    }

    public void testAffixSettingsValidatorDependencies() {
        Setting<Integer> affix = Setting.affixKeySetting("abc.", "def", k -> Setting.intSetting(k, 10));
        Setting<Integer> fix0 = Setting.intSetting("abc.tuv", 20, 0);
        Setting<Integer> fix1 = Setting.intSetting("abc.qrx", 20, 0, new Setting.Validator<Integer>() {
            @Override
            public void validate(Integer value) {}

            String toString(Map<Setting<?>, Object> s) {
                return s.entrySet()
                    .stream()
                    .map(e -> e.getKey().getKey() + ":" + e.getValue().toString())
                    .sorted()
                    .collect(Collectors.joining(","));
            }

            @Override
            public void validate(Integer value, Map<Setting<?>, Object> settings, boolean isPresent) {
                if (settings.get(fix0).equals(fix0.getDefault(Settings.EMPTY))) {
                    settings.remove(fix0);
                }
                if (settings.size() == 1) {
                    throw new IllegalArgumentException(toString(settings));
                } else if (settings.size() == 2) {
                    throw new IllegalArgumentException(toString(settings));
                }
            }

            @Override
            public Iterator<Setting<?>> settings() {
                List<Setting<?>> a = Arrays.asList(affix, fix0);
                return a.iterator();
            }
        });

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> fix1.get(Settings.builder().put("abc.1.def", 11).put("abc.2.def", 12).put("abc.qrx", 11).build())
        );
        assertThat(e.getMessage(), is("abc.1.def:11,abc.2.def:12"));

        e = expectThrows(
            IllegalArgumentException.class,
            () -> fix1.get(Settings.builder().put("abc.3.def", 13).put("abc.qrx", 20).build())
        );
        assertThat(e.getMessage(), is("abc.3.def:13"));

        e = expectThrows(
            IllegalArgumentException.class,
            () -> fix1.get(Settings.builder().put("abc.4.def", 14).put("abc.qrx", 20).put("abc.tuv", 50).build())
        );
        assertThat(e.getMessage(), is("abc.4.def:14,abc.tuv:50"));

        assertEquals(
            fix1.get(Settings.builder().put("abc.3.def", 13).put("abc.1.def", 11).put("abc.2.def", 12).put("abc.qrx", 20).build()),
            Integer.valueOf(20)
        );

        assertEquals(fix1.get(Settings.builder().put("abc.qrx", 30).build()), Integer.valueOf(30));
    }

    // Integer

    public void testIntWithDefaultValue() {
        Setting<Integer> integerSetting = Setting.intSetting("foo.bar", 42);
        assertEquals(integerSetting.get(Settings.EMPTY), Integer.valueOf(42));
    }

    public void testIntWithFallbackValue() {
        Setting<Integer> fallbackSetting = Setting.intSetting("foo.baz", 2);
        Setting<Integer> integerSetting = Setting.intSetting("foo.bar", fallbackSetting);
        assertEquals(integerSetting.get(Settings.EMPTY), Integer.valueOf(2));
        assertEquals(integerSetting.get(Settings.builder().put("foo.bar", 3).build()), Integer.valueOf(3));
        assertEquals(integerSetting.get(Settings.builder().put("foo.baz", 3).build()), Integer.valueOf(3));
    }

    public void testIntWithMinMax() {
        Setting<Integer> integerSetting = Setting.intSetting("foo.bar", 1, 0, 10, Property.NodeScope);
        try {
            integerSetting.get(Settings.builder().put("foo.bar", 11).build());
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("Failed to parse value [11] for setting [foo.bar] must be <= 10", ex.getMessage());
        }

        try {
            integerSetting.get(Settings.builder().put("foo.bar", -1).build());
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("Failed to parse value [-1] for setting [foo.bar] must be >= 0", ex.getMessage());
        }

        assertEquals(5, integerSetting.get(Settings.builder().put("foo.bar", 5).build()).intValue());
        assertEquals(1, integerSetting.get(Settings.EMPTY).intValue());
    }

    public void testIntegerParser() throws Exception {
        String expectedKey = "test key";
        int expectedMinValue = Integer.MIN_VALUE;
        int expectedMaxValue = Integer.MAX_VALUE;
        boolean expectedFilteredStatus = true;
        IntegerParser integerParser = new IntegerParser(expectedMinValue, expectedMaxValue, expectedKey, expectedFilteredStatus);

        assertEquals(expectedKey, integerParser.getKey());
        assertEquals(expectedMinValue, integerParser.getMin());
        assertEquals(expectedMaxValue, integerParser.getMax());
        assertEquals(expectedFilteredStatus, integerParser.getFilterStatus());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            integerParser.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                integerParser = new IntegerParser(in);
                assertEquals(expectedKey, integerParser.getKey());
                assertEquals(expectedMinValue, integerParser.getMin());
                assertEquals(expectedMaxValue, integerParser.getMax());
                assertEquals(expectedFilteredStatus, integerParser.getFilterStatus());
            }
        }
    }

    // Long

    public void testLongWithDefaultValue() {
        Setting<Long> longSetting = Setting.longSetting("foo.bar", 42);
        assertEquals(longSetting.get(Settings.EMPTY), Long.valueOf(42));
    }

    public void testLongWithFallbackValue() {
        Setting<Long> fallbackSetting = Setting.longSetting("foo.baz", 2);
        Setting<Long> longSetting = Setting.longSetting("foo.bar", fallbackSetting);
        assertEquals(longSetting.get(Settings.EMPTY), Long.valueOf(2));
        assertEquals(longSetting.get(Settings.builder().put("foo.bar", 3).build()), Long.valueOf(3));
        assertEquals(longSetting.get(Settings.builder().put("foo.baz", 3).build()), Long.valueOf(3));
    }

    public void testLongWithMinMax() {
        Setting<Long> longSetting = Setting.longSetting("foo.bar", 1, 0, 10, Property.NodeScope);
        try {
            longSetting.get(Settings.builder().put("foo.bar", 11).build());
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("Failed to parse value [11] for setting [foo.bar] must be <= 10", ex.getMessage());
        }

        try {
            longSetting.get(Settings.builder().put("foo.bar", -1).build());
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("Failed to parse value [-1] for setting [foo.bar] must be >= 0", ex.getMessage());
        }

        assertEquals(5, longSetting.get(Settings.builder().put("foo.bar", 5).build()).longValue());
        assertEquals(1, longSetting.get(Settings.EMPTY).longValue());
    }

    public void testLongParser() throws Exception {
        String expectedKey = "test key";
        long expectedMinValue = Long.MIN_VALUE;
        long expectedMaxValue = Long.MAX_VALUE;
        boolean expectedFilteredStatus = true;
        LongParser longParser = new LongParser(expectedMinValue, expectedMaxValue, expectedKey, expectedFilteredStatus);

        assertEquals(expectedKey, longParser.getKey());
        assertEquals(expectedMinValue, longParser.getMin());
        assertEquals(expectedMaxValue, longParser.getMax());
        assertEquals(expectedFilteredStatus, longParser.getFilterStatus());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            longParser.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                longParser = new LongParser(in);
                assertEquals(expectedKey, longParser.getKey());
                assertEquals(expectedMinValue, longParser.getMin());
                assertEquals(expectedMaxValue, longParser.getMax());
                assertEquals(expectedFilteredStatus, longParser.getFilterStatus());
            }
        }
    }

    // Float

    public void testFloatWithDefaultValue() {
        Setting<Float> floatSetting = Setting.floatSetting("foo.bar", (float) 42.1);
        assertEquals(floatSetting.get(Settings.EMPTY), Float.valueOf((float) 42.1));
    }

    public void testFloatWithFallbackValue() {
        Setting<Float> fallbackSetting = Setting.floatSetting("foo.baz", (float) 2.1);
        Setting<Float> floatSetting = Setting.floatSetting("foo.bar", fallbackSetting);
        assertEquals(floatSetting.get(Settings.EMPTY), Float.valueOf((float) 2.1));
        assertEquals(floatSetting.get(Settings.builder().put("foo.bar", 3.2).build()), Float.valueOf((float) 3.2));
        assertEquals(floatSetting.get(Settings.builder().put("foo.baz", 3.2).build()), Float.valueOf((float) 3.2));
    }

    public void testFloatWithMinMax() {
        Setting<Float> floatSetting = Setting.floatSetting("foo.bar", (float) 1.2, 0, 10, Property.NodeScope);
        try {
            floatSetting.get(Settings.builder().put("foo.bar", (float) 11.3).build());
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("Failed to parse value [11.3] for setting [foo.bar] must be <= 10.0", ex.getMessage());
        }

        try {
            floatSetting.get(Settings.builder().put("foo.bar", (float) -1.4).build());
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("Failed to parse value [-1.4] for setting [foo.bar] must be >= 0.0", ex.getMessage());
        }

        assertEquals(5.6, floatSetting.get(Settings.builder().put("foo.bar", (float) 5.6).build()).floatValue(), 0.01);
        assertEquals(1.2, floatSetting.get(Settings.EMPTY).floatValue(), 0.01);
    }

    public void testFloatParser() throws Exception {
        String expectedKey = "test key";
        float expectedMinValue = Float.MIN_VALUE;
        float expectedMaxValue = Float.MAX_VALUE;
        boolean expectedFilteredStatus = true;
        FloatParser floatParser = new FloatParser(expectedMinValue, expectedMaxValue, expectedKey, expectedFilteredStatus);

        assertEquals(expectedKey, floatParser.getKey());
        assertEquals(expectedMinValue, floatParser.getMin(), 0.01);
        assertEquals(expectedMaxValue, floatParser.getMax(), 0.01);
        assertEquals(expectedFilteredStatus, floatParser.getFilterStatus());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            floatParser.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                floatParser = new FloatParser(in);
                assertEquals(expectedKey, floatParser.getKey());
                assertEquals(expectedMinValue, floatParser.getMin(), 0.01);
                assertEquals(expectedMaxValue, floatParser.getMax(), 0.01);
                assertEquals(expectedFilteredStatus, floatParser.getFilterStatus());
            }
        }
    }

    // Double

    public void testDoubleWithDefaultValue() {
        Setting<Double> doubleSetting = Setting.doubleSetting("foo.bar", 42.1);
        assertEquals(doubleSetting.get(Settings.EMPTY), Double.valueOf(42.1));

        Setting<Double> doubleSettingWithValidator = Setting.doubleSetting("foo.bar", 42.1, value -> {
            if (value <= 0.0) {
                throw new IllegalArgumentException("The setting foo.bar must be >0");
            }
        });
        try {
            assertThrows(
                IllegalArgumentException.class,
                () -> doubleSettingWithValidator.get(Settings.builder().put("foo.bar", randomFrom(-1, 0)).build())
            );
        } catch (IllegalArgumentException ex) {
            assertEquals("The setting foo.bar must be >0", ex.getMessage());
        }
    }

    public void testDoubleWithFallbackValue() {
        Setting<Double> fallbackSetting = Setting.doubleSetting("foo.baz", 2.1);
        Setting<Double> doubleSetting = Setting.doubleSetting("foo.bar", fallbackSetting);
        assertEquals(doubleSetting.get(Settings.EMPTY), Double.valueOf(2.1));
        assertEquals(doubleSetting.get(Settings.builder().put("foo.bar", 3.2).build()), Double.valueOf(3.2));
        assertEquals(doubleSetting.get(Settings.builder().put("foo.baz", 3.2).build()), Double.valueOf(3.2));

        Setting<Double> doubleSettingWithValidator = Setting.doubleSetting("foo.bar", fallbackSetting, value -> {
            if (value <= 0.0) {
                throw new IllegalArgumentException("The setting foo.bar must be >0");
            }
        });
        try {
            assertThrows(
                IllegalArgumentException.class,
                () -> doubleSettingWithValidator.get(Settings.builder().put("foo.bar", randomFrom(-1, 0)).build())
            );
        } catch (IllegalArgumentException ex) {
            assertEquals("The setting foo.bar must be >0", ex.getMessage());
        }
    }

    public void testDoubleWithMinMax() throws Exception {
        Setting<Double> doubleSetting = Setting.doubleSetting("foo.bar", 1.2, 0, 10, Property.NodeScope);
        try {
            doubleSetting.get(Settings.builder().put("foo.bar", 11.3).build());
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("Failed to parse value [11.3] for setting [foo.bar] must be <= 10.0", ex.getMessage());
        }

        try {
            doubleSetting.get(Settings.builder().put("foo.bar", -1.4).build());
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("Failed to parse value [-1.4] for setting [foo.bar] must be >= 0.0", ex.getMessage());
        }

        assertEquals(5.6, doubleSetting.get(Settings.builder().put("foo.bar", 5.6).build()).doubleValue(), 0.01);
        assertEquals(1.2, doubleSetting.get(Settings.EMPTY).doubleValue(), 0.01);
    }

    public void testDoubleParser() throws Exception {
        String expectedKey = "test key";
        double expectedMinValue = Double.MIN_VALUE;
        double expectedMaxValue = Double.MAX_VALUE;
        boolean expectedFilteredStatus = true;
        DoubleParser doubleParser = new DoubleParser(expectedMinValue, expectedMaxValue, expectedKey, expectedFilteredStatus);

        assertEquals(expectedKey, doubleParser.getKey());
        assertEquals(expectedMinValue, doubleParser.getMin(), 0.01);
        assertEquals(expectedMaxValue, doubleParser.getMax(), 0.01);
        assertEquals(expectedFilteredStatus, doubleParser.getFilterStatus());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            doubleParser.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                doubleParser = new DoubleParser(in);
                assertEquals(expectedKey, doubleParser.getKey());
                assertEquals(expectedMinValue, doubleParser.getMin(), 0.01);
                assertEquals(expectedMaxValue, doubleParser.getMax(), 0.01);
                assertEquals(expectedFilteredStatus, doubleParser.getFilterStatus());
            }
        }
    }

    // ByteSizeValue
    public void testByteSizeValueParser() throws Exception {
        String expectedKey = "test key";
        ByteSizeValue expectedMinValue = new ByteSizeValue((long) 1);
        ByteSizeValue expectedMaxValue = new ByteSizeValue(Long.MAX_VALUE);
        ByteSizeValueParser byteSizeValueParser = new ByteSizeValueParser(expectedMinValue, expectedMaxValue, expectedKey);

        assertEquals(expectedKey, byteSizeValueParser.getKey());
        assertEquals(expectedMinValue, byteSizeValueParser.getMin());
        assertEquals(expectedMaxValue, byteSizeValueParser.getMax());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            byteSizeValueParser.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                byteSizeValueParser = new ByteSizeValueParser(in);
                assertEquals(expectedKey, byteSizeValueParser.getKey());
                assertEquals(expectedMinValue, byteSizeValueParser.getMin());
                assertEquals(expectedMaxValue, byteSizeValueParser.getMax());
            }
        }
    }

    // MemorySizeValue
    public void testMemorySizeValueParser() throws Exception {
        String expectedKey = "test key";
        MemorySizeValueParser memorySizeValueParser = new MemorySizeValueParser(expectedKey);

        assertEquals(expectedKey, memorySizeValueParser.getKey());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            memorySizeValueParser.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                memorySizeValueParser = new MemorySizeValueParser(in);
                assertEquals(expectedKey, memorySizeValueParser.getKey());
            }
        }
    }

    /**
     * Only one single scope can be added to any setting
     */
    public void testMutuallyExclusiveScopes() {
        // Those should pass
        Setting<String> setting = Setting.simpleString("foo.bar", Property.NodeScope);
        assertThat(setting.hasNodeScope(), is(true));
        assertThat(setting.hasIndexScope(), is(false));
        setting = Setting.simpleString("foo.bar", Property.IndexScope);
        assertThat(setting.hasIndexScope(), is(true));
        assertThat(setting.hasNodeScope(), is(false));

        // We accept settings with no scope but they will be rejected when we register with SettingsModule.registerSetting
        setting = Setting.simpleString("foo.bar");
        assertThat(setting.hasIndexScope(), is(false));
        assertThat(setting.hasNodeScope(), is(false));

        // We accept settings with multiple scopes but they will be rejected when we register with SettingsModule.registerSetting
        setting = Setting.simpleString("foo.bar", Property.IndexScope, Property.NodeScope);
        assertThat(setting.hasIndexScope(), is(true));
        assertThat(setting.hasNodeScope(), is(true));
    }

    /**
     * We can't have Null properties
     */
    public void testRejectNullProperties() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> Setting.simpleString("foo.bar", (Property[]) null)
        );
        assertThat(ex.getMessage(), containsString("properties cannot be null for setting"));
    }

    public void testRejectConflictingDynamicAndFinalProperties() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> Setting.simpleString("foo.bar", Property.Final, Property.Dynamic)
        );
        assertThat(ex.getMessage(), containsString("final setting [foo.bar] cannot be dynamic"));
    }

    public void testRejectNonIndexScopedNotCopyableOnResizeSetting() {
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> Setting.simpleString("foo.bar", Property.NotCopyableOnResize)
        );
        assertThat(e, hasToString(containsString("non-index-scoped setting [foo.bar] can not have property [NotCopyableOnResize]")));
    }

    public void testRejectNonIndexScopedInternalIndexSetting() {
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> Setting.simpleString("foo.bar", Property.InternalIndex)
        );
        assertThat(e, hasToString(containsString("non-index-scoped setting [foo.bar] can not have property [InternalIndex]")));
    }

    public void testRejectNonIndexScopedPrivateIndexSetting() {
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> Setting.simpleString("foo.bar", Property.PrivateIndex)
        );
        assertThat(e, hasToString(containsString("non-index-scoped setting [foo.bar] can not have property [PrivateIndex]")));
    }

    public void testTimeValue() {
        final TimeValue random = TimeValue.parseTimeValue(randomTimeValue(), "test");

        Setting<TimeValue> setting = Setting.timeSetting("foo", random);
        assertThat(setting.get(Settings.EMPTY), equalTo(random));

        final int factor = randomIntBetween(1, 10);
        setting = Setting.timeSetting("foo", (s) -> TimeValue.timeValueMillis(random.getMillis() * factor), TimeValue.ZERO);
        assertThat(setting.get(Settings.builder().put("foo", "12h").build()), equalTo(TimeValue.timeValueHours(12)));
        assertThat(setting.get(Settings.EMPTY).getMillis(), equalTo(random.getMillis() * factor));
    }

    public void testMinTimeValueParser() throws Exception {
        String expectedKey = "test key";
        TimeValue expectedMinValue = TimeValue.timeValueSeconds(0);
        boolean expectedFilteredStatus = true;
        MinTimeValueParser minTimeValueParser = new MinTimeValueParser(expectedKey, expectedMinValue, expectedFilteredStatus);

        assertEquals(expectedKey, minTimeValueParser.getKey());
        assertEquals(expectedMinValue, minTimeValueParser.getMin());
        assertEquals(expectedFilteredStatus, minTimeValueParser.getFilterStatus());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            minTimeValueParser.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                minTimeValueParser = new MinTimeValueParser(in);
                assertEquals(expectedKey, minTimeValueParser.getKey());
                assertEquals(expectedMinValue, minTimeValueParser.getMin());
                assertEquals(expectedFilteredStatus, minTimeValueParser.getFilterStatus());
            }
        }
    }

    public void testMinMaxTimeValueParser() throws Exception {
        String expectedKey = "test key";
        TimeValue expectedMinValue = TimeValue.timeValueSeconds(0);
        TimeValue expectedMaxValue = TimeValue.MAX_VALUE;
        boolean expectedFilteredStatus = true;
        MinMaxTimeValueParser minMaxTimeValueParser = new MinMaxTimeValueParser(
            expectedKey,
            expectedMinValue,
            expectedMaxValue,
            expectedFilteredStatus
        );

        assertEquals(expectedKey, minMaxTimeValueParser.getKey());
        assertEquals(expectedMinValue, minMaxTimeValueParser.getMin());
        assertEquals(expectedMaxValue, minMaxTimeValueParser.getMax());
        assertEquals(expectedFilteredStatus, minMaxTimeValueParser.getFilterStatus());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            minMaxTimeValueParser.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                minMaxTimeValueParser = new MinMaxTimeValueParser(in);
                assertEquals(expectedKey, minMaxTimeValueParser.getKey());
                assertEquals(expectedMinValue, minMaxTimeValueParser.getMin());
                assertEquals(expectedMaxValue, minMaxTimeValueParser.getMax());
                assertEquals(expectedFilteredStatus, minMaxTimeValueParser.getFilterStatus());
            }
        }
    }

    public void testTimeValueBounds() {
        Setting<TimeValue> settingWithLowerBound = Setting.timeSetting(
            "foo",
            TimeValue.timeValueSeconds(10),
            TimeValue.timeValueSeconds(5)
        );
        assertThat(settingWithLowerBound.get(Settings.EMPTY), equalTo(TimeValue.timeValueSeconds(10)));

        assertThat(settingWithLowerBound.get(Settings.builder().put("foo", "5000ms").build()), equalTo(TimeValue.timeValueSeconds(5)));
        IllegalArgumentException illegalArgumentException = expectThrows(
            IllegalArgumentException.class,
            () -> settingWithLowerBound.get(Settings.builder().put("foo", "4999ms").build())
        );

        assertThat(illegalArgumentException.getMessage(), equalTo("failed to parse value [4999ms] for setting [foo], must be >= [5s]"));

        Setting<TimeValue> settingWithBothBounds = Setting.timeSetting(
            "bar",
            TimeValue.timeValueSeconds(10),
            TimeValue.timeValueSeconds(5),
            TimeValue.timeValueSeconds(20)
        );
        assertThat(settingWithBothBounds.get(Settings.EMPTY), equalTo(TimeValue.timeValueSeconds(10)));

        assertThat(settingWithBothBounds.get(Settings.builder().put("bar", "5000ms").build()), equalTo(TimeValue.timeValueSeconds(5)));
        assertThat(settingWithBothBounds.get(Settings.builder().put("bar", "20000ms").build()), equalTo(TimeValue.timeValueSeconds(20)));
        illegalArgumentException = expectThrows(
            IllegalArgumentException.class,
            () -> settingWithBothBounds.get(Settings.builder().put("bar", "4999ms").build())
        );
        assertThat(illegalArgumentException.getMessage(), equalTo("failed to parse value [4999ms] for setting [bar], must be >= [5s]"));

        illegalArgumentException = expectThrows(
            IllegalArgumentException.class,
            () -> settingWithBothBounds.get(Settings.builder().put("bar", "20001ms").build())
        );
        assertThat(illegalArgumentException.getMessage(), equalTo("failed to parse value [20001ms] for setting [bar], must be <= [20s]"));
    }

    public void testSettingsGroupUpdater() {
        Setting<Integer> intSetting = Setting.intSetting("prefix.foo", 1, Property.NodeScope, Property.Dynamic);
        Setting<Integer> intSetting2 = Setting.intSetting("prefix.same", 1, Property.NodeScope, Property.Dynamic);
        AbstractScopedSettings.SettingUpdater<Settings> updater = Setting.groupedSettingsUpdater(
            s -> {},
            Arrays.asList(intSetting, intSetting2)
        );

        Settings current = Settings.builder().put("prefix.foo", 123).put("prefix.same", 5555).build();
        Settings previous = Settings.builder().put("prefix.foo", 321).put("prefix.same", 5555).build();
        assertTrue(updater.apply(current, previous));
    }

    public void testSettingsGroupUpdaterRemoval() {
        Setting<Integer> intSetting = Setting.intSetting("prefix.foo", 1, Property.NodeScope, Property.Dynamic);
        Setting<Integer> intSetting2 = Setting.intSetting("prefix.same", 1, Property.NodeScope, Property.Dynamic);
        AbstractScopedSettings.SettingUpdater<Settings> updater = Setting.groupedSettingsUpdater(
            s -> {},
            Arrays.asList(intSetting, intSetting2)
        );

        Settings current = Settings.builder().put("prefix.same", 5555).build();
        Settings previous = Settings.builder().put("prefix.foo", 321).put("prefix.same", 5555).build();
        assertTrue(updater.apply(current, previous));
    }

    public void testSettingsGroupUpdaterWithAffixSetting() {
        Setting<Integer> intSetting = Setting.intSetting("prefix.foo", 1, Property.NodeScope, Property.Dynamic);
        Setting.AffixSetting<String> prefixKeySetting = Setting.prefixKeySetting(
            "prefix.foo.bar.",
            key -> Setting.simpleString(key, Property.NodeScope, Property.Dynamic)
        );
        Setting.AffixSetting<String> affixSetting = Setting.affixKeySetting(
            "prefix.foo.",
            "suffix",
            key -> Setting.simpleString(key, Property.NodeScope, Property.Dynamic)
        );

        AbstractScopedSettings.SettingUpdater<Settings> updater = Setting.groupedSettingsUpdater(
            s -> {},
            Arrays.asList(intSetting, prefixKeySetting, affixSetting)
        );

        Settings.Builder currentSettingsBuilder = Settings.builder().put("prefix.foo.bar.baz", "foo").put("prefix.foo.infix.suffix", "foo");
        Settings.Builder previousSettingsBuilder = Settings.builder()
            .put("prefix.foo.bar.baz", "foo")
            .put("prefix.foo.infix.suffix", "foo");
        boolean removePrefixKeySetting = randomBoolean();
        boolean changePrefixKeySetting = randomBoolean();
        boolean removeAffixKeySetting = randomBoolean();
        boolean changeAffixKeySetting = randomBoolean();
        boolean removeAffixNamespace = randomBoolean();

        if (removePrefixKeySetting) {
            previousSettingsBuilder.remove("prefix.foo.bar.baz");
        }
        if (changePrefixKeySetting) {
            currentSettingsBuilder.put("prefix.foo.bar.baz", "bar");
        }
        if (removeAffixKeySetting) {
            previousSettingsBuilder.remove("prefix.foo.infix.suffix");
        }
        if (changeAffixKeySetting) {
            currentSettingsBuilder.put("prefix.foo.infix.suffix", "bar");
        }
        if (removeAffixKeySetting == false && changeAffixKeySetting == false && removeAffixNamespace) {
            currentSettingsBuilder.remove("prefix.foo.infix.suffix");
            currentSettingsBuilder.put("prefix.foo.infix2.suffix", "bar");
            previousSettingsBuilder.put("prefix.foo.infix2.suffix", "bar");
        }

        boolean expectedChange = removeAffixKeySetting
            || removePrefixKeySetting
            || changeAffixKeySetting
            || changePrefixKeySetting
            || removeAffixNamespace;
        assertThat(updater.apply(currentSettingsBuilder.build(), previousSettingsBuilder.build()), is(expectedChange));
    }

    public void testAffixNamespacesWithGroupSetting() {
        final Setting.AffixSetting<Settings> affixSetting = Setting.affixKeySetting(
            "prefix.",
            "suffix",
            (key) -> Setting.groupSetting(key + ".", Setting.Property.Dynamic, Setting.Property.NodeScope)
        );

        assertThat(affixSetting.getNamespaces(Settings.builder().put("prefix.infix.suffix", "anything").build()), hasSize(1));
        assertThat(affixSetting.getNamespaces(Settings.builder().put("prefix.infix.suffix.anything", "anything").build()), hasSize(1));
    }

    public void testGroupSettingUpdaterValidator() {
        final Setting.AffixSetting<Integer> affixSetting = Setting.affixKeySetting(
            "prefix.",
            "suffix",
            (key) -> Setting.intSetting(key, 5, Setting.Property.Dynamic, Setting.Property.NodeScope)
        );
        Setting<Integer> fixSetting = Setting.intSetting("abc", 1, Property.NodeScope);

        Consumer<Settings> validator = s -> {
            if (affixSetting.getNamespaces(s).contains("foo")) {
                if (fixSetting.get(s) == 2) {
                    throw new IllegalArgumentException("foo and 2 can't go together");
                }
            } else if (affixSetting.getNamespaces(s).contains("bar")) {
                throw new IllegalArgumentException("no bar");
            }
        };

        AbstractScopedSettings.SettingUpdater<Settings> updater = Setting.groupedSettingsUpdater(
            s -> {},
            Arrays.asList(affixSetting, fixSetting),
            validator
        );

        IllegalArgumentException illegal = expectThrows(IllegalArgumentException.class, () -> {
            updater.getValue(Settings.builder().put("prefix.foo.suffix", 5).put("abc", 2).build(), Settings.EMPTY);
        });
        assertEquals("foo and 2 can't go together", illegal.getMessage());

        illegal = expectThrows(IllegalArgumentException.class, () -> {
            updater.getValue(Settings.builder().put("prefix.bar.suffix", 6).put("abc", 3).build(), Settings.EMPTY);
        });
        assertEquals("no bar", illegal.getMessage());

        Settings s = updater.getValue(
            Settings.builder().put("prefix.foo.suffix", 5).put("prefix.bar.suffix", 5).put("abc", 3).build(),
            Settings.EMPTY
        );
        assertNotNull(s);
    }

    public void testExists() {
        final Setting<?> fooSetting = Setting.simpleString("foo", Property.NodeScope);
        assertFalse(fooSetting.exists(Settings.EMPTY));
        assertTrue(fooSetting.exists(Settings.builder().put("foo", "bar").build()));
    }

    public void testExistsWithFallback() {
        final int count = randomIntBetween(1, 16);
        Setting<String> current = Setting.simpleString("fallback0", Property.NodeScope);
        for (int i = 1; i < count; i++) {
            final Setting<String> next = new Setting<>(
                new Setting.SimpleKey("fallback" + i),
                current,
                Function.identity(),
                Property.NodeScope
            );
            current = next;
        }
        final Setting<String> fooSetting = new Setting<>(new Setting.SimpleKey("foo"), current, Function.identity(), Property.NodeScope);
        assertFalse(fooSetting.exists(Settings.EMPTY));
        if (randomBoolean()) {
            assertTrue(fooSetting.exists(Settings.builder().put("foo", "bar").build()));
        } else {
            final String setting = "fallback" + randomIntBetween(0, count - 1);
            assertFalse(fooSetting.exists(Settings.builder().put(setting, "bar").build()));
            assertTrue(fooSetting.existsOrFallbackExists(Settings.builder().put(setting, "bar").build()));
        }
    }

    public void testAffixMapUpdateWithNullSettingValue() {
        // GIVEN an affix setting changed from "prefix._foo"="bar" to "prefix._foo"=null
        final Settings current = Settings.builder().put("prefix._foo", (String) null).build();

        final Settings previous = Settings.builder().put("prefix._foo", "bar").build();

        final Setting.AffixSetting<String> affixSetting = Setting.prefixKeySetting(
            "prefix" + ".",
            key -> Setting.simpleString(key, Property.Dynamic, Property.NodeScope)
        );

        final Consumer<Map<String, String>> consumer = (map) -> {};
        final BiConsumer<String, String> validator = (s1, s2) -> {};

        // WHEN creating an affix updater
        final SettingUpdater<Map<String, String>> updater = affixSetting.newAffixMapUpdater(consumer, logger, validator);

        // THEN affix updater is always expected to have changed (even when defaults are omitted)
        assertTrue(updater.hasChanged(current, previous));

        // THEN changes are expected when defaults aren't omitted
        final Map<String, String> updatedSettings = updater.getValue(current, previous);
        assertNotNull(updatedSettings);
        assertEquals(1, updatedSettings.size());

        // THEN changes are reported when defaults aren't omitted
        final String key = updatedSettings.keySet().iterator().next();
        final String value = updatedSettings.get(key);
        assertEquals("_foo", key);
        assertEquals("", value);
    }

    public void testNonSecureSettingInKeystore() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("foo", "bar");
        final Settings settings = Settings.builder().setSecureSettings(secureSettings).build();
        Setting<String> setting = Setting.simpleString("foo", Property.NodeScope);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> setting.get(settings));
        assertThat(e.getMessage(), containsString("must be stored inside opensearch.yml"));
    }

    @TestLogging(value = "org.opensearch.common.settings.IndexScopedSettings:INFO", reason = "to ensure we log INFO-level messages from IndexScopedSettings")
    public void testLogSettingUpdate() throws Exception {
        final IndexMetadata metadata = newIndexMeta(
            "index1",
            Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "20s").build()
        );
        final IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);

        final Logger logger = LogManager.getLogger(IndexScopedSettings.class);
        try (MockLogAppender mockLogAppender = MockLogAppender.createForLoggers(logger)) {
            mockLogAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "message",
                    "org.opensearch.common.settings.IndexScopedSettings",
                    Level.INFO,
                    "updating [index.refresh_interval] from [20s] to [10s]"
                ) {
                    @Override
                    public boolean innerMatch(LogEvent event) {
                        return event.getMarker().getName().equals(" [index1]");
                    }
                }
            );
            settings.updateIndexMetadata(
                newIndexMeta("index1", Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "10s").build())
            );

            mockLogAppender.assertAllExpectationsMatched();
        }
    }
}
