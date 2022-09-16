/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.settings;

import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamInput;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.unit.ByteSizeUnit;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.opensearch.common.settings.Setting.Property;
import static org.opensearch.common.settings.WriteableSetting.WriteableSettingGenericType;

public class WriteableSettingTests extends OpenSearchTestCase {

    // These settings have a default value and null fallback
    private final Map<WriteableSettingGenericType, Setting<?>> settingMap = new EnumMap<>(WriteableSettingGenericType.class);
    // These settings have a fallback setting instead of a default
    private final Map<WriteableSettingGenericType, Setting<?>> settingWithFallbackMap = new EnumMap<>(WriteableSettingGenericType.class);

    @SuppressWarnings("unchecked")
    @Before
    public void setup() throws Exception {
        super.setUp();
        settingMap.put(
            WriteableSettingGenericType.Boolean,
            Setting.boolSetting("boolSettingBase", false, Property.NodeScope, Property.Dynamic)
        );
        settingMap.put(WriteableSettingGenericType.Integer, Setting.intSetting("intSettingBase", 6, Property.NodeScope, Property.Dynamic));
        settingMap.put(WriteableSettingGenericType.Long, Setting.longSetting("longSettingBase", 42L, Property.NodeScope, Property.Dynamic));
        settingMap.put(
            WriteableSettingGenericType.Float,
            Setting.floatSetting("floatSettingBase", 6.2f, Property.NodeScope, Property.Dynamic)
        );
        settingMap.put(
            WriteableSettingGenericType.Double,
            Setting.doubleSetting("doubleSettingBase", 42.2d, Property.NodeScope, Property.Dynamic)
        );
        settingMap.put(
            WriteableSettingGenericType.String,
            Setting.simpleString("simpleStringBase", "foo", Property.NodeScope, Property.Dynamic)
        );
        settingMap.put(
            WriteableSettingGenericType.TimeValue,
            Setting.timeSetting("timeSettingBase", new TimeValue(5, TimeUnit.MILLISECONDS), Property.NodeScope, Property.Dynamic)
        );
        settingMap.put(
            WriteableSettingGenericType.ByteSizeValue,
            Setting.byteSizeSetting("byteSizeSettingBase", new ByteSizeValue(10, ByteSizeUnit.KB), Property.NodeScope, Property.Dynamic)
        );
        settingMap.put(
            WriteableSettingGenericType.Version,
            Setting.versionSetting("versionSettingBase", Version.CURRENT, Property.NodeScope, Property.Dynamic)
        );

        settingWithFallbackMap.put(
            WriteableSettingGenericType.Boolean,
            Setting.boolSetting(
                "boolSetting",
                (Setting<Boolean>) settingMap.get(WriteableSettingGenericType.Boolean),
                Property.NodeScope,
                Property.Dynamic
            )
        );
        settingWithFallbackMap.put(
            WriteableSettingGenericType.Integer,
            Setting.intSetting(
                "intSetting",
                (Setting<Integer>) settingMap.get(WriteableSettingGenericType.Integer),
                Property.NodeScope,
                Property.Dynamic
            )
        );
        settingWithFallbackMap.put(
            WriteableSettingGenericType.Long,
            Setting.longSetting(
                "longSetting",
                (Setting<Long>) settingMap.get(WriteableSettingGenericType.Long),
                Property.NodeScope,
                Property.Dynamic
            )
        );
        settingWithFallbackMap.put(
            WriteableSettingGenericType.Float,
            Setting.floatSetting(
                "floatSetting",
                (Setting<Float>) settingMap.get(WriteableSettingGenericType.Float),
                Property.NodeScope,
                Property.Dynamic
            )
        );
        settingWithFallbackMap.put(
            WriteableSettingGenericType.Double,
            Setting.doubleSetting(
                "doubleSetting",
                (Setting<Double>) settingMap.get(WriteableSettingGenericType.Double),
                Property.NodeScope,
                Property.Dynamic
            )
        );
        settingWithFallbackMap.put(
            WriteableSettingGenericType.String,
            Setting.simpleString(
                "simpleString",
                (Setting<String>) settingMap.get(WriteableSettingGenericType.String),
                Property.NodeScope,
                Property.Dynamic
            )
        );
        settingWithFallbackMap.put(
            WriteableSettingGenericType.TimeValue,
            Setting.timeSetting(
                "timeSetting",
                (Setting<TimeValue>) settingMap.get(WriteableSettingGenericType.TimeValue),
                Property.NodeScope,
                Property.Dynamic
            )
        );
        settingWithFallbackMap.put(
            WriteableSettingGenericType.ByteSizeValue,
            Setting.byteSizeSetting(
                "byteSizeSetting",
                (Setting<ByteSizeValue>) settingMap.get(WriteableSettingGenericType.ByteSizeValue),
                Property.NodeScope,
                Property.Dynamic
            )
        );
        // No fallback for versionSetting

    }

    @SuppressWarnings("unchecked")
    public void testBooleanSetting() throws IOException {
        WriteableSetting ws = new WriteableSetting(settingMap.get(WriteableSettingGenericType.Boolean));
        assertEquals(WriteableSettingGenericType.Boolean, ws.getType());
        Setting<Boolean> setting = (Setting<Boolean>) ws.getSetting();
        assertEquals("boolSettingBase", setting.getKey());
        assertFalse(setting.getDefault(Settings.EMPTY));
        EnumSet<Property> props = setting.getProperties();
        assertEquals(2, props.size());
        assertTrue(props.contains(Property.NodeScope));
        assertTrue(props.contains(Property.Dynamic));

        WriteableSetting wsfb = new WriteableSetting(settingWithFallbackMap.get(WriteableSettingGenericType.Boolean));
        assertEquals(WriteableSettingGenericType.Boolean, wsfb.getType());
        setting = (Setting<Boolean>) wsfb.getSetting();
        assertEquals("boolSetting", setting.getKey());
        assertFalse(setting.getDefault(Settings.EMPTY));
        props = setting.getProperties();
        assertEquals(2, props.size());
        assertTrue(props.contains(Property.NodeScope));
        assertTrue(props.contains(Property.Dynamic));

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            wsfb.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                WriteableSetting wsIn = new WriteableSetting(in);

                assertEquals(WriteableSettingGenericType.Boolean, wsIn.getType());
                setting = (Setting<Boolean>) wsIn.getSetting();
                assertEquals("boolSetting", setting.getKey());
                assertFalse(setting.getDefault(Settings.EMPTY));
                props = setting.getProperties();
                assertEquals(2, props.size());
                assertTrue(props.contains(Property.NodeScope));
                assertTrue(props.contains(Property.Dynamic));
            }
        }

    }

    @SuppressWarnings("unchecked")
    public void testIntegerSetting() throws IOException {
        WriteableSetting ws = new WriteableSetting(settingMap.get(WriteableSettingGenericType.Integer));
        assertEquals(WriteableSettingGenericType.Integer, ws.getType());
        Setting<Integer> setting = (Setting<Integer>) ws.getSetting();
        assertEquals("intSettingBase", setting.getKey());
        assertEquals(6, (int) setting.getDefault(Settings.EMPTY));
        EnumSet<Property> props = setting.getProperties();
        assertEquals(2, props.size());
        assertTrue(props.contains(Property.NodeScope));
        assertTrue(props.contains(Property.Dynamic));

        WriteableSetting wsfb = new WriteableSetting(settingWithFallbackMap.get(WriteableSettingGenericType.Integer));
        assertEquals(WriteableSettingGenericType.Integer, wsfb.getType());
        setting = (Setting<Integer>) wsfb.getSetting();
        assertEquals("intSetting", setting.getKey());
        assertEquals(6, (int) setting.getDefault(Settings.EMPTY));
        props = setting.getProperties();
        assertEquals(2, props.size());
        assertTrue(props.contains(Property.NodeScope));
        assertTrue(props.contains(Property.Dynamic));

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            wsfb.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                WriteableSetting wsIn = new WriteableSetting(in);

                assertEquals(WriteableSettingGenericType.Integer, wsIn.getType());
                setting = (Setting<Integer>) wsIn.getSetting();
                assertEquals("intSetting", setting.getKey());
                assertEquals(6, (int) setting.getDefault(Settings.EMPTY));
                props = setting.getProperties();
                assertEquals(2, props.size());
                assertTrue(props.contains(Property.NodeScope));
                assertTrue(props.contains(Property.Dynamic));
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void testLongSetting() throws IOException {
        WriteableSetting ws = new WriteableSetting(settingMap.get(WriteableSettingGenericType.Long));
        assertEquals(WriteableSettingGenericType.Long, ws.getType());
        Setting<Long> setting = (Setting<Long>) ws.getSetting();
        assertEquals("longSettingBase", setting.getKey());
        assertEquals(42L, (long) setting.getDefault(Settings.EMPTY));
        EnumSet<Property> props = setting.getProperties();
        assertEquals(2, props.size());
        assertTrue(props.contains(Property.NodeScope));
        assertTrue(props.contains(Property.Dynamic));

        WriteableSetting wsfb = new WriteableSetting(settingWithFallbackMap.get(WriteableSettingGenericType.Long));
        assertEquals(WriteableSettingGenericType.Long, wsfb.getType());
        setting = (Setting<Long>) wsfb.getSetting();
        assertEquals("longSetting", setting.getKey());
        assertEquals(42L, (long) setting.getDefault(Settings.EMPTY));
        props = setting.getProperties();
        assertEquals(2, props.size());
        assertTrue(props.contains(Property.NodeScope));
        assertTrue(props.contains(Property.Dynamic));

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            wsfb.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                WriteableSetting wsIn = new WriteableSetting(in);

                assertEquals(WriteableSettingGenericType.Long, wsIn.getType());
                setting = (Setting<Long>) wsIn.getSetting();
                assertEquals("longSetting", setting.getKey());
                assertEquals(42L, (long) setting.getDefault(Settings.EMPTY));
                props = setting.getProperties();
                assertEquals(2, props.size());
                assertTrue(props.contains(Property.NodeScope));
                assertTrue(props.contains(Property.Dynamic));
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void testFloatSetting() throws IOException {
        WriteableSetting ws = new WriteableSetting(settingMap.get(WriteableSettingGenericType.Float));
        assertEquals(WriteableSettingGenericType.Float, ws.getType());
        Setting<Float> setting = (Setting<Float>) ws.getSetting();
        assertEquals("floatSettingBase", setting.getKey());
        assertEquals(6.2f, (float) setting.getDefault(Settings.EMPTY), Float.MIN_NORMAL);
        EnumSet<Property> props = setting.getProperties();
        assertEquals(2, props.size());
        assertTrue(props.contains(Property.NodeScope));
        assertTrue(props.contains(Property.Dynamic));

        WriteableSetting wsfb = new WriteableSetting(settingWithFallbackMap.get(WriteableSettingGenericType.Float));
        assertEquals(WriteableSettingGenericType.Float, wsfb.getType());
        setting = (Setting<Float>) wsfb.getSetting();
        assertEquals("floatSetting", setting.getKey());
        assertEquals(6.2f, (float) setting.getDefault(Settings.EMPTY), Float.MIN_NORMAL);
        props = setting.getProperties();
        assertEquals(2, props.size());
        assertTrue(props.contains(Property.NodeScope));
        assertTrue(props.contains(Property.Dynamic));

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            wsfb.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                WriteableSetting wsIn = new WriteableSetting(in);

                assertEquals(WriteableSettingGenericType.Float, wsIn.getType());
                setting = (Setting<Float>) wsIn.getSetting();
                assertEquals("floatSetting", setting.getKey());
                assertEquals(6.2f, (Float) setting.getDefault(Settings.EMPTY), Float.MIN_NORMAL);
                props = setting.getProperties();
                assertEquals(2, props.size());
                assertTrue(props.contains(Property.NodeScope));
                assertTrue(props.contains(Property.Dynamic));
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void testDoubleSetting() throws IOException {
        WriteableSetting ws = new WriteableSetting(settingMap.get(WriteableSettingGenericType.Double));
        assertEquals(WriteableSettingGenericType.Double, ws.getType());
        Setting<Double> setting = (Setting<Double>) ws.getSetting();
        assertEquals("doubleSettingBase", setting.getKey());
        assertEquals(42.2d, (double) setting.getDefault(Settings.EMPTY), Double.MIN_NORMAL);
        EnumSet<Property> props = setting.getProperties();
        assertEquals(2, props.size());
        assertTrue(props.contains(Property.NodeScope));
        assertTrue(props.contains(Property.Dynamic));

        WriteableSetting wsfb = new WriteableSetting(settingWithFallbackMap.get(WriteableSettingGenericType.Double));
        assertEquals(WriteableSettingGenericType.Double, wsfb.getType());
        setting = (Setting<Double>) wsfb.getSetting();
        assertEquals("doubleSetting", setting.getKey());
        assertEquals(42.2d, (double) setting.getDefault(Settings.EMPTY), Double.MIN_NORMAL);
        props = setting.getProperties();
        assertEquals(2, props.size());
        assertTrue(props.contains(Property.NodeScope));
        assertTrue(props.contains(Property.Dynamic));

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            wsfb.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                WriteableSetting wsIn = new WriteableSetting(in);

                assertEquals(WriteableSettingGenericType.Double, wsIn.getType());
                setting = (Setting<Double>) wsIn.getSetting();
                assertEquals("doubleSetting", setting.getKey());
                assertEquals(42.2d, (double) setting.getDefault(Settings.EMPTY), Double.MIN_NORMAL);
                props = setting.getProperties();
                assertEquals(2, props.size());
                assertTrue(props.contains(Property.NodeScope));
                assertTrue(props.contains(Property.Dynamic));
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void testStringSetting() throws IOException {
        WriteableSetting ws = new WriteableSetting(settingMap.get(WriteableSettingGenericType.String));
        assertEquals(WriteableSettingGenericType.String, ws.getType());
        Setting<String> setting = (Setting<String>) ws.getSetting();
        assertEquals("simpleStringBase", setting.getKey());
        assertEquals("foo", (String) setting.getDefault(Settings.EMPTY));
        EnumSet<Property> props = setting.getProperties();
        assertEquals(2, props.size());
        assertTrue(props.contains(Property.NodeScope));
        assertTrue(props.contains(Property.Dynamic));

        WriteableSetting wsfb = new WriteableSetting(settingWithFallbackMap.get(WriteableSettingGenericType.String));
        assertEquals(WriteableSettingGenericType.String, wsfb.getType());
        setting = (Setting<String>) wsfb.getSetting();
        assertEquals("simpleString", setting.getKey());
        assertEquals("foo", (String) setting.getDefault(Settings.EMPTY));
        props = setting.getProperties();
        assertEquals(2, props.size());
        assertTrue(props.contains(Property.NodeScope));
        assertTrue(props.contains(Property.Dynamic));

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            wsfb.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                WriteableSetting wsIn = new WriteableSetting(in);

                assertEquals(WriteableSettingGenericType.String, wsIn.getType());
                setting = (Setting<String>) wsIn.getSetting();
                assertEquals("simpleString", setting.getKey());
                assertEquals("foo", (String) setting.getDefault(Settings.EMPTY));
                props = setting.getProperties();
                assertEquals(2, props.size());
                assertTrue(props.contains(Property.NodeScope));
                assertTrue(props.contains(Property.Dynamic));
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void testTimeValueSetting() throws IOException {
        WriteableSetting ws = new WriteableSetting(settingMap.get(WriteableSettingGenericType.TimeValue));
        assertEquals(WriteableSettingGenericType.TimeValue, ws.getType());
        Setting<TimeValue> setting = (Setting<TimeValue>) ws.getSetting();
        assertEquals("timeSettingBase", setting.getKey());
        assertEquals(new TimeValue(5, TimeUnit.MILLISECONDS), (TimeValue) setting.getDefault(Settings.EMPTY));
        EnumSet<Property> props = setting.getProperties();
        assertEquals(2, props.size());
        assertTrue(props.contains(Property.NodeScope));
        assertTrue(props.contains(Property.Dynamic));

        WriteableSetting wsfb = new WriteableSetting(settingWithFallbackMap.get(WriteableSettingGenericType.TimeValue));
        assertEquals(WriteableSettingGenericType.TimeValue, wsfb.getType());
        setting = (Setting<TimeValue>) wsfb.getSetting();
        assertEquals("timeSetting", setting.getKey());
        assertEquals(new TimeValue(5, TimeUnit.MILLISECONDS), (TimeValue) setting.getDefault(Settings.EMPTY));
        props = setting.getProperties();
        assertEquals(2, props.size());
        assertTrue(props.contains(Property.NodeScope));
        assertTrue(props.contains(Property.Dynamic));

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            wsfb.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                WriteableSetting wsIn = new WriteableSetting(in);

                assertEquals(WriteableSettingGenericType.TimeValue, wsIn.getType());
                setting = (Setting<TimeValue>) wsIn.getSetting();
                assertEquals("timeSetting", setting.getKey());
                assertEquals(new TimeValue(5, TimeUnit.MILLISECONDS), (TimeValue) setting.getDefault(Settings.EMPTY));
                props = setting.getProperties();
                assertEquals(2, props.size());
                assertTrue(props.contains(Property.NodeScope));
                assertTrue(props.contains(Property.Dynamic));
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void testByteSizeValueSetting() throws IOException {
        WriteableSetting ws = new WriteableSetting(settingMap.get(WriteableSettingGenericType.ByteSizeValue));
        assertEquals(WriteableSettingGenericType.ByteSizeValue, ws.getType());
        Setting<ByteSizeValue> setting = (Setting<ByteSizeValue>) ws.getSetting();
        assertEquals("byteSizeSettingBase", setting.getKey());
        assertEquals(new ByteSizeValue(10, ByteSizeUnit.KB), (ByteSizeValue) setting.getDefault(Settings.EMPTY));
        EnumSet<Property> props = setting.getProperties();
        assertEquals(2, props.size());
        assertTrue(props.contains(Property.NodeScope));
        assertTrue(props.contains(Property.Dynamic));

        WriteableSetting wsfb = new WriteableSetting(settingWithFallbackMap.get(WriteableSettingGenericType.ByteSizeValue));
        assertEquals(WriteableSettingGenericType.ByteSizeValue, wsfb.getType());
        setting = (Setting<ByteSizeValue>) wsfb.getSetting();
        assertEquals("byteSizeSetting", setting.getKey());
        assertEquals(new ByteSizeValue(10, ByteSizeUnit.KB), (ByteSizeValue) setting.getDefault(Settings.EMPTY));
        props = setting.getProperties();
        assertEquals(2, props.size());
        assertTrue(props.contains(Property.NodeScope));
        assertTrue(props.contains(Property.Dynamic));

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            wsfb.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                WriteableSetting wsIn = new WriteableSetting(in);

                assertEquals(WriteableSettingGenericType.ByteSizeValue, wsIn.getType());
                setting = (Setting<ByteSizeValue>) wsIn.getSetting();
                assertEquals("byteSizeSetting", setting.getKey());
                assertEquals(new ByteSizeValue(10, ByteSizeUnit.KB), (ByteSizeValue) setting.getDefault(Settings.EMPTY));
                props = setting.getProperties();
                assertEquals(2, props.size());
                assertTrue(props.contains(Property.NodeScope));
                assertTrue(props.contains(Property.Dynamic));
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void testVersionSetting() throws IOException {
        WriteableSetting ws = new WriteableSetting(settingMap.get(WriteableSettingGenericType.Version));
        assertEquals(WriteableSettingGenericType.Version, ws.getType());
        Setting<Version> setting = (Setting<Version>) ws.getSetting();
        assertEquals("versionSettingBase", setting.getKey());
        assertEquals(Version.CURRENT, (Version) setting.getDefault(Settings.EMPTY));
        EnumSet<Property> props = setting.getProperties();
        assertEquals(2, props.size());
        assertTrue(props.contains(Property.NodeScope));
        assertTrue(props.contains(Property.Dynamic));

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            ws.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                WriteableSetting wsIn = new WriteableSetting(in);

                assertEquals(WriteableSettingGenericType.Version, wsIn.getType());
                setting = (Setting<Version>) wsIn.getSetting();
                assertEquals("versionSettingBase", setting.getKey());
                assertEquals(Version.CURRENT, (Version) setting.getDefault(Settings.EMPTY));
                props = setting.getProperties();
                assertEquals(2, props.size());
                assertTrue(props.contains(Property.NodeScope));
                assertTrue(props.contains(Property.Dynamic));
            }
        }
    }

    @SuppressForbidden(reason = "The only way to test these is via reflection")
    public void testExceptionHandling() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
        // abuse reflection to change default value, no way to do this with given code
        Setting<String> setting = Setting.simpleString("");
        Field dv = setting.getClass().getDeclaredField("defaultValue");
        dv.setAccessible(true);
        Field p = setting.getClass().getDeclaredField("parser");
        p.setAccessible(true);

        // test null default value
        dv.set(setting, null);
        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> new WriteableSetting(setting));
        assertTrue(iae.getMessage().contains("null default value"));

        // test devault value type not in enum
        Function<Settings, String> dvfi = s -> "";
        dv.set(setting, dvfi);
        Function<String, WriteableSettingTests> pfi = s -> new WriteableSettingTests();
        p.set(setting, pfi);
        UnsupportedOperationException uoe = expectThrows(UnsupportedOperationException.class, () -> new WriteableSetting(setting));
        assertTrue(uoe.getMessage().contains("generic type: WriteableSettingTests"));
    }
}
