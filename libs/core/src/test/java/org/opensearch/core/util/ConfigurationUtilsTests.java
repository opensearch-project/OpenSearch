/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.core.util;

import org.opensearch.OpenSearchParseException;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class ConfigurationUtilsTests extends OpenSearchTestCase {
    private Map<String, Object> config;

    @Before
    public void setConfig() {
        config = new HashMap<>();
        config.put("foo", "bar");
        config.put("boolVal", true);
        config.put("null", null);
        config.put("arr", Arrays.asList("1", "2", "3"));
        config.put("ip", "127.0.0.1");
        config.put("num", 1);
        config.put("double", 1.0);
    }

    public void testReadStringProperty() {
        String val = ConfigurationUtils.readStringProperty(config, "foo");
        assertThat(val, equalTo("bar"));
        String val1 = ConfigurationUtils.readStringProperty(config, "foo1", "none");
        assertThat(val1, equalTo("none"));
        try {
            ConfigurationUtils.readStringProperty(config, "foo1", null);
        } catch (OpenSearchParseException e) {
            assertThat(e.getMessage(), equalTo("[foo1] required property is missing"));
        }
    }

    public void testOptionalReadStringProperty() {
        String val = ConfigurationUtils.readOptionalStringProperty(config, "foo");
        assertThat(val, equalTo("bar"));
        String val1 = ConfigurationUtils.readOptionalStringProperty(config, "foo1");
        assertThat(val, equalTo("bar"));
        assertThat(val1, equalTo(null));
    }

    public void testReadStringPropertyInvalidType() {
        try {
            ConfigurationUtils.readStringProperty(config, "arr");
        } catch (OpenSearchParseException e) {
            assertThat(e.getMessage(), equalTo("[arr] property isn't a string, but of type [java.util.Arrays$ArrayList]"));
        }
    }

    public void testReadBooleanProperty() {
        Boolean val = ConfigurationUtils.readBooleanProperty(config, "boolVal", false);
        assertThat(val, equalTo(true));
    }

    public void testReadNullBooleanProperty() {
        Boolean val = ConfigurationUtils.readBooleanProperty(config, "null", false);
        assertThat(val, equalTo(false));
    }

    public void testReadBooleanPropertyInvalidType() {
        try {
            ConfigurationUtils.readBooleanProperty(config, "arr", true);
        } catch (OpenSearchParseException e) {
            assertThat(e.getMessage(), equalTo("[arr] property isn't a boolean, but of type [java.util.Arrays$ArrayList]"));
        }
    }

    public void testReadStringOrIntProperty() {
        String val1 = ConfigurationUtils.readStringOrIntProperty(config, "foo", null);
        String val2 = ConfigurationUtils.readStringOrIntProperty(config, "num", null);
        assertThat(val1, equalTo("bar"));
        assertThat(val2, equalTo("1"));
    }

    public void testOptionalReadStringOrIntProperty() {
        String val1 = ConfigurationUtils.readOptionalStringOrIntProperty(config, "foo");
        String val2 = ConfigurationUtils.readOptionalStringOrIntProperty(config, "num");
        String val3 = ConfigurationUtils.readOptionalStringOrIntProperty(config, "num1");
        assertThat(val1, equalTo("bar"));
        assertThat(val2, equalTo("1"));
        assertThat(val3, equalTo(null));
    }

    public void testReadIntProperty() {
        int val = ConfigurationUtils.readIntProperty(config, "num", null);
        assertThat(val, equalTo(1));
        try {
            ConfigurationUtils.readIntProperty(config, "foo", 2);
        } catch (OpenSearchParseException e) {
            assertThat(e.getMessage(), equalTo("[foo] property cannot be converted to an int [bar]"));
        }
        try {
            ConfigurationUtils.readIntProperty(config, "foo1", 2);
        } catch (OpenSearchParseException e) {
            assertThat(e.getMessage(), equalTo("required property is missing"));
        }
    }

    public void testReadDoubleProperty() {
        double val = ConfigurationUtils.readDoubleProperty(config, "double");
        assertThat(val, equalTo(1.0));
        try {
            ConfigurationUtils.readDoubleProperty(config, "foo");
        } catch (OpenSearchParseException e) {
            assertThat(e.getMessage(), equalTo("[foo] property cannot be converted to a double [bar]"));
        }
        try {
            ConfigurationUtils.readDoubleProperty(config, "foo1");
        } catch (OpenSearchParseException e) {
            assertThat(e.getMessage(), equalTo("[foo1] required property is missing"));
        }
    }

    public void testReadStringOrIntPropertyInvalidType() {
        try {
            ConfigurationUtils.readStringOrIntProperty(config, "arr", null);
        } catch (OpenSearchParseException e) {
            assertThat(e.getMessage(), equalTo("[arr] property isn't a string or int, but of type [java.util.Arrays$ArrayList]"));
        }
    }

}
