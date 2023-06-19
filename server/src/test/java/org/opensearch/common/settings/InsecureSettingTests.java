/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.settings;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import org.opensearch.common.logging.Loggers;
import org.opensearch.test.OpenSearchTestCase;

public class InsecureSettingTests extends OpenSearchTestCase {
    private List<String> rootLogMsgs = new ArrayList<>();
    private AbstractAppender rootAppender;

    protected void assertSettingWarning() {
        assertWarnings(
            "[setting.name] setting was deprecated in OpenSearch and will be removed in a future release! See the breaking changes documentation for the next major version."
        );

        Assert.assertTrue(
            this.rootLogMsgs.stream()
                .anyMatch(
                    msg -> msg.equals(
                        "Setting [setting.name] is insecure, but a secure variant [setting.name_secure] is advised to be used instead"
                    )
                )
        );
    }

    @Before
    public void addInsecureSettingsAppender() {
        this.rootLogMsgs.clear();
        rootAppender = new AbstractAppender("root", null, null, true, Property.EMPTY_ARRAY) {
            @Override
            public void append(LogEvent event) {
                String message = event.getMessage().getFormattedMessage();
                InsecureSettingTests.this.rootLogMsgs.add(message);
            }
        };
        Loggers.addAppender(LogManager.getRootLogger(), rootAppender);
        rootAppender.start();
    }

    @After
    public void removeInsecureSettingsAppender() {
        Loggers.removeAppender(LogManager.getRootLogger(), rootAppender);
    }

    public void testShouldRaiseExceptionByDefault() {
        final var setting = SecureSetting.insecureString("setting.name");
        final var settings = Settings.builder().put(setting.getKey(), "value").build();

        final var exception = Assert.assertThrows(IllegalArgumentException.class, () -> setting.get(settings));
        Assert.assertEquals(
            "Setting [setting.name] is insecure, but property [allow_insecure_settings] is not set",
            exception.getMessage()
        );
    }

    public void testShouldLogWarn() {
        final var setting = SecureSetting.insecureString("setting.name", "setting.name_secure", true);
        final var settings = Settings.builder().put(setting.getKey(), "value").build();

        Assert.assertEquals("value", setting.get(settings).toString());
        assertSettingWarning();
    }

    public void testShouldLogWarnOnce() {
        final var setting = SecureSetting.insecureString("setting.name", "setting.name_secure", true);
        final var settings = Settings.builder().put(setting.getKey(), "value").build();

        Assert.assertEquals("value", setting.get(settings).toString());
        Assert.assertEquals("value", setting.get(settings).toString());
        assertSettingWarning();

        // check that warning was only logged once
        Assert.assertEquals(1, this.rootLogMsgs.stream().filter(msg -> msg.contains("but a secure variant")).count());

    }

    public void testShouldRaiseExceptionIfConfigured() {
        final var setting = SecureSetting.insecureString("setting.name", "setting.name_secure", false);
        final var settings = Settings.builder().put(setting.getKey(), "value").build();

        final var exception = Assert.assertThrows(IllegalArgumentException.class, () -> setting.get(settings));
        Assert.assertEquals(
            "Setting [setting.name] is insecure, but property [allow_insecure_settings] is not set",
            exception.getMessage()
        );
    }

    public void testShouldFallbackToInsecure() {
        final var insecureSetting = SecureSetting.insecureString("setting.name", "setting.name_secure", true);
        final var secureSetting = SecureSetting.secureString("setting.name_secure", insecureSetting);
        final var settings = Settings.builder().put(insecureSetting.getKey(), "value").build();

        Assert.assertEquals("value", secureSetting.get(settings).toString());
        assertSettingWarning();
    }
}
