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

package org.opensearch.test.loggerusage;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.MessageSupplier;
import org.apache.logging.log4j.util.Supplier;
import org.opensearch.common.SuppressLoggerChecks;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.loggerusage.OpenSearchLoggerUsageChecker.WrongLoggerUsage;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.oneOf;

public class OpenSearchLoggerUsageTests extends OpenSearchTestCase {

    public void testLoggerUsageChecks() throws IOException {
        for (Method method : getClass().getMethods()) {
            if (method.getDeclaringClass().equals(getClass())) {
                if (method.getName().startsWith("check")) {
                    logger.info("Checking logger usage for method {}", method.getName());
                    InputStream classInputStream = getClass().getResourceAsStream(getClass().getSimpleName() + ".class");
                    List<WrongLoggerUsage> errors = new ArrayList<>();
                    OpenSearchLoggerUsageChecker.check(errors::add, classInputStream,
                        m -> m.equals(method.getName()) || m.startsWith("lambda$" + method.getName()));
                    if (method.getName().startsWith("checkFail")) {
                        assertFalse("Expected " + method.getName() + " to have wrong Logger usage", errors.isEmpty());
                    } else {
                        assertTrue("Method " + method.getName() + " has unexpected Logger usage errors: " + errors, errors.isEmpty());
                    }
                } else {
                    assertTrue("only allow methods starting with test or check in this class", method.getName().startsWith("test"));
                }
            }
        }
    }

    public void testLoggerUsageCheckerCompatibilityWithLog4j2Logger() {
        for (Method method : Logger.class.getMethods()) {
            if (OpenSearchLoggerUsageChecker.LOGGER_METHODS.contains(method.getName())) {
                assertThat(method.getParameterCount(), greaterThanOrEqualTo(1));
                final Class<?>[] parameterTypes = method.getParameterTypes();
                int markerOffset = parameterTypes[0].equals(Marker.class) ? 1 : 0;
                int paramLength = parameterTypes.length - markerOffset;
                if (method.isVarArgs()) {
                    assertEquals(2, paramLength);
                    assertEquals(String.class, parameterTypes[markerOffset]);
                    assertThat(parameterTypes[markerOffset + 1], is(oneOf(Object[].class, Supplier[].class)));
                } else {
                    assertThat(parameterTypes[markerOffset], is(oneOf(Message.class, MessageSupplier.class,
                        CharSequence.class, Object.class, String.class, Supplier.class)));

                    if (paramLength == 2) {
                        assertThat(parameterTypes[markerOffset + 1], is(oneOf(Throwable.class, Object.class)));
                        if (parameterTypes[markerOffset + 1].equals(Object.class)) {
                            assertEquals(String.class, parameterTypes[markerOffset]);
                        }
                    }
                    if (paramLength > 2) {
                        assertEquals(String.class, parameterTypes[markerOffset]);
                        assertThat(paramLength, lessThanOrEqualTo(11));
                        for (int i = 1; i < paramLength; i++) {
                            assertEquals(Object.class, parameterTypes[markerOffset + i]);
                        }
                    }
                }
            }
        }

        for (String methodName : OpenSearchLoggerUsageChecker.LOGGER_METHODS) {
            assertEquals(48, Stream.of(Logger.class.getMethods()).filter(m -> methodName.equals(m.getName())).count());
        }

        for (Constructor<?> constructor : ParameterizedMessage.class.getConstructors()) {
            assertThat(constructor.getParameterCount(), greaterThanOrEqualTo(2));
            final Class<?>[] parameterTypes = constructor.getParameterTypes();
            assertEquals(String.class, parameterTypes[0]);
            assertThat(parameterTypes[1], is(oneOf(String[].class, Object[].class, Object.class)));

            if (parameterTypes.length > 2) {
                assertEquals(3, parameterTypes.length);
                if (parameterTypes[1].equals(Object.class)) {
                    assertEquals(Object.class, parameterTypes[2]);
                } else {
                    assertEquals(Throwable.class, parameterTypes[2]);
                }
            }
        }

        assertEquals(5, ParameterizedMessage.class.getConstructors().length);
    }
    public void checkForSubclasses() {
        logger.debug(new TestMessage("message", "x-opaque-id"));
    }

    public void checkArraySizeForSubclasses() {
        logger.debug(new TestMessage("message {}", "x-opaque-id", 1));
    }

    public void checkFailArraySizeForSubclasses(Object... arr) {
        logger.debug(new TestMessage("message {}", "x-opaque-id", arr));
    }

    public void checkFailArraySize(String... arr) {
        logger.debug(new ParameterizedMessage("text {}", (Object[])arr));
    }

    public void checkNumberOfArguments1() {
        logger.info("Hello {}", "world");
    }

    public void checkFailNumberOfArguments1() {
        logger.info("Hello {}");
    }

    @SuppressLoggerChecks(reason = "test ignore functionality")
    public void checkIgnoreWhenAnnotationPresent() {
        logger.info("Hello {}");
    }

    public void checkNumberOfArguments2() {
        logger.info("Hello {}, {}, {}", "world", 2, "third argument");
    }

    public void checkFailNumberOfArguments2() {
        logger.info("Hello {}, {}", "world", 2, "third argument");
    }

    public void checkNumberOfArguments3() {
        logger.info("Hello {}, {}, {}, {}, {}, {}, {}", "world", 2, "third argument", 4, 5, 6, new String("last arg"));
    }

    public void checkFailNumberOfArguments3() {
        logger.info("Hello {}, {}, {}, {}, {}, {}, {}", "world", 2, "third argument", 4, 5, 6, 7, new String("last arg"));
    }

    public void checkNumberOfArgumentsParameterizedMessage1() {
        logger.info(new ParameterizedMessage("Hello {}, {}, {}", "world", 2, "third argument"));
    }

    public void checkFailNumberOfArgumentsParameterizedMessage1() {
        logger.info(new ParameterizedMessage("Hello {}, {}", "world", 2, "third argument"));
    }

    public void checkNumberOfArgumentsParameterizedMessage2() {
        logger.info(new ParameterizedMessage("Hello {}, {}", "world", 2));
    }

    public void checkFailNumberOfArgumentsParameterizedMessage2() {
        logger.info(new ParameterizedMessage("Hello {}, {}, {}", "world", 2));
    }

    public void checkNumberOfArgumentsParameterizedMessage3() {
        logger.info((Supplier<?>) () -> new ParameterizedMessage("Hello {}, {}, {}", "world", 2, "third argument"));
    }

    public void checkFailNumberOfArgumentsParameterizedMessage3() {
        logger.info((Supplier<?>) () -> new ParameterizedMessage("Hello {}, {}", "world", 2, "third argument"));
    }

    public void checkOrderOfExceptionArgument() {
        logger.info("Hello", new Exception());
    }

    public void checkOrderOfExceptionArgument1() {
        logger.info((Supplier<?>) () -> new ParameterizedMessage("Hello {}", "world"), new Exception());
    }

    public void checkFailOrderOfExceptionArgument1() {
        logger.info("Hello {}", "world", new Exception());
    }

    public void checkOrderOfExceptionArgument2() {
        logger.info((Supplier<?>) () -> new ParameterizedMessage("Hello {}, {}", "world", 42), new Exception());
    }

    public void checkFailOrderOfExceptionArgument2() {
        logger.info("Hello {}, {}", "world", 42, new Exception());
    }

    public void checkNonConstantMessageWithZeroArguments(boolean b) {
        logger.info(Boolean.toString(b), new Exception());
    }

    public void checkFailNonConstantMessageWithArguments(boolean b) {
        logger.info((Supplier<?>) () -> new ParameterizedMessage(Boolean.toString(b), 42), new Exception());
    }

    public void checkComplexUsage(boolean b) {
        String message = "Hello {}, {}";
        Object[] args = new Object[] { "world", 42 };
        if (b) {
            message = "also two args {}{}";
            args = new Object[] { "world", 43 };
        }
        logger.info(message, args);
    }

    public void checkFailComplexUsage1(boolean b) {
        String message = "Hello {}, {}";
        Object[] args = new Object[] { "world", 42 };
        if (b) {
            message = "just one arg {}";
            args = new Object[] { "world", 43 };
        }
        logger.info(message, args);
    }

    public void checkFailComplexUsage2(boolean b) {
        String message = "Hello {}, {}";
        Object[] args = new Object[] { "world", 42 };
        if (b) {
            message = "also two args {}{}";
            args = new Object[] { "world", 43, "another argument" };
        }
        logger.info(message, args);
    }

    public void checkDeprecationLogger() {
        DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(OpenSearchLoggerUsageTests.class);
        deprecationLogger.deprecate("key","message {}", 123);
    }

}
