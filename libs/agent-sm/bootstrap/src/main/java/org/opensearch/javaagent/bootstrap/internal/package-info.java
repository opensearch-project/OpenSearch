/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Internal agent support classes that must be loaded by the boot classloader
 * so that bytecode woven into JDK classes (either inlined ByteBuddy Advice or
 * MethodDelegation stubs) can resolve them. These classes are implementation
 * details of the Java agent and are not part of any public API; do not depend
 * on them from outside {@code :libs:agent-sm:agent}.
 */
package org.opensearch.javaagent.bootstrap.internal;
