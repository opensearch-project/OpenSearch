/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.bootstrap;

import org.opensearch.common.SuppressForbidden;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InvalidClassException;
import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests for the process-wide deserialization filter installed by {@link Bootstrap#initializeSerialFilter()}.
 * <p>
 * The filter rejects all Java deserialization by default. Plugins that need deserialization
 * (e.g., security plugin) opt in by calling {@code setObjectInputFilter()} on their stream,
 * which overrides the JVM-wide filter for that stream.
 * <p>
 * Note: String/primitive types use special serialization type codes (TC_STRING) that bypass
 * ObjectInputFilter checks. These tests use ArrayList to exercise the filter on real object types.
 */
@SuppressForbidden(reason = "testing the runtime serialization filter that protects against java deserialization")
public class BootstrapSerialFilterTests extends OpenSearchTestCase {

    private static final boolean FILTER_INSTALLED;

    static {
        // Install the JVM-wide filter. This can only be set once per JVM — if another test
        // or the framework already set it, the end-to-end tests are skipped.
        boolean installed = false;
        try {
            ObjectInputFilter.Config.setSerialFilter(BootstrapSettings.REJECT_ALL_FILTER);
            installed = true;
        } catch (IllegalStateException e) {
            // Already set
        }
        FILTER_INSTALLED = installed;
    }

    // --- Unit tests for the filter logic (always run) ---

    public void testRejectAllFilterRejectsClasses() {
        assertEquals(ObjectInputFilter.Status.REJECTED, BootstrapSettings.REJECT_ALL_FILTER.checkInput(filterInfo(String.class)));
    }

    public void testRejectAllFilterRejectsAnyClass() {
        assertEquals(ObjectInputFilter.Status.REJECTED, BootstrapSettings.REJECT_ALL_FILTER.checkInput(filterInfo(Runtime.class)));
    }

    public void testRejectAllFilterUndecidedForNullClass() {
        // null serialClass = stream metadata check (depth, bytes, refs), not a class resolution
        assertEquals(ObjectInputFilter.Status.UNDECIDED, BootstrapSettings.REJECT_ALL_FILTER.checkInput(filterInfo(null)));
    }

    // --- End-to-end tests showing actual runtime behavior ---

    /**
     * When a plugin uses ObjectInputStream without setting its own filter,
     * deserialization fails with InvalidClassException at runtime.
     * This is the protection against unexpected deserialization in plugins/dependencies.
     */
    public void testDeserializationRejectedWithoutExplicitFilter() throws Exception {
        assumeTrue("JVM-wide serial filter not installed in this JVM", FILTER_INSTALLED);

        byte[] serialized = serialize(new ArrayList<>(List.of("a", "b")));
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(serialized))) {
            InvalidClassException e = expectThrows(InvalidClassException.class, ois::readObject);
            assertTrue(e.getMessage().contains("REJECTED"));
        }
    }

    /**
     * When a plugin explicitly sets its own filter (like security plugin's SafeObjectInputStream),
     * the stream-level filter overrides the JVM-wide reject-all, and deserialization succeeds.
     */
    public void testDeserializationAllowedWithExplicitFilter() throws Exception {
        assumeTrue("JVM-wide serial filter not installed in this JVM", FILTER_INSTALLED);

        byte[] serialized = serialize(new ArrayList<>(List.of("a", "b")));
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(serialized))) {
            // This is what security plugin does — sets a filter on the stream
            ois.setObjectInputFilter(ObjectInputFilter.Config.createFilter("maxdepth=10"));
            Object result = ois.readObject();
            assertEquals(List.of("a", "b"), result);
        }
    }

    /**
     * Proves the stream-level filter is actually enforced — not just bypassing all checks.
     * A maxdepth=2 filter allows a shallow ArrayList but rejects a deeply nested structure.
     */
    public void testStreamFilterDepthConstraintIsEnforced() throws Exception {
        assumeTrue("JVM-wide serial filter not installed in this JVM", FILTER_INSTALLED);

        // Create a deeply nested object: ArrayList -> ArrayList -> ArrayList (depth=3)
        ArrayList<Object> deep = new ArrayList<>();
        deep.add(new ArrayList<>(List.of(new ArrayList<>(List.of("nested")))));

        byte[] serialized = serialize(deep);
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(serialized))) {
            // maxdepth=2 should reject the depth=3 structure
            ois.setObjectInputFilter(ObjectInputFilter.Config.createFilter("maxdepth=2"));
            InvalidClassException e = expectThrows(InvalidClassException.class, ois::readObject);
            assertTrue(e.getMessage().contains("REJECTED"));
        }
    }

    // --- Helpers ---

    private static ObjectInputFilter.FilterInfo filterInfo(Class<?> clazz) {
        return new ObjectInputFilter.FilterInfo() {
            public Class<?> serialClass() {
                return clazz;
            }

            public long arrayLength() {
                return -1;
            }

            public long depth() {
                return 1;
            }

            public long references() {
                return 1;
            }

            public long streamBytes() {
                return 0;
            }
        };
    }

    private static byte[] serialize(Object obj) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(obj);
        }
        return baos.toByteArray();
    }
}
