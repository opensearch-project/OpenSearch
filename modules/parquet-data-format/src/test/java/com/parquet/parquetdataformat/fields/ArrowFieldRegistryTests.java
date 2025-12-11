/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.fields;

import com.parquet.parquetdataformat.fields.core.data.number.LongParquetField;
import com.parquet.parquetdataformat.plugins.fields.ParquetFieldPlugin;
import com.parquet.parquetdataformat.vsr.ManagedVSR;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.test.OpenSearchTestCase;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit test for ArrowFieldRegistry
 */
public class ArrowFieldRegistryTests extends OpenSearchTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    public void testRegistryInitializationCreatesNonEmptyRegistry() {
        ArrowFieldRegistry.RegistryStats stats = ArrowFieldRegistry.getStats();

        assertNotNull("Registry stats should not be null", stats);
        assertTrue("Registry should contain some fields after initialization", stats.getTotalFields() > 0);

        Set<String> fieldTypes = stats.getAllFieldTypes();
        assertNotNull("Field types set should not be null", fieldTypes);
        assertFalse("Field types set should not be empty", fieldTypes.isEmpty());

        assertTrue("Primary term field should be registered",
            fieldTypes.contains(SeqNoFieldMapper.PRIMARY_TERM_NAME));

        assertEquals("Total fields should match field types size",
            stats.getTotalFields(), fieldTypes.size());
    }

    public void testGetParquetFieldReturnsCorrectImplementation() {
        ParquetField primaryTermField = ArrowFieldRegistry.getParquetField(SeqNoFieldMapper.PRIMARY_TERM_NAME);
        assertNotNull("Primary term field should be registered", primaryTermField);
        assertTrue("Primary term field should be LongParquetField",
            primaryTermField instanceof LongParquetField);

        Set<String> registeredTypes = ArrowFieldRegistry.getRegisteredFieldNames();
        assertFalse("Should have registered field types", registeredTypes.isEmpty());

        for (String fieldType : registeredTypes) {
            ParquetField field = ArrowFieldRegistry.getParquetField(fieldType);
            assertNotNull("Should return a ParquetField for registered type: " + fieldType, field);
            assertNotNull("ParquetField should have valid ArrowType for: " + fieldType, field.getArrowType());
            assertNotNull("ParquetField should have valid FieldType for: " + fieldType, field.getFieldType());
        }

        ParquetField unknownField = ArrowFieldRegistry.getParquetField("unknown_field_type");
        assertNull("Should return null for unknown field type", unknownField);
    }

    public void testRegisterPluginWithEmptyOrNullFields() throws Exception {
        int initialSize = ArrowFieldRegistry.getStats().getTotalFields();

        registerPluginViaReflection(() -> Collections.emptyMap(), "EmptyPlugin");

        assertEquals("Registry size should be unchanged after empty plugin",
            initialSize, ArrowFieldRegistry.getStats().getTotalFields());

        registerPluginViaReflection(() -> null, "NullPlugin");

        assertEquals("Registry size should be unchanged after null plugin",
            initialSize, ArrowFieldRegistry.getStats().getTotalFields());
    }

    public void testRegisterPluginWithNullFieldType() throws Exception {
        Map<String, ParquetField> fieldsWithNullKey = new HashMap<>();
        fieldsWithNullKey.put(null, new LongParquetField());

        Exception exception = expectThrows(Exception.class, () -> {
            registerPluginViaReflection(() -> fieldsWithNullKey, "NullKeyPlugin");
        });

        Throwable cause = exception.getCause() != null ? exception.getCause() : exception;
        assertTrue("Should be IllegalArgumentException", cause instanceof IllegalArgumentException);
        assertTrue("Exception message should mention null field type",
            cause.getMessage().contains("Field type name cannot be null or empty"));
    }

    public void testRegisterPluginWithEmptyFieldType() throws Exception {
        Map<String, ParquetField> fieldsWithEmptyKey = new HashMap<>();
        fieldsWithEmptyKey.put("", new LongParquetField());

        Exception exception1 = expectThrows(Exception.class, () -> {
            registerPluginViaReflection(() -> fieldsWithEmptyKey, "EmptyKeyPlugin");
        });

        Throwable cause1 = exception1.getCause() != null ? exception1.getCause() : exception1;
        assertTrue("Should be IllegalArgumentException", cause1 instanceof IllegalArgumentException);
        assertTrue("Exception message should mention empty field type",
            cause1.getMessage().contains("cannot be null or empty"));

        Map<String, ParquetField> fieldsWithWhitespaceKey = new HashMap<>();
        fieldsWithWhitespaceKey.put("   ", new LongParquetField());

        Exception exception2 = expectThrows(Exception.class, () -> {
            registerPluginViaReflection(() -> fieldsWithWhitespaceKey, "WhitespacePlugin");
        });

        Throwable cause2 = exception2.getCause() != null ? exception2.getCause() : exception2;
        assertTrue("Should be IllegalArgumentException", cause2 instanceof IllegalArgumentException);
        assertTrue("Exception message should mention empty field type",
            cause2.getMessage().contains("cannot be null or empty"));
    }

    public void testRegisterPluginWithNullParquetField() throws Exception {
        Map<String, ParquetField> fieldsWithNullValue = new HashMap<>();
        fieldsWithNullValue.put("test_field", null);

        Exception exception = expectThrows(Exception.class, () -> {
            registerPluginViaReflection(() -> fieldsWithNullValue, "NullValuePlugin");
        });

        Throwable cause = exception.getCause() != null ? exception.getCause() : exception;
        assertTrue("Should be IllegalArgumentException", cause instanceof IllegalArgumentException);
        assertTrue("Exception message should mention null ParquetField",
            cause.getMessage().contains("ParquetField implementation cannot be null"));
    }

    public void testRegisterPluginWithInvalidParquetField() throws Exception {
        ParquetField failingArrowTypeField = new ParquetField() {
            @Override
            protected void addToGroup(MappedFieldType mappedFieldType, ManagedVSR managedVSR, Object parseValue) {}

            @Override
            public ArrowType getArrowType() {
                throw new RuntimeException("Arrow type failure");
            }

            @Override
            public FieldType getFieldType() {
                return new FieldType(true, new ArrowType.Utf8(), null);
            }
        };

        Map<String, ParquetField> fieldsWithFailingArrowType = new HashMap<>();
        fieldsWithFailingArrowType.put("failing_arrow_type", failingArrowTypeField);

        Exception exception1 = expectThrows(Exception.class, () -> {
            registerPluginViaReflection(() -> fieldsWithFailingArrowType, "FailingArrowTypePlugin");
        });

        Throwable cause1 = exception1.getCause() != null ? exception1.getCause() : exception1;
        assertTrue("Should be IllegalArgumentException", cause1 instanceof IllegalArgumentException);
        assertTrue("Exception message should mention invalid ParquetField implementation",
            cause1.getMessage().contains("Invalid ParquetField implementation"));
        assertTrue("Exception message should mention field type",
            cause1.getMessage().contains("failing_arrow_type"));

        ParquetField failingFieldTypeField = new ParquetField() {
            @Override
            protected void addToGroup(MappedFieldType mappedFieldType, ManagedVSR managedVSR, Object parseValue) {}

            @Override
            public ArrowType getArrowType() {
                return new ArrowType.Utf8();
            }

            @Override
            public FieldType getFieldType() {
                throw new RuntimeException("Field type failure");
            }
        };

        Map<String, ParquetField> fieldsWithFailingFieldType = new HashMap<>();
        fieldsWithFailingFieldType.put("failing_field_type", failingFieldTypeField);

        Exception exception2 = expectThrows(Exception.class, () -> {
            registerPluginViaReflection(() -> fieldsWithFailingFieldType, "FailingFieldTypePlugin");
        });

        Throwable cause2 = exception2.getCause() != null ? exception2.getCause() : exception2;
        assertTrue("Should be IllegalArgumentException", cause2 instanceof IllegalArgumentException);
        assertTrue("Exception message should mention invalid ParquetField implementation",
            cause2.getMessage().contains("Invalid ParquetField implementation"));
    }

    public void testRegisterPluginWithDuplicateFieldName() throws Exception {
        Set<String> existingFields = ArrowFieldRegistry.getRegisteredFieldNames();
        String existingFieldType = existingFields.iterator().next();

        Map<String, ParquetField> duplicateFields = new HashMap<>();
        duplicateFields.put(existingFieldType, new LongParquetField());

        Exception exception = expectThrows(Exception.class, () -> {
            registerPluginViaReflection(() -> duplicateFields, "DuplicatePlugin");
        });

        Throwable cause = exception.getCause() != null ? exception.getCause() : exception;
        assertTrue("Should be IllegalArgumentException", cause instanceof IllegalArgumentException);
        assertTrue("Exception message should mention field already registered",
            cause.getMessage().contains("is already registered"));
        assertTrue("Exception message should mention the field type",
            cause.getMessage().contains(existingFieldType));
    }

    public void testGetStatsReturnsCorrectStats() {
        ArrowFieldRegistry.RegistryStats stats = ArrowFieldRegistry.getStats();

        assertNotNull("Stats should not be null", stats);
        assertTrue("Total fields should be positive", stats.getTotalFields() > 0);
        assertNotNull("All field types should not be null", stats.getAllFieldTypes());
        assertFalse("All field types should not be empty", stats.getAllFieldTypes().isEmpty());

        assertEquals("Total fields should match field types size",
            stats.getTotalFields(), stats.getAllFieldTypes().size());

        String statsString = stats.toString();
        assertNotNull("Stats toString should not be null", statsString);
        assertTrue("Stats toString should contain total count",
            statsString.contains("total=" + stats.getTotalFields()));

        ArrowFieldRegistry.RegistryStats stats2 = ArrowFieldRegistry.getStats();
        assertEquals("Total fields should be consistent between calls",
            stats.getTotalFields(), stats2.getTotalFields());
        assertEquals("Field types should be consistent between calls",
            stats.getAllFieldTypes(), stats2.getAllFieldTypes());
    }

    public void testRegistryIsThreadSafe() throws Exception {
        int numThreads = 10;
        int operationsPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < numThreads; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < operationsPerThread; j++) {
                        ArrowFieldRegistry.getStats();
                        ArrowFieldRegistry.getRegisteredFieldNames();
                        ArrowFieldRegistry.getParquetField(SeqNoFieldMapper.PRIMARY_TERM_NAME);
                    }
                    successCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue("All threads should complete successfully",
            latch.await(30, TimeUnit.SECONDS));
        assertEquals("All threads should succeed", numThreads, successCount.get());

        executor.shutdown();
        assertTrue("Executor should terminate", executor.awaitTermination(5, TimeUnit.SECONDS));

        ArrowFieldRegistry.RegistryStats finalStats = ArrowFieldRegistry.getStats();
        assertTrue("Registry should still have fields after concurrent access",
            finalStats.getTotalFields() > 0);
        assertTrue("Primary term should still be registered",
            finalStats.getAllFieldTypes().contains(SeqNoFieldMapper.PRIMARY_TERM_NAME));
    }

    public void testPrimaryTermAlwaysAdded() {
        Set<String> fieldTypes = ArrowFieldRegistry.getRegisteredFieldNames();

        assertTrue("PRIMARY_TERM should always be registered",
            fieldTypes.contains(SeqNoFieldMapper.PRIMARY_TERM_NAME));

        ParquetField primaryTermField = ArrowFieldRegistry.getParquetField(SeqNoFieldMapper.PRIMARY_TERM_NAME);
        assertNotNull("PRIMARY_TERM field should not be null", primaryTermField);
        assertTrue("PRIMARY_TERM should be LongParquetField",
            primaryTermField instanceof LongParquetField);
    }

    public void testGetParquetFieldHandlesNullAndEmptyInput() {
        expectThrows(NullPointerException.class, () -> {
            ArrowFieldRegistry.getParquetField(null);
        });

        assertNull("Should return null for empty string",
            ArrowFieldRegistry.getParquetField(""));
        assertNull("Should return null for whitespace string",
            ArrowFieldRegistry.getParquetField("   "));
    }

    public void testGetRegisteredFieldNamesReturnsUnmodifiableSet() {
        Set<String> fieldNames = ArrowFieldRegistry.getRegisteredFieldNames();

        assertNotNull("Field names set should not be null", fieldNames);

        expectThrows(UnsupportedOperationException.class, () -> {
            fieldNames.add("test_field");
        });

        expectThrows(UnsupportedOperationException.class, () -> {
            fieldNames.remove(fieldNames.iterator().next());
        });
    }

    private void registerPluginViaReflection(ParquetFieldPlugin plugin, String pluginName) throws Exception {
        Method registerPluginMethod = ArrowFieldRegistry.class.getDeclaredMethod("registerPlugin",
            ParquetFieldPlugin.class,
            String.class);
        registerPluginMethod.setAccessible(true);
        registerPluginMethod.invoke(null, plugin, pluginName);
    }
}
