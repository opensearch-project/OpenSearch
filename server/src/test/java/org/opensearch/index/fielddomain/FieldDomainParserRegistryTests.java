/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.fielddomain;

import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class FieldDomainParserRegistryTests extends OpenSearchTestCase {
    public void testDefaultRegistryContainsDateRangeParser() {
        assertTrue(FieldDomainParserRegistry.defaultRegistry().contains(DateRangeFieldDomain.TYPE));
    }

    public void testReturnsCorrectType() {
        FieldDomainParserRegistry registry = new FieldDomainParserRegistry(
            List.of(FieldDomainParserRegistry.entry(FieldDomain.class, new TestParser("test")))
        );

        assertTrue(registry.contains("test"));
    }

    public void testReturnsEmptyForUnknownType() {
        FieldDomainParserRegistry registry = new FieldDomainParserRegistry(
            List.of(FieldDomainParserRegistry.entry(FieldDomain.class, new TestParser("test")))
        );

        assertFalse(registry.contains("unknown"));
        assertTrue(registry.fromCustomData("unknown", "field", Map.of(), "fields.field.").isEmpty());
    }

    public void testFromCustomDataReturnsEmptyWhenRegisteredParserReturnsEmpty() {
        FieldDomainParserRegistry registry = new FieldDomainParserRegistry(
            List.of(FieldDomainParserRegistry.entry(FieldDomain.class, new TestParser("test")))
        );

        assertTrue(registry.fromCustomData("test", "field", Map.of(), "fields.field.").isEmpty());
    }

    public void testRejectsDuplicateParserTypes() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new FieldDomainParserRegistry(
                List.of(
                    FieldDomainParserRegistry.entry(FieldDomain.class, new TestParser("test")),
                    FieldDomainParserRegistry.entry(FieldDomain.class, new TestParser("test"))
                )
            )
        );

        assertThat(exception.getMessage(), equalTo("field domain parser [test] is already registered"));
    }

    public void testRejectsNullParserListOrParser() {
        expectThrows(NullPointerException.class, () -> new FieldDomainParserRegistry(null));
        expectThrows(NullPointerException.class, () -> new FieldDomainParserRegistry(java.util.Collections.singletonList(null)));
    }

    public void testEntryRejectsNullDomainClassOrParser() {
        expectThrows(NullPointerException.class, () -> FieldDomainParserRegistry.entry(null, new TypedTestParser("test")));
        expectThrows(NullPointerException.class, () -> FieldDomainParserRegistry.entry(TypedTestDomain.class, null));
    }

    public void testFromCustomDataDelegatesToRegisteredEntry() {
        TypedTestDomain domain = new TypedTestDomain("event.ingested", "test");
        FieldDomainParserRegistry registry = new FieldDomainParserRegistry(
            List.of(FieldDomainParserRegistry.entry(TypedTestDomain.class, new TypedTestParser("test", Optional.of(domain))))
        );

        Optional<FieldDomain> maybeDomain = registry.fromCustomData(
            "test",
            "event.ingested",
            Map.of("fields.event.ingested.value", "value"),
            "fields.event.ingested."
        );

        assertTrue(maybeDomain.isPresent());
        assertThat(maybeDomain.get(), sameInstance(domain));
    }

    public void testWriteToCustomDataDelegatesToRegisteredEntry() {
        FieldDomainParserRegistry registry = new FieldDomainParserRegistry(
            List.of(FieldDomainParserRegistry.entry(TypedTestDomain.class, new TypedTestParser("test")))
        );
        Map<String, String> targetCustomData = new HashMap<>();

        registry.writeToCustomData(new TypedTestDomain("event.ingested", "test"), targetCustomData, "fields.event.ingested.");

        assertThat(targetCustomData.get("fields.event.ingested.written"), equalTo("event.ingested"));
    }

    public void testWriteToCustomDataRejectsUnknownType() {
        FieldDomainParserRegistry registry = new FieldDomainParserRegistry(List.of());

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> registry.writeToCustomData(new TypedTestDomain("event.ingested", "unknown"), new HashMap<>(), "fields.event.ingested.")
        );

        assertThat(exception.getMessage(), equalTo("unsupported field domain type [unknown]"));
    }

    public void testWriteToCustomDataRejectsMismatchedDomainClass() {
        FieldDomainParserRegistry registry = new FieldDomainParserRegistry(
            List.of(FieldDomainParserRegistry.entry(TypedTestDomain.class, new TypedTestParser("test")))
        );

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> registry.writeToCustomData(new OtherTestDomain("event.ingested", "test"), new HashMap<>(), "fields.event.ingested.")
        );

        assertThat(exception.getMessage(), containsString("is not supported by parser [test]"));
    }

    public void testRemoveFieldKeysDelegatesToAllRegisteredParsers() {
        FieldDomainParserRegistry registry = new FieldDomainParserRegistry(
            List.of(
                FieldDomainParserRegistry.entry(FieldDomain.class, new TestParser("alpha")),
                FieldDomainParserRegistry.entry(FieldDomain.class, new TestParser("beta"))
            )
        );
        Map<String, String> target = new HashMap<>();
        target.put("fields.event.alpha.owned", "removed");
        target.put("fields.event.beta.owned", "removed");
        target.put("fields.event.unowned", "preserved");

        registry.removeFieldKeys(target, "fields.event.");

        assertFalse(target.containsKey("fields.event.alpha.owned"));
        assertFalse(target.containsKey("fields.event.beta.owned"));
        assertThat(target.get("fields.event.unowned"), equalTo("preserved"));
    }

    private static final class TestParser implements FieldDomainParser<FieldDomain> {
        private final String type;

        private TestParser(String type) {
            this.type = type;
        }

        @Override
        public String type() {
            return type;
        }

        @Override
        public Optional<FieldDomain> fromCustomData(String field, Map<String, String> customData, String prefix) {
            return Optional.empty();
        }

        @Override
        public void writeToCustomData(FieldDomain domain, Map<String, String> targetCustomData, String prefix) {}

        @Override
        public void removeFieldKeys(Map<String, String> target, String prefix) {
            target.remove(prefix + type + ".owned");
        }
    }

    private static final class TypedTestParser implements FieldDomainParser<TypedTestDomain> {
        private final String type;
        private final Optional<TypedTestDomain> domain;

        private TypedTestParser(String type) {
            this(type, Optional.empty());
        }

        private TypedTestParser(String type, Optional<TypedTestDomain> domain) {
            this.type = type;
            this.domain = domain;
        }

        @Override
        public String type() {
            return type;
        }

        @Override
        public Optional<TypedTestDomain> fromCustomData(String field, Map<String, String> customData, String prefix) {
            return domain;
        }

        @Override
        public void writeToCustomData(TypedTestDomain domain, Map<String, String> targetCustomData, String prefix) {
            targetCustomData.put(prefix + "written", domain.field());
        }

        @Override
        public void removeFieldKeys(Map<String, String> target, String prefix) {
            target.remove(prefix + "written");
        }
    }

    private static final class TypedTestDomain implements FieldDomain {
        private final String field;
        private final String type;

        private TypedTestDomain(String field, String type) {
            this.field = field;
            this.type = type;
        }

        @Override
        public String field() {
            return field;
        }

        @Override
        public String type() {
            return type;
        }

        @Override
        public boolean finalized() {
            return true;
        }
    }

    private static final class OtherTestDomain implements FieldDomain {
        private final String field;
        private final String type;

        private OtherTestDomain(String field, String type) {
            this.field = field;
            this.type = type;
        }

        @Override
        public String field() {
            return field;
        }

        @Override
        public String type() {
            return type;
        }

        @Override
        public boolean finalized() {
            return true;
        }
    }
}
