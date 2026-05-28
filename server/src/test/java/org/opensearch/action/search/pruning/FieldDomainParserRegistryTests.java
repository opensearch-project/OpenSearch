/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search.pruning;

import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class FieldDomainParserRegistryTests extends OpenSearchTestCase {
    public void testDefaultRegistryContainsDateRangeParser() {
        Optional<FieldDomainParser> parser = FieldDomainParserRegistry.defaultRegistry().get(DateRangeFieldDomain.TYPE);

        assertTrue(parser.isPresent());
        assertThat(parser.get(), instanceOf(DateRangeFieldDomainParser.class));
    }

    public void testReturnsCorrectType() {
        FieldDomainParserRegistry registry = new FieldDomainParserRegistry(List.of(new TestParser("test")));

        assertTrue(registry.get("test").isPresent());
    }

    public void testReturnsEmptyForUnknownType() {
        FieldDomainParserRegistry registry = new FieldDomainParserRegistry(List.of(new TestParser("test")));

        assertTrue(registry.get("unknown").isEmpty());
    }

    public void testRejectsDuplicateParserTypes() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new FieldDomainParserRegistry(List.of(new TestParser("test"), new TestParser("test")))
        );

        assertThat(exception.getMessage(), equalTo("field domain parser [test] is already registered"));
    }

    public void testRejectsNullParserListOrParser() {
        expectThrows(NullPointerException.class, () -> new FieldDomainParserRegistry(null));
        expectThrows(NullPointerException.class, () -> new FieldDomainParserRegistry(java.util.Collections.singletonList(null)));
    }

    public void testRemoveFieldKeysDelegatesToAllRegisteredParsers() {
        FieldDomainParserRegistry registry = new FieldDomainParserRegistry(List.of(new TestParser("alpha"), new TestParser("beta")));
        Map<String, String> target = new HashMap<>();
        target.put("fields.event.alpha.owned", "removed");
        target.put("fields.event.beta.owned", "removed");
        target.put("fields.event.unowned", "preserved");

        registry.removeFieldKeys(target, "fields.event.");

        assertFalse(target.containsKey("fields.event.alpha.owned"));
        assertFalse(target.containsKey("fields.event.beta.owned"));
        assertThat(target.get("fields.event.unowned"), equalTo("preserved"));
    }

    private static final class TestParser implements FieldDomainParser {
        private final String type;

        private TestParser(String type) {
            this.type = type;
        }

        @Override
        public String type() {
            return type;
        }

        @Override
        public Optional<FieldDomain> parse(String field, Map<String, String> customData, String prefix) {
            return Optional.empty();
        }

        @Override
        public void encode(FieldDomain domain, Map<String, String> target, String prefix) {}

        @Override
        public void removeFieldKeys(Map<String, String> target, String prefix) {
            target.remove(prefix + type + ".owned");
        }
    }
}
