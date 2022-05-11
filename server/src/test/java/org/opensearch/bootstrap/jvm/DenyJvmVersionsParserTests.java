/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.bootstrap.jvm;

import org.opensearch.bootstrap.jvm.DenyJvmVersionsParser.VersionPredicate;
import org.opensearch.test.OpenSearchTestCase;

import java.lang.Runtime.Version;
import java.util.Collection;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.collection.IsEmptyCollection.empty;

public class DenyJvmVersionsParserTests extends OpenSearchTestCase {
    public void testDefaults() {
        final Collection<VersionPredicate> versions = DenyJvmVersionsParser.getDeniedJvmVersions();
        assertThat(versions, not(empty()));
    }

    public void testSingleVersion() {
        final VersionPredicate predicate = DenyJvmVersionsParser.parse("11.0.2: know to be flawed version");
        assertThat(predicate.test(Version.parse("11.0.2")), is(true));
        assertThat(predicate.test(Version.parse("11.0.1")), is(false));
        assertThat(predicate.test(Version.parse("11.0.3")), is(false));
        assertThat(predicate.getReason(), equalTo("know to be flawed version"));
    }

    public void testVersionRangeLowerIncluded() {
        final VersionPredicate predicate = DenyJvmVersionsParser.parse("[11.0.2, 11.0.14): know to be flawed version");
        assertThat(predicate.test(Version.parse("11.0.2")), is(true));
        assertThat(predicate.test(Version.parse("11.0.1")), is(false));
        assertThat(predicate.test(Version.parse("11.0.13+1")), is(true));
        assertThat(predicate.test(Version.parse("11.0.14")), is(false));
        assertThat(predicate.getReason(), equalTo("know to be flawed version"));
    }

    public void testVersionRangeUpperIncluded() {
        final VersionPredicate predicate = DenyJvmVersionsParser.parse("[11.0.2,): know to be flawed version");
        assertThat(predicate.test(Version.parse("11.0.2")), is(true));
        assertThat(predicate.test(Version.parse("11.0.1")), is(false));
        assertThat(predicate.test(Version.parse("11.0.13+1")), is(true));
        assertThat(predicate.test(Version.parse("11.0.14")), is(true));
        assertThat(predicate.test(Version.parse("17.2.1")), is(true));
        assertThat(predicate.getReason(), equalTo("know to be flawed version"));
    }

    public void testVersionRangeLowerAndUpperIncluded() {
        final VersionPredicate predicate = DenyJvmVersionsParser.parse("[11.0.2, 11.0.14]: know to be flawed version");
        assertThat(predicate.test(Version.parse("11.0.2")), is(true));
        assertThat(predicate.test(Version.parse("11.0.1")), is(false));
        assertThat(predicate.test(Version.parse("11.0.13+1")), is(true));
        assertThat(predicate.test(Version.parse("11.0.14")), is(true));
        assertThat(predicate.test(Version.parse("11.0.14.1")), is(false));
        assertThat(predicate.test(Version.parse("11.0.15")), is(false));
        assertThat(predicate.getReason(), equalTo("know to be flawed version"));
    }

    public void testAllVersionsRange() {
        final VersionPredicate predicate = DenyJvmVersionsParser.parse("(,): know to be flawed version");
        assertThat(predicate.test(Version.parse("11.0.2")), is(true));
        assertThat(predicate.test(Version.parse("11.0.1")), is(true));
        assertThat(predicate.test(Version.parse("11.0.13+1")), is(true));
        assertThat(predicate.test(Version.parse("11.0.14")), is(true));
        assertThat(predicate.test(Version.parse("11.0.14.1")), is(true));
        assertThat(predicate.test(Version.parse("11.0.15")), is(true));
        assertThat(predicate.test(Version.parse("17.2.1")), is(true));
        assertThat(predicate.getReason(), equalTo("know to be flawed version"));
    }

    public void testAllVersionsRangeIncluded() {
        final VersionPredicate predicate = DenyJvmVersionsParser.parse("[*, *]: know to be flawed version");
        assertThat(predicate.test(Version.parse("11.0.2")), is(true));
        assertThat(predicate.test(Version.parse("11.0.1")), is(true));
        assertThat(predicate.test(Version.parse("11.0.13+1")), is(true));
        assertThat(predicate.test(Version.parse("11.0.14")), is(true));
        assertThat(predicate.test(Version.parse("11.0.14.1")), is(true));
        assertThat(predicate.test(Version.parse("11.0.15")), is(true));
        assertThat(predicate.test(Version.parse("17.2.1")), is(true));
        assertThat(predicate.getReason(), equalTo("know to be flawed version"));
    }

    public void testIncorrectVersionRanges() {
        assertThrows(IllegalArgumentException.class, () -> DenyJvmVersionsParser.parse("[*, *: know to be flawed version"));
        assertThrows(IllegalArgumentException.class, () -> DenyJvmVersionsParser.parse("*, *: know to be flawed version"));
        assertThrows(IllegalArgumentException.class, () -> DenyJvmVersionsParser.parse("*, *): know to be flawed version"));
        assertThrows(IllegalArgumentException.class, () -> DenyJvmVersionsParser.parse("(): know to be flawed version"));
        assertThrows(IllegalArgumentException.class, () -> DenyJvmVersionsParser.parse("[]: know to be flawed version"));
        assertThrows(IllegalArgumentException.class, () -> DenyJvmVersionsParser.parse("[,]"));
        assertThrows(IllegalArgumentException.class, () -> DenyJvmVersionsParser.parse("11.0.2"));
    }
}
