/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.rest.RestParameter.Property;
import org.opensearch.rest.action.admin.cluster.RestDeleteRepositoryAction;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class RestParameterTests extends OpenSearchTestCase {

    public void testRequiredStringMissing() {
        RestParameter<String> index = RestParameter.simpleString("index", Property.Required, Property.Path);
        FakeRestRequest request = new FakeRestRequest();

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> index.get(request));
        assertThat(e.getMessage(), containsString("missing required parameter [index]"));
        assertTrue(index.isRequired());
        assertTrue(index.isPath());
        assertFalse(index.hasDefault());
    }

    public void testOptionalStringDefault() {
        RestParameter<String> metric = RestParameter.simpleString("metric", "_all");
        FakeRestRequest request = new FakeRestRequest();

        assertThat(metric.get(request), equalTo("_all"));
        assertFalse(metric.isRequired());
        assertFalse(metric.isPath());

        request.params().put("metric", "docs");
        assertThat(metric.get(request), equalTo("docs"));
        assertTrue(request.consumedParams().contains("metric"));
    }

    public void testBooleanEmptyValueMeansTrue() {
        RestParameter<Boolean> local = RestParameter.boolParam("local", false);
        FakeRestRequest request = new FakeRestRequest();
        request.params().put("local", "");

        assertThat(local.get(request), equalTo(true));
    }

    public void testBooleanUsesDefaultWhenAbsent() {
        RestParameter<Boolean> local = RestParameter.boolParam("local", true);
        assertThat(local.get(new FakeRestRequest()), equalTo(true));
    }

    public void testRequiredBooleanRejectsStaticDefaultFactory() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> RestParameter.boolParam("local", false, Property.Required)
        );
        assertThat(e.getMessage(), containsString("cannot be required and have a default value"));
    }

    public void testPrimitiveWithoutDefaultMustBeRequired() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> RestParameter.intParam("size"));
        assertThat(e.getMessage(), containsString("must be Property.Required"));
    }

    public void testIntParseFailure() {
        RestParameter<Integer> size = RestParameter.intParam("size", 10);
        FakeRestRequest request = new FakeRestRequest();
        request.params().put("size", "nope");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> size.get(request));
        assertThat(e.getMessage(), containsString("failed to parse int parameter [size]"));
    }

    public void testTimeValueDynamicDefault() {
        RestParameter<TimeValue> timeout = RestParameter.timeValue("timeout");
        TimeValue fallback = TimeValue.timeValueSeconds(30);
        FakeRestRequest request = new FakeRestRequest();

        assertThat(timeout.get(request, fallback), equalTo(fallback));
        assertThat(timeout.defaultValue(), nullValue());

        request.params().put("timeout", "5s");
        assertThat(timeout.get(request, fallback), equalTo(new TimeValue(5, TimeUnit.SECONDS)));
    }

    public void testRequiredRejectsDynamicDefault() {
        RestParameter<String> repository = RestParameter.simpleString("repository", Property.Required);
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> repository.get(new FakeRestRequest(), "fallback"));
        assertThat(e.getMessage(), containsString("cannot take a fallback default"));
    }

    public void testStringArraySplitsAndDefaultsToEmpty() {
        RestParameter<String[]> indices = RestParameter.stringArray("index", Property.Path);
        assertThat(indices.get(new FakeRestRequest()), equalTo(Strings.EMPTY_ARRAY));

        FakeRestRequest request = new FakeRestRequest();
        request.params().put("index", "a,b");
        assertThat(indices.get(request), equalTo(new String[] { "a", "b" }));
    }

    public void testByteSizeAndEnum() {
        ByteSizeValue oneByte = new ByteSizeValue(1);
        RestParameter<ByteSizeValue> size = RestParameter.byteSize("max_size", oneByte);
        assertThat(size.get(new FakeRestRequest()), equalTo(oneByte));

        RestParameter<TimeUnit> unit = RestParameter.enumParam("unit", TimeUnit.class, Property.Required);
        FakeRestRequest request = new FakeRestRequest();
        request.params().put("unit", "SECONDS");
        assertThat(unit.get(request), equalTo(TimeUnit.SECONDS));

        request.params().put("unit", "not-a-unit");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> unit.get(request));
        assertThat(e.getMessage(), containsString("as [TimeUnit]"));
    }

    public void testDeprecatedPropertyIsMetadataOnly() {
        RestParameter<TimeValue> masterTimeout = RestParameter.timeValue("master_timeout", Property.Deprecated);
        assertTrue(masterTimeout.isDeprecated());
        assertFalse(masterTimeout.isRequired());
    }

    public void testDeleteRepositoryDeclaresRequiredAndOptionalParams() {
        Map<String, RestParameter<?>> declared = new RestDeleteRepositoryAction().parameters()
            .stream()
            .collect(Collectors.toMap(RestParameter::getKey, parameter -> parameter));

        assertTrue(declared.get("repository").isRequired());
        assertTrue(declared.get("repository").isPath());
        assertFalse(declared.get("timeout").isRequired());
        assertFalse(declared.get("timeout").isPath());
        assertTrue(declared.get("master_timeout").isDeprecated());
        assertThat(
            declared.keySet(),
            equalTo(new HashSet<>(Arrays.asList("repository", "timeout", "cluster_manager_timeout", "master_timeout")))
        );
    }
}
