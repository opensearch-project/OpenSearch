/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.rest;

import org.opensearch.common.unit.TimeValue;
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

    public void testRequiredStringIsMetadataAndMissingValueIsNull() {
        RestParameter<String> index = RestParameter.simpleString("index", Property.Required, Property.Path);
        FakeRestRequest request = new FakeRestRequest();

        assertThat(index.get(request), nullValue());
        assertTrue(index.isRequired());
        assertTrue(index.isPath());
    }

    public void testOptionalStringReturnsNullAndConsumesValue() {
        RestParameter<String> metric = RestParameter.simpleString("metric");
        FakeRestRequest request = new FakeRestRequest();

        assertThat(metric.get(request), nullValue());
        assertFalse(metric.isRequired());
        assertFalse(metric.isPath());

        request.params().put("metric", "docs");
        assertThat(metric.get(request), equalTo("docs"));
        assertTrue(request.consumedParams().contains("metric"));
    }

    public void testBooleanEmptyValueMeansTrue() {
        RestParameter<Boolean> local = RestParameter.boolParam("local");
        FakeRestRequest request = new FakeRestRequest();
        request.params().put("local", "");

        assertThat(local.get(request), equalTo(true));
    }

    public void testTypedRestRequestAccessorUsesActionDefaultWhenAbsent() {
        RestParameter<TimeValue> timeout = RestParameter.timeValue("timeout");
        TimeValue fallback = TimeValue.timeValueSeconds(30);
        FakeRestRequest request = new FakeRestRequest();

        assertThat(request.param(timeout, fallback), equalTo(fallback));

        request.params().put("timeout", "5s");
        assertThat(request.param(timeout, fallback), equalTo(new TimeValue(5, TimeUnit.SECONDS)));
    }

    public void testIntParseFailure() {
        RestParameter<Integer> size = RestParameter.intParam("size");
        FakeRestRequest request = new FakeRestRequest();
        request.params().put("size", "nope");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> size.get(request));
        assertThat(e.getMessage(), containsString("failed to parse int parameter [size]"));
    }

    public void testStringArraySplitsAndReturnsNullWhenAbsent() {
        RestParameter<String[]> indices = RestParameter.stringArray("index", Property.Path);
        assertThat(indices.get(new FakeRestRequest()), nullValue());

        FakeRestRequest request = new FakeRestRequest();
        request.params().put("index", "a,b");
        assertThat(indices.get(request), equalTo(new String[] { "a", "b" }));
    }

    public void testByteSizeAndEnum() {
        RestParameter<ByteSizeValue> size = RestParameter.byteSize("max_size");
        RestParameter<TimeUnit> unit = RestParameter.enumParam("unit", TimeUnit.class, Property.Required);
        FakeRestRequest request = new FakeRestRequest();
        request.params().put("max_size", "1b");
        request.params().put("unit", "SECONDS");

        assertThat(size.get(request), equalTo(new ByteSizeValue(1)));
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
