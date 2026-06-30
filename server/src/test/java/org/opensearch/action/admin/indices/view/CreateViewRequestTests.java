/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.view;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.hamcrest.MatcherAssert;

import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.nullValue;

public class CreateViewRequestTests extends AbstractWireSerializingTestCase<CreateViewAction.Request> {

    @Override
    protected Writeable.Reader<CreateViewAction.Request> instanceReader() {
        return CreateViewAction.Request::new;
    }

    @Override
    protected CreateViewAction.Request createTestInstance() {
        return new CreateViewAction.Request(
            randomAlphaOfLength(8),
            randomAlphaOfLength(8),
            randomList(5, () -> new CreateViewAction.Request.Target(randomAlphaOfLength(8)))
        );
    }

    public void testValidateRequest() {
        final CreateViewAction.Request request = new CreateViewAction.Request(
            "my-view",
            "this is a description",
            List.of(new CreateViewAction.Request.Target("my-indices-*"))
        );

        MatcherAssert.assertThat(request.validate(), nullValue());
    }

    public void testValidateRequestWithoutName() {
        final CreateViewAction.Request request = new CreateViewAction.Request("", null, null);
        final ActionRequestValidationException e = request.validate();

        MatcherAssert.assertThat(e.validationErrors(), contains("name cannot be empty or null", "targets cannot be empty"));
    }

    public void testSizeThresholds() {
        final String validName = randomAlphaOfLength(8);
        final String validDescription = randomAlphaOfLength(20);
        final int validTargetLength = randomIntBetween(1, 5);
        final String validIndexPattern = randomAlphaOfLength(8);

        final CreateViewAction.Request requestNameTooBig = new CreateViewAction.Request(
            randomAlphaOfLength(65),
            validDescription,
            randomList(1, validTargetLength, () -> new CreateViewAction.Request.Target(validIndexPattern))
        );
        MatcherAssert.assertThat(
            requestNameTooBig.validate().validationErrors(),
            contains("name must be less than 64 characters in length")
        );

        final CreateViewAction.Request requestDescriptionTooBig = new CreateViewAction.Request(
            validName,
            randomAlphaOfLength(257),
            randomList(1, validTargetLength, () -> new CreateViewAction.Request.Target(validIndexPattern))
        );
        MatcherAssert.assertThat(
            requestDescriptionTooBig.validate().validationErrors(),
            contains("description must be less than 256 characters in length")
        );

        final CreateViewAction.Request requestTargetsSize = new CreateViewAction.Request(
            validName,
            validDescription,
            randomList(26, 26, () -> new CreateViewAction.Request.Target(validIndexPattern))
        );
        MatcherAssert.assertThat(requestTargetsSize.validate().validationErrors(), contains("view cannot have more than 25 targets"));

        final CreateViewAction.Request requestTargetsIndexPatternSize = new CreateViewAction.Request(
            validName,
            validDescription,
            randomList(1, 1, () -> new CreateViewAction.Request.Target(randomAlphaOfLength(65)))
        );
        MatcherAssert.assertThat(
            requestTargetsIndexPatternSize.validate().validationErrors(),
            contains("target index pattern must be less than 64 characters in length")
        );
    }

}
