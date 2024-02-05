/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.cluster.metadata.View.Target;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class ViewTests extends AbstractSerializingTestCase<View> {

    private static List<Target> randomTargets() {
        int numTargets = randomIntBetween(0, 128);
        return randomList(numTargets, () -> new View.Target(randomAlphaOfLength(8)));
    }

    private static View randomInstance() {
        final List<Target> targets = randomTargets();
        final String viewName = randomAlphaOfLength(10);
        final String description = randomAlphaOfLength(100);
        return new View(viewName, description, Math.abs(randomLong()), Math.abs(randomLong()), targets);
    }

    @Override
    protected View doParseInstance(XContentParser parser) throws IOException {
        return View.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<View> instanceReader() {
        return View::new;
    }

    @Override
    protected View createTestInstance() {
        return randomInstance();
    }

    public void testNullName() {
        final NullPointerException npe = assertThrows(NullPointerException.class, () -> new View(null, null, null, null, null));

        assertThat(npe.getMessage(), equalTo("Name must be provided"));
    }

    public void testNullTargets() {
        final NullPointerException npe = assertThrows(NullPointerException.class, () -> new View("name", null, null, null, null));

        assertThat(npe.getMessage(), equalTo("Targets are required on a view"));
    }

    public void testNullTargetIndexPattern() {
        final NullPointerException npe = assertThrows(NullPointerException.class, () -> new View.Target((String) null));

        assertThat(npe.getMessage(), equalTo("IndexPattern is required"));
    }

    public void testDefaultValues() {
        final View view = new View("myName", null, null, null, List.of());

        assertThat(view.getName(), equalTo("myName"));
        assertThat(view.getDescription(), equalTo(null));
        assertThat(view.getCreatedAt(), equalTo(-1L));
        assertThat(view.getModifiedAt(), equalTo(-1L));
        assertThat(view.getTargets(), empty());
    }
}
