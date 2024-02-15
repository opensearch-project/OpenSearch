/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.cluster.metadata.View.Target;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.AbstractSerializingTestCase;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class ViewTests extends AbstractSerializingTestCase<View> {

    private static Set<Target> randomTargets() {
        int numTargets = randomIntBetween(1, 25);
        return new TreeSet<>(randomList(1, numTargets, () -> new View.Target(randomAlphaOfLength(8))));
    }

    private static View randomInstance() {
        final Set<Target> targets = randomTargets();
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

        MatcherAssert.assertThat(npe.getMessage(), equalTo("Name must be provided"));
    }

    public void testNullTargets() {
        final NullPointerException npe = assertThrows(NullPointerException.class, () -> new View("name", null, null, null, null));

        MatcherAssert.assertThat(npe.getMessage(), equalTo("Targets are required on a view"));
    }

    public void testNullTargetIndexPattern() {
        final NullPointerException npe = assertThrows(NullPointerException.class, () -> new View.Target((String) null));

        MatcherAssert.assertThat(npe.getMessage(), equalTo("IndexPattern is required"));
    }

    public void testDefaultValues() {
        final View view = new View("myName", null, null, null, Set.of());

        MatcherAssert.assertThat(view.getName(), equalTo("myName"));
        MatcherAssert.assertThat(view.getDescription(), equalTo(null));
        MatcherAssert.assertThat(view.getCreatedAt(), equalTo(-1L));
        MatcherAssert.assertThat(view.getModifiedAt(), equalTo(-1L));
        MatcherAssert.assertThat(view.getTargets(), empty());
    }
}
