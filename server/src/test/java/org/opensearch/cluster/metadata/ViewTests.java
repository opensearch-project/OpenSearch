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
import org.opensearch.common.UUIDs;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.opensearch.cluster.DataStreamTestHelper.createTimestampField;
import static org.opensearch.cluster.metadata.DataStream.getDefaultBackingIndexName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class ViewTests extends AbstractSerializingTestCase<View> {

    private static List<Target> randomTargets() {
        int numTargets = randomIntBetween(0, 128);
        List<Target> targets = new ArrayList<>(numTargets);
        for (int i = 0; i < numTargets; i++) {
            targets.add(new Target(randomAlphaOfLength(10).toLowerCase(Locale.ROOT));
        }
        return targets;
    }

    private static View randomInstance() {
        final List<Target> targets = randomTargets();
        final String viewName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String description = randomAlphaOfLength(100).toLowerCase(Locale.ROOT);
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
        final NullPointerException npe = assertThrows(NullPointerException.class, () -> new View.Target(null));

        assertThat(npe.getMessage(), equalTo("IndexPattern is required"));
    }


    public void testDefaultValues() {
        final View view = new View("myName", null, null, null, List.of());

        assertThat(view.name, equalTo("myName"));
        assertThat(view.description, equalTo(null));
        assertThat(view.createdAt, equalTo(-1L));
        assertThat(view.modifiedAt, equalTo(-1L));
        assertThat(view.targets, empty());
    }


}
