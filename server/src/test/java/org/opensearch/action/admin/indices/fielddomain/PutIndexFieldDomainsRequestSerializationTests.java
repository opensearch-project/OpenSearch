/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.fielddomain;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.Index;
import org.opensearch.index.fielddomain.DateRangeFieldDomain;
import org.opensearch.index.fielddomain.FieldDomain;
import org.opensearch.index.fielddomain.IndexFieldDomainMetadata;
import org.opensearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Wire serialization and equality tests for {@link PutIndexFieldDomainsRequest}.
 */
public class PutIndexFieldDomainsRequestSerializationTests extends AbstractWireSerializingTestCase<PutIndexFieldDomainsRequest> {
    @Override
    protected PutIndexFieldDomainsRequest createTestInstance() {
        return randomRequest();
    }

    @Override
    protected Writeable.Reader<PutIndexFieldDomainsRequest> instanceReader() {
        return PutIndexFieldDomainsRequest::new;
    }

    @Override
    protected PutIndexFieldDomainsRequest mutateInstance(PutIndexFieldDomainsRequest instance) throws IOException {
        PutIndexFieldDomainsRequest mutation = copyRequest(instance);
        Supplier<TimeValue> timeoutSupplier = () -> TimeValue.parseTimeValue(randomTimeValue(), "field_domain_test");
        List<Runnable> mutators = new ArrayList<>();
        mutators.add(
            () -> mutation.targetIndex(new Index(instance.targetIndex().getName() + "-mutated", instance.targetIndex().getUUID()))
        );
        mutators.add(
            () -> mutation.targetIndex(new Index(instance.targetIndex().getName(), instance.targetIndex().getUUID() + "-mutated"))
        );
        mutators.add(() -> mutation.timeout(randomValueOtherThan(instance.timeout(), timeoutSupplier)));
        mutators.add(() -> mutation.clusterManagerNodeTimeout(randomValueOtherThan(instance.clusterManagerNodeTimeout(), timeoutSupplier)));
        mutators.add(
            () -> mutation.fieldDomain(
                new DateRangeFieldDomain("mutated." + randomAlphaOfLengthBetween(4, 8), 10_000L, 20_000L, true, "test")
            )
        );
        randomFrom(mutators).run();
        return mutation;
    }

    private static PutIndexFieldDomainsRequest randomRequest() {
        PutIndexFieldDomainsRequest request = new PutIndexFieldDomainsRequest(
            new Index("logs-" + randomAlphaOfLengthBetween(4, 8), randomAlphaOfLengthBetween(8, 16))
        );
        request.fieldDomains(randomDomains());
        request.timeout(randomPositiveTimeValue());
        request.clusterManagerNodeTimeout(randomPositiveTimeValue());
        return request;
    }

    private static PutIndexFieldDomainsRequest copyRequest(PutIndexFieldDomainsRequest request) {
        PutIndexFieldDomainsRequest copy = new PutIndexFieldDomainsRequest(request.targetIndex());
        copy.fieldDomains(IndexFieldDomainMetadata.getInstance().validateAndParseCustomData(request.fieldDomainCustomData()));
        copy.timeout(request.timeout());
        copy.clusterManagerNodeTimeout(request.clusterManagerNodeTimeout());
        return copy;
    }

    private static List<FieldDomain> randomDomains() {
        int domainCount = randomIntBetween(1, 3);
        List<FieldDomain> domains = new ArrayList<>(domainCount);
        for (int i = 0; i < domainCount; i++) {
            long min = randomLongBetween(0L, 1_000_000L);
            long max = randomLongBetween(min, min + 1_000_000L);
            domains.add(new DateRangeFieldDomain("field." + i, min, max, randomBoolean(), "test"));
        }
        return domains;
    }
}
