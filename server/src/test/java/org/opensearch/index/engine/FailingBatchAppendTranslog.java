/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.index.translog.LocalTranslog;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogConfig;
import org.opensearch.index.translog.TranslogDeletionPolicy;
import org.opensearch.index.translog.TranslogFactory;
import org.opensearch.index.translog.TranslogOperationHelper;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

/**
 * A {@link LocalTranslog} whose batched {@code add(List)} fails once armed, in one of two ways:
 * <ul>
 *   <li>{@link FailureClass#TRAGIC}: as a real writer failure does -- the writer's shared tragedy holder records the
 *       exception, the translog closes itself ({@code closeOnTragicEvent}) and the exception is rethrown, so that
 *       {@code InternalTranslogManager#getTragicExceptionIfClosed()} returns this exact instance afterwards;</li>
 *   <li>{@link FailureClass#NON_TRAGIC}: the exception is thrown with the translog left open, the shape a failure
 *       takes when the engine's {@code maybeFailEngine} must NOT fail the engine.</li>
 * </ul>
 * Per-operation adds are left untouched so a test's setup is unaffected. Install it through
 * {@link #factory(AtomicReference)} as the engine config's translog factory, and arm it by setting the reference.
 */
final class FailingBatchAppendTranslog extends LocalTranslog {

    enum FailureClass {
        TRAGIC,
        NON_TRAGIC
    }

    static final class ArmedFailure {
        final IOException exception;
        final FailureClass failureClass;

        ArmedFailure(IOException exception, FailureClass failureClass) {
            this.exception = exception;
            this.failureClass = failureClass;
        }
    }

    private final AtomicReference<ArmedFailure> armedFailure;

    private FailingBatchAppendTranslog(
        TranslogConfig config,
        String translogUUID,
        TranslogDeletionPolicy deletionPolicy,
        LongSupplier globalCheckpointSupplier,
        LongSupplier primaryTermSupplier,
        LongConsumer persistedSequenceNumberConsumer,
        TranslogOperationHelper translogOperationHelper,
        AtomicReference<ArmedFailure> armedFailure
    ) throws IOException {
        super(
            config,
            translogUUID,
            deletionPolicy,
            globalCheckpointSupplier,
            primaryTermSupplier,
            persistedSequenceNumberConsumer,
            translogOperationHelper
        );
        this.armedFailure = armedFailure;
    }

    @Override
    public Location[] add(List<Operation> operations) throws IOException {
        final ArmedFailure armed = armedFailure.get();
        if (armed != null) {
            if (armed.failureClass == FailureClass.TRAGIC) {
                tragedy.setTragicException(armed.exception);
                closeOnTragicEvent(armed.exception);
            }
            throw armed.exception;
        }
        return super.add(operations);
    }

    static TranslogFactory factory(AtomicReference<ArmedFailure> armedFailure) {
        return new TranslogFactory() {
            @Override
            public Translog newTranslog(
                TranslogConfig config,
                String translogUUID,
                TranslogDeletionPolicy deletionPolicy,
                LongSupplier globalCheckpointSupplier,
                LongSupplier primaryTermSupplier,
                LongConsumer persistedSequenceNumberConsumer,
                BooleanSupplier startedPrimarySupplier
            ) throws IOException {
                return newTranslog(
                    config,
                    translogUUID,
                    deletionPolicy,
                    globalCheckpointSupplier,
                    primaryTermSupplier,
                    persistedSequenceNumberConsumer,
                    startedPrimarySupplier,
                    TranslogOperationHelper.DEFAULT
                );
            }

            @Override
            public Translog newTranslog(
                TranslogConfig config,
                String translogUUID,
                TranslogDeletionPolicy deletionPolicy,
                LongSupplier globalCheckpointSupplier,
                LongSupplier primaryTermSupplier,
                LongConsumer persistedSequenceNumberConsumer,
                BooleanSupplier startedPrimarySupplier,
                TranslogOperationHelper translogOperationHelper
            ) throws IOException {
                return new FailingBatchAppendTranslog(
                    config,
                    translogUUID,
                    deletionPolicy,
                    globalCheckpointSupplier,
                    primaryTermSupplier,
                    persistedSequenceNumberConsumer,
                    translogOperationHelper,
                    armedFailure
                );
            }
        };
    }
}
