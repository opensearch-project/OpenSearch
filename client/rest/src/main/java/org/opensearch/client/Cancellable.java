package org.opensearch.client;

import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.core5.concurrent.CancellableDependency;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Represents an operation that can be cancelled.
 * Returned when executing async requests through {@link RestClient#performRequestAsync(Request, ResponseListener)} so that the
 * request can be cancelled if needed.
 *
 * <p>Important notes:
 * <ul>
 *   <li>Canceling the {@link CancellableDependency} will attempt to cancel the associated {@link java.util.concurrent.Future}.</li>
 *   <li>Cancelling a request does not automatically abort execution on the server side; that must be implemented per-API.</li>
 * </ul>
 */
public class Cancellable implements org.apache.hc.core5.concurrent.Cancellable {

    private static final Logger LOGGER = Logger.getLogger(Cancellable.class.getName());

    /**
     * A sentinel NO-OP instance used where cancellation is not supported.
     * Methods on this instance intentionally throw {@link UnsupportedOperationException}.
     */
    static final Cancellable NO_OP = new Cancellable(null) {
        @Override
        public synchronized boolean cancel() {
            throw new UnsupportedOperationException("NO_OP Cancellable does not support cancel()");
        }

        @Override
        void runIfNotCancelled(Runnable runnable) {
            throw new UnsupportedOperationException("NO_OP Cancellable does not support runIfNotCancelled()");
        }

        @Override
        <T> T callIfNotCancelled(Callable<T> callable) throws IOException {
            throw new UnsupportedOperationException("NO_OP Cancellable does not support callIfNotCancelled()");
        }
    };

    /**
     * Construct a Cancellable from an underlying {@link CancellableDependency}.
     * Use {@link #fromRequest(CancellableDependency)} instead of calling this constructor directly.
     */
    private final CancellableDependency httpRequest;

    private Cancellable(CancellableDependency httpRequest) {
        this.httpRequest = httpRequest;
    }

    /**
     * Factory that returns a new Cancellable bound to the given httpRequest.
     *
     * @param httpRequest non-null cancellable dependency
     * @return a new {@link Cancellable}
     * @throws NullPointerException if httpRequest is null
     */
    static Cancellable fromRequest(CancellableDependency httpRequest) {
        return new Cancellable(Objects.requireNonNull(httpRequest, "httpRequest cannot be null"));
    }

    /**
     * Cancel the on-going request associated with this instance.
     *
     * @return true if the request was cancelled, false otherwise.
     * @throws UnsupportedOperationException if this is the NO_OP instance
     */
    @Override
    public synchronized boolean cancel() {
        if (this.httpRequest == null) {
            // NO_OP overrides cancel() but keep defensive behavior just in case.
            throw new UnsupportedOperationException("Cancellation not supported for this instance");
        }
        try {
            return this.httpRequest.cancel();
        } catch (RuntimeException e) {
            // Log unexpected runtime exceptions and rethrow as unchecked to avoid hiding errors.
            LOGGER.log(Level.WARNING, "Unexpected exception while cancelling request", e);
            throw e;
        }
    }

    /**
     * Executes {@code runnable} only if the underlying request hasn't been cancelled.
     * If it has been cancelled, a {@link CancellationException} is thrown.
     *
     * This method is synchronized to coordinate with {@link #cancel()} to avoid races
     * where cancellation happens between attempts.
     *
     * @param runnable to execute if not cancelled
     * @throws CancellationException        if the request was already cancelled
     * @throws UnsupportedOperationException if this is the NO_OP instance
     */
    synchronized void runIfNotCancelled(Runnable runnable) {
        if (this.httpRequest == null) {
            throw new UnsupportedOperationException("Operation not supported on NO_OP instance");
        }
        if (this.httpRequest.isCancelled()) {
            throw newCancellationException();
        }
        try {
            runnable.run();
        } catch (RuntimeException e) {
            LOGGER.log(Level.WARNING, "Runtime exception while running retry attempt", e);
            throw e;
        }
    }

    /**
     * Same as {@link #runIfNotCancelled(Runnable)} but for {@link Callable} that returns a value
     * and may throw an {@link IOException}.
     *
     * @param callable operation to run if not cancelled
     * @param <T>      return type
     * @return callable result
     * @throws IOException                 if the callable throws an IOException or wraps another exception
     * @throws CancellationException       if the request was already cancelled
     * @throws UnsupportedOperationException if this is the NO_OP instance
     */
    synchronized <T> T callIfNotCancelled(Callable<T> callable) throws IOException {
        if (this.httpRequest == null) {
            throw new UnsupportedOperationException("Operation not supported on NO_OP instance");
        }
        if (this.httpRequest.isCancelled()) {
            throw newCancellationException();
        }
        try {
            return callable.call();
        } catch (final IOException ex) {
            throw ex;
        } catch (final Exception ex) {
            // Wrap checked or unchecked exceptions other than IOException
            throw new IOException("Unexpected error executing callable", ex);
        } catch (final Error err) {
            // Errors should propagate but log for observability
            LOGGER.log(Level.SEVERE, "Error while executing callable", err);
            throw err;
        }
    }

    static CancellationException newCancellationException() {
        return new CancellationException("request was cancelled");
    }
}
