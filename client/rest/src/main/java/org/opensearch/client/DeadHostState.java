final class DeadHostState implements Comparable<DeadHostState> {

    private static final long MIN_TIMEOUT_NANOS = TimeUnit.MINUTES.toNanos(1);
    static final long MAX_TIMEOUT_NANOS = TimeUnit.MINUTES.toNanos(30);

    /** Supplies current time (for testing and real usage) */
    static final Supplier<Long> DEFAULT_TIME_SUPPLIER = System::nanoTime;

    private final int failedAttempts;
    private final long deadUntilNanos;
    private final Supplier<Long> timeSupplier;

    /**
     * Creates the initial dead state after the first failure.
     */
    DeadHostState(Supplier<Long> timeSupplier) {
        long now = timeSupplier.get();
        this.failedAttempts = 1;
        this.deadUntilNanos = now + MIN_TIMEOUT_NANOS;
        this.timeSupplier = timeSupplier;
    }

    /**
     * Creates an updated dead host state using exponential backoff strategy.
     */
    DeadHostState(DeadHostState previous) {
        long now = previous.timeSupplier.get();
        this.timeSupplier = previous.timeSupplier;

        int attempts = previous.failedAttempts + 1;
        this.failedAttempts = attempts;

        long backoff = calculateBackoff(previous.failedAttempts);
        this.deadUntilNanos = now + backoff;
    }

    /**
     * Calculates exponential backoff (bounded between 1 minute and 30 minutes).
     */
    private long calculateBackoff(int previousAttempts) {
        // Exponential backoff: grows moderately with attempts
        double exponent = previousAttempts * 0.5 - 1;
        long calculated = (long) (MIN_TIMEOUT_NANOS * 2 * Math.pow(2, exponent));

        return Math.min(calculated, MAX_TIMEOUT_NANOS);
    }

    /** Returns true if it's time to retry this host. */
    boolean shallBeRetried() {
        return timeSupplier.get() >= deadUntilNanos;
    }

    long getDeadUntilNanos() {
        return deadUntilNanos;
    }

    int getFailedAttempts() {
        return failedAttempts;
    }

    @Override
    public int compareTo(DeadHostState other) {
        if (this.timeSupplier != other.timeSupplier) {
            throw new IllegalArgumentException(
                "Cannot compare DeadHostStates using different time suppliers."
            );
        }
        return Long.compare(this.deadUntilNanos, other.deadUntilNanos);
    }

    @Override
    public String toString() {
        return "DeadHostState{" +
            "failedAttempts=" + failedAttempts +
            ", deadUntilNanos=" + deadUntilNanos +
            '}';
    }
}
