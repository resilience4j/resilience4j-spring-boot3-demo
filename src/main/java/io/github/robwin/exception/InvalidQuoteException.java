package io.github.robwin.exception;

/**
 * The market data provider answered (HTTP 200), but the quote can't be used: it is stale or off-market.
 * Listed in the "fx-rates" circuit breaker's recordExceptions, so a bad quote counts as a failure.
 */
public class InvalidQuoteException extends RuntimeException {

    private final String reason;

    public InvalidQuoteException(String reason, String message) {
        super(message);
        this.reason = reason;
    }

    /** Short machine-readable reason, e.g. "stale-quote" or "off-market-rate". */
    public String getReason() {
        return reason;
    }
}
