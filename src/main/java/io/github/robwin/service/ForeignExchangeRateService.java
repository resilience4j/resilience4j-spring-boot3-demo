package io.github.robwin.service;


import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.robwin.exception.InvalidQuoteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpServerErrorException;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This Service shows how to use the CircuitBreaker annotation for a foreign exchange rate backend
 * - failures to simulate: errors now and then, plus bad data (stale or off-market quotes)
 *
 * <p>Simulates the market data provider that prices cross-border payments (USD to EUR, GBP, JPY, ...).
 * Its distinguishing failure is that a 200 OK isn't always a success. Now and then the provider's feed
 * freezes: it keeps answering, but with the same quote and an ever older timestamp. Every quote is
 * validated, and a quote older than {@code max-quote-age} (stale) or more than {@code off-market-threshold}
 * away from the last good rate (off-market) throws an InvalidQuoteException. The "fx-rates" instance lists
 * that exception in its recordExceptions, so bad data opens the breaker just like HTTP errors do.</p>
 *
 * <p>Modes (property {@code fx-rates.simulation.mode}, or {@link #setMode} at runtime):</p>
 * <ul>
 *   <li>{@code HEALTHY} - live quotes, no errors</li>
 *   <li>{@code FLAKY}   - 2% HTTP errors, rare off-market quotes, and random feed freezes (default)</li>
 *   <li>{@code FROZEN}  - the feed is frozen; quotes go stale a few seconds after switching</li>
 *   <li>{@code DOWN}    - every call fails with an HTTP error</li>
 * </ul>
 *
 * <p>Fallback ("cached rate with a TTL, priced defensively"): every good quote is cached per currency.
 * When the provider fails, sends bad data, or the breaker is OPEN, the cached rate is used if it is no
 * older than {@code cache-ttl}, with a wider spread (the bank carries the risk of the market moving) and
 * marked indicative. If there is no cached rate young enough, the quote is refused and the FX payment is
 * deferred, because executing at a badly outdated rate costs real money.</p>
 *
 * <p>Rates are simulated (a random walk around fixed starting mid-rates), not real market data.</p>
 */
@Component(value = "foreignExchangeRateService")
public class ForeignExchangeRateService {

    public static final String FX_RATES = "fx-rates";

    public enum FxMode { HEALTHY, FLAKY, FROZEN, DOWN }

    private record Quote(BigDecimal mid, long quoteTimestampMs) {
    }

    private static final Logger LOG = LoggerFactory.getLogger(ForeignExchangeRateService.class);

    private static final String BASE_CURRENCY = "USD";

    private static final long QUOTE_MIN_MS = 15;
    private static final long QUOTE_MAX_MS = 90;
    private static final long ERROR_MIN_MS = 10;
    private static final long ERROR_MAX_MS = 60;

    // Spread charged on live quotes vs cached (indicative) quotes, in basis points
    private static final int LIVE_SPREAD_BPS = 25;
    private static final int CACHED_SPREAD_BPS = 75;

    // Each live quote moves the mid by up to +/- 0.03%
    private static final double RANDOM_WALK_STEP = 0.0003;

    private static final Duration MIN_GAP_BETWEEN_FREEZES = Duration.ofSeconds(90);

    /** Simulated starting mid-rates, 1 USD = x. */
    private static final Map<String, BigDecimal> STARTING_MIDS = Map.of(
            "EUR", new BigDecimal("0.9200"),
            "GBP", new BigDecimal("0.7900"),
            "JPY", new BigDecimal("150.00"),
            "CAD", new BigDecimal("1.3700"),
            "MXN", new BigDecimal("17.500"),
            "INR", new BigDecimal("83.50"),
            "SGD", new BigDecimal("1.3400"));

    private static final List<String> CURRENCIES = List.copyOf(STARTING_MIDS.keySet());

    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final Duration maxQuoteAge;
    private final double offMarketThreshold;
    private final Duration cacheTtl;
    private final double baselineErrorProbability;
    private final double offMarketProbability;
    private final Duration meanTimeBetweenFreezes;
    private final Duration freezeMin;
    private final Duration freezeMax;
    private volatile FxMode mode;

    /** The provider's current mid per currency (random walk). */
    private final Map<String, BigDecimal> providerMids = new ConcurrentHashMap<>(STARTING_MIDS);

    /** Last validated quote per currency, used by the fallback. */
    private final Map<String, Quote> lastGoodQuotes = new ConcurrentHashMap<>();

    // Feed freeze schedule. Guarded by "this".
    private long freezeStartMs = -1;
    private long freezeEndMs;
    private long nextFreezeStartMs;
    private final Map<String, Quote> frozenQuotes = new ConcurrentHashMap<>();

    private final AtomicLong liveCount = new AtomicLong();
    private final AtomicLong cachedCount = new AtomicLong();
    private final AtomicLong refusedCount = new AtomicLong();
    private final Map<String, AtomicLong> invalidQuotesByReason = new ConcurrentHashMap<>();

    public ForeignExchangeRateService(
            CircuitBreakerRegistry circuitBreakerRegistry,
            @Value("${fx-rates.simulation.mode:FLAKY}") FxMode mode,
            @Value("${fx-rates.simulation.max-quote-age:5s}") Duration maxQuoteAge,
            @Value("${fx-rates.simulation.off-market-threshold:0.03}") double offMarketThreshold,
            @Value("${fx-rates.simulation.cache-ttl:10m}") Duration cacheTtl,
            @Value("${fx-rates.simulation.baseline-error-probability:0.02}") double baselineErrorProbability,
            @Value("${fx-rates.simulation.off-market-probability:0.01}") double offMarketProbability,
            @Value("${fx-rates.simulation.mean-time-between-freezes:6m}") Duration meanTimeBetweenFreezes,
            @Value("${fx-rates.simulation.freeze-min:60s}") Duration freezeMin,
            @Value("${fx-rates.simulation.freeze-max:150s}") Duration freezeMax) {
        if (freezeMin.compareTo(freezeMax) > 0) {
            throw new IllegalArgumentException("fx-rates.simulation.freeze-min must not exceed freeze-max");
        }
        this.circuitBreakerRegistry = circuitBreakerRegistry;
        this.mode = mode;
        this.maxQuoteAge = maxQuoteAge;
        this.offMarketThreshold = offMarketThreshold;
        this.cacheTtl = cacheTtl;
        this.baselineErrorProbability = baselineErrorProbability;
        this.offMarketProbability = offMarketProbability;
        this.meanTimeBetweenFreezes = meanTimeBetweenFreezes;
        this.freezeMin = freezeMin;
        this.freezeMax = freezeMax;
        this.nextFreezeStartMs = System.currentTimeMillis() + nextGapMs();
        LOG.info("FX rate simulation: mode={}, maxQuoteAge={}, cacheTtl={}, meanTimeBetweenFreezes={}, freeze={}..{}",
                mode, maxQuoteAge, cacheTtl, meanTimeBetweenFreezes, freezeMin, freezeMax);
    }

    /** Prices a USD amount in the target currency with a live, validated quote. */
    @CircuitBreaker(name = FX_RATES, fallbackMethod = "cachedRateFallback")
    public Mono<String> quote(String currency, BigDecimal usdAmount) {
        return Mono.defer(() -> {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            if (random.nextDouble() < currentErrorProbability()) {
                return Mono.delay(Duration.ofMillis(random.nextLong(ERROR_MIN_MS, ERROR_MAX_MS + 1)))
                        .then(Mono.error(randomProviderError()));
            }
            long latencyMs = random.nextLong(QUOTE_MIN_MS, QUOTE_MAX_MS + 1);
            return Mono.delay(Duration.ofMillis(latencyMs)).map(tick -> {
                Quote quote = providerQuote(currency);
                validate(currency, quote);
                lastGoodQuotes.put(currency, quote);
                liveCount.incrementAndGet();
                long ageMs = System.currentTimeMillis() - quote.quoteTimestampMs();
                return describe(currency, quote.mid(), LIVE_SPREAD_BPS, usdAmount)
                        + String.format(" quoteAgeMs=%d source=market-data latencyMs=%d", ageMs, latencyMs);
            });
        });
    }

    /**
     * Provider error, bad quote, or breaker OPEN: price from the cached rate with a wider spread if it is
     * younger than the cache TTL; otherwise refuse, so the FX payment is deferred rather than mispriced.
     */
    private Mono<String> cachedRateFallback(String currency, BigDecimal usdAmount, Exception ex) {
        String reason = fallbackReason(ex);
        if (ex instanceof InvalidQuoteException invalid) {
            invalidQuotesByReason.computeIfAbsent(invalid.getReason(), r -> new AtomicLong()).incrementAndGet();
        }
        Quote cached = lastGoodQuotes.get(currency);
        if (cached != null) {
            long ageSeconds = (System.currentTimeMillis() - cached.quoteTimestampMs()) / 1000;
            if (ageSeconds <= cacheTtl.toSeconds()) {
                cachedCount.incrementAndGet();
                return Mono.just(describe(currency, cached.mid(), CACHED_SPREAD_BPS, usdAmount)
                        + String.format(" quoteAgeSeconds=%d source=cache indicative=true reason=%s", ageSeconds, reason));
            }
        }
        refusedCount.incrementAndGet();
        return Mono.just(String.format("RATE_UNAVAILABLE pair=%s/%s amount=%s action=DEFER_FX_PAYMENT reason=%s",
                BASE_CURRENCY, currency, usdAmount, reason));
    }

    private static String fallbackReason(Exception ex) {
        if (ex instanceof CallNotPermittedException) {
            return "fx-circuit-open";
        }
        if (ex instanceof InvalidQuoteException invalid) {
            return invalid.getReason();
        }
        return "provider-error";
    }

    /** "QUOTE pair=USD/EUR mid=0.9213 rate=0.9190 spreadBps=25 amount=1000.00 USD converted=919.00 EUR" */
    private static String describe(String currency, BigDecimal mid, int spreadBps, BigDecimal usdAmount) {
        // The customer buys the foreign currency, so the bank's rate is below mid by the spread
        BigDecimal rate = mid.multiply(BigDecimal.ONE.subtract(BigDecimal.valueOf(spreadBps, 4)))
                .setScale(mid.scale(), RoundingMode.HALF_DOWN);
        BigDecimal converted = usdAmount.multiply(rate).setScale(2, RoundingMode.HALF_DOWN);
        return String.format("QUOTE pair=%s/%s mid=%s rate=%s spreadBps=%d amount=%s %s converted=%s %s",
                BASE_CURRENCY, currency, mid, rate, spreadBps, usdAmount, BASE_CURRENCY, converted, currency);
    }

    // ---- quote generation and validation ------------------------------------------------------------

    /** What the provider sends back: a fresh random-walk quote, a frozen one, or now and then an off-market one. */
    private Quote providerQuote(String currency) {
        if (feedFrozen()) {
            return frozenQuotes.computeIfAbsent(currency,
                    c -> new Quote(providerMids.get(c), frozenSince()));
        }
        ThreadLocalRandom random = ThreadLocalRandom.current();
        BigDecimal mid = providerMids.compute(currency, (c, current) -> {
            double step = random.nextDouble(-RANDOM_WALK_STEP, RANDOM_WALK_STEP);
            return current.multiply(BigDecimal.valueOf(1.0 + step)).setScale(current.scale(), RoundingMode.HALF_EVEN);
        });
        if (mode == FxMode.FLAKY && random.nextDouble() < offMarketProbability) {
            // A bad tick: 5-10% away from the market. The provider's real mid is unchanged.
            double jump = random.nextDouble(0.05, 0.10) * (random.nextBoolean() ? 1 : -1);
            mid = mid.multiply(BigDecimal.valueOf(1.0 + jump)).setScale(mid.scale(), RoundingMode.HALF_EVEN);
        }
        return new Quote(mid, System.currentTimeMillis());
    }

    private void validate(String currency, Quote quote) {
        long ageMs = System.currentTimeMillis() - quote.quoteTimestampMs();
        if (ageMs > maxQuoteAge.toMillis()) {
            throw new InvalidQuoteException("stale-quote",
                    String.format("USD/%s quote is %dms old (max %dms)", currency, ageMs, maxQuoteAge.toMillis()));
        }
        Quote lastGood = lastGoodQuotes.get(currency);
        BigDecimal reference = lastGood != null ? lastGood.mid() : STARTING_MIDS.get(currency);
        double deviation = quote.mid().subtract(reference).abs()
                .divide(reference, 6, RoundingMode.HALF_EVEN).doubleValue();
        if (deviation > offMarketThreshold) {
            throw new InvalidQuoteException("off-market-rate",
                    String.format("USD/%s quote %s is %.1f%% away from %s", currency, quote.mid(), deviation * 100, reference));
        }
    }

    // ---- simulation helpers -------------------------------------------------------------------------

    public String randomCurrency() {
        return CURRENCIES.get(ThreadLocalRandom.current().nextInt(CURRENCIES.size()));
    }

    /** Validates and normalizes a target currency code. */
    public String normalizeCurrency(String currency) {
        String normalized = currency.trim().toUpperCase(Locale.ROOT);
        if (!STARTING_MIDS.containsKey(normalized)) {
            throw new IllegalArgumentException("Unsupported currency '" + currency + "'. Use one of " + CURRENCIES);
        }
        return normalized;
    }

    /** Random USD amount between 100.00 and 50,000.00. */
    public BigDecimal randomUsdAmount() {
        double value = ThreadLocalRandom.current().nextDouble(100.0, 50_000.0);
        return BigDecimal.valueOf(value).setScale(2, RoundingMode.HALF_UP);
    }

    public FxMode getMode() {
        return mode;
    }

    public synchronized void setMode(FxMode mode) {
        LOG.info("FX rate simulation mode changed: {} -> {}", this.mode, mode);
        this.mode = mode;
        // Start or end the freeze cleanly so FROZEN quotes age from the moment of switching
        freezeStartMs = mode == FxMode.FROZEN ? System.currentTimeMillis() : -1;
        freezeEndMs = 0;
        frozenQuotes.clear();
    }

    public String circuitBreakerState() {
        return circuitBreakerRegistry.circuitBreaker(FX_RATES).getState().name();
    }

    public long liveCount() {
        return liveCount.get();
    }

    public long cachedCount() {
        return cachedCount.get();
    }

    public long refusedCount() {
        return refusedCount.get();
    }

    public Map<String, Long> invalidQuotesByReason() {
        Map<String, Long> snapshot = new java.util.TreeMap<>();
        invalidQuotesByReason.forEach((reason, count) -> snapshot.put(reason, count.get()));
        return snapshot;
    }

    /** True while the provider's feed is frozen (always in FROZEN mode, during random freezes in FLAKY mode). */
    public synchronized boolean feedFrozen() {
        switch (mode) {
            case FROZEN:
                if (freezeStartMs < 0) {
                    freezeStartMs = System.currentTimeMillis();
                }
                return true;
            case FLAKY:
                long now = System.currentTimeMillis();
                if (now < freezeEndMs) {
                    return true;
                }
                if (now >= nextFreezeStartMs) {
                    long freezeLengthMs = ThreadLocalRandom.current()
                            .nextLong(freezeMin.toMillis(), freezeMax.toMillis() + 1);
                    freezeStartMs = now;
                    freezeEndMs = now + freezeLengthMs;
                    nextFreezeStartMs = freezeEndMs + nextGapMs();
                    frozenQuotes.clear();
                    LOG.info("FX market data feed frozen, for {}s", freezeLengthMs / 1000);
                    return true;
                }
                if (freezeStartMs >= 0) {
                    // A freeze just ended: the feed resumes with fresh quotes
                    freezeStartMs = -1;
                    frozenQuotes.clear();
                }
                return false;
            default:
                return false;
        }
    }

    /** Seconds left in the current freeze, or until the next one starts (FLAKY mode). */
    public synchronized long secondsToNextFreezeChange() {
        long now = System.currentTimeMillis();
        long target = now < freezeEndMs ? freezeEndMs : nextFreezeStartMs;
        return Math.max(0, (target - now) / 1000);
    }

    private synchronized long frozenSince() {
        return freezeStartMs >= 0 ? freezeStartMs : System.currentTimeMillis();
    }

    private double currentErrorProbability() {
        return switch (mode) {
            case HEALTHY, FROZEN -> 0.0;
            case FLAKY -> baselineErrorProbability;
            case DOWN -> 1.0;
        };
    }

    /** Random gap between feed freezes: exponentially distributed around the mean, with a floor. */
    private long nextGapMs() {
        double u = ThreadLocalRandom.current().nextDouble();
        long gap = Math.round(-meanTimeBetweenFreezes.toMillis() * Math.log(1.0 - u));
        return Math.max(MIN_GAP_BETWEEN_FREEZES.toMillis(), gap);
    }

    private static Exception randomProviderError() {
        return ThreadLocalRandom.current().nextBoolean()
                ? new HttpServerErrorException(HttpStatus.SERVICE_UNAVAILABLE, "Market data provider unavailable")
                : new IOException("Connection reset by market data provider");
    }
}
