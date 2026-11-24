package io.github.robwin.service;


import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpServerErrorException;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This Service shows how to combine the Retry and CircuitBreaker annotations for a sanctions screening backend
 * - failures to simulate: errors now and then, concentrated in watchlist refresh windows
 *
 * <p>Simulates the sanctions / watchlist screening engine that every outgoing payment's beneficiary must
 * clear before the payment is released. Several times an hour the engine reloads its watchlists, and for a
 * couple of minutes most calls fail with HTTP 503 ("watchlist reload in progress"). Outside those windows
 * about 1% of calls fail.</p>
 *
 * <p>This is the only dependency with a FAIL-CLOSED fallback. If the payment can't be screened, it is HELD
 * for compliance review; it is never released unscreened. Because of that, the breaker is the strictest one
 * (failureRateThreshold 25%), and it is also the only one with a @Retry: a 503 during a reload is usually
 * transient, and holding a payment for manual review is expensive, so two quick retries are worth it.</p>
 *
 * <p>Retry and CircuitBreaker interplay: Retry is the outer aspect, so each attempt goes through the breaker
 * and is recorded separately. During a reload window, retries multiply the failed calls the breaker sees,
 * so it opens quickly, and then CallNotPermittedException (not in retryExceptions) stops the retries from
 * adding load to a struggling engine. The fallback is declared on @Retry, the outermost aspect; a fallback
 * on @CircuitBreaker would turn every error into HELD before Retry could see it, so nothing would be retried.</p>
 *
 * <p>A potential watchlist match is a business outcome, not a failure: the call succeeds and the payment is
 * held for an analyst. All watchlist entries and beneficiary names here are fictional.</p>
 *
 * <p>Modes (property {@code sanctions-screening.simulation.mode}, or {@link #setMode} at runtime):</p>
 * <ul>
 *   <li>{@code HEALTHY}   - 1% errors, no reload windows</li>
 *   <li>{@code SCHEDULED} - watchlist reload window at the end of every cycle (default: the last 2 minutes
 *       of every 15)</li>
 *   <li>{@code DOWN}      - every call fails</li>
 * </ul>
 */
@Component(value = "sanctionsScreeningService")
public class SactionsScreeningService {

    public static final String SANCTIONS_SCREENING = "sanctions-screening";

    public enum ScreeningMode { HEALTHY, SCHEDULED, DOWN }

    private static final Logger LOG = LoggerFactory.getLogger(SactionsScreeningService.class);

    private static final long SCREEN_MIN_MS = 30;
    private static final long SCREEN_MAX_MS = 150;
    private static final long ERROR_MIN_MS = 10;
    private static final long ERROR_MAX_MS = 60;

    /** Fictional watchlist. A beneficiary sharing two or more name words with an entry is a potential match. */
    private static final List<String> WATCHLIST = List.of(
            "OBSIDIAN HARBOR TRADING",
            "NORTHWIND SHELL HOLDINGS",
            "CRIMSON DELTA LOGISTICS",
            "IRONGATE MARITIME SERVICES",
            "SILVERFERN EXPORT COMPANY");

    /** Fictional near-variants of watchlist entries, used to generate potential matches. */
    private static final List<String> NEAR_MATCHES = List.of(
            "OBSIDIAN HARBOUR TRADING LTD",
            "NORTHWIND SHELL HOLDINGS SA",
            "CRIMSON DELTA LOGISTICS GMBH",
            "IRONGATE MARITIME PTE");

    /** Fictional, benign beneficiaries. */
    private static final List<String> BENIGN_BENEFICIARIES = List.of(
            "BLUE MEADOW FOODS GMBH", "LARKSPUR ANALYTICS LTD", "TIDEWATER CERAMICS SA",
            "MAPLEWOOD MEDICAL SUPPLY", "SUNRISE TEXTILES PVT LTD", "HARBORLIGHT SOFTWARE PTE",
            "COPPERLEAF ENGINEERING", "WILLOWBROOK AGRICULTURE", "QUARTZLINE OPTICS KK");

    private static final List<String> COUNTRIES = List.of("DE", "GB", "JP", "CA", "MX", "IN", "SG", "BR", "FR");

    // Words that don't count towards a name match
    private static final Set<String> NOISE_WORDS = Set.of(
            "LTD", "SA", "GMBH", "PTE", "PVT", "KK", "INC", "LLC", "CO", "COMPANY", "THE", "AND");

    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final Duration cycleLength;
    private final Duration refreshWindow;
    private final double refreshErrorProbability;
    private final double baselineErrorProbability;
    private final double potentialMatchProbability;
    private volatile ScreeningMode mode;

    /** Attempts per payment, so the response can show how many retries it took. */
    private final Map<String, AtomicInteger> attemptsByPayment = new ConcurrentHashMap<>();

    private final AtomicLong clearedCount = new AtomicLong();
    private final AtomicLong potentialMatchCount = new AtomicLong();
    private final AtomicLong heldUnscreenedCount = new AtomicLong();

    public SactionsScreeningService(
            CircuitBreakerRegistry circuitBreakerRegistry,
            @Value("${sanctions-screening.simulation.mode:SCHEDULED}") ScreeningMode mode,
            @Value("${sanctions-screening.simulation.cycle-length:15m}") Duration cycleLength,
            @Value("${sanctions-screening.simulation.refresh-window:2m}") Duration refreshWindow,
            @Value("${sanctions-screening.simulation.refresh-error-probability:0.7}") double refreshErrorProbability,
            @Value("${sanctions-screening.simulation.baseline-error-probability:0.01}") double baselineErrorProbability,
            @Value("${sanctions-screening.simulation.potential-match-probability:0.03}") double potentialMatchProbability) {
        if (refreshWindow.compareTo(cycleLength) > 0) {
            throw new IllegalArgumentException("sanctions-screening.simulation.refresh-window must not exceed cycle-length");
        }
        this.circuitBreakerRegistry = circuitBreakerRegistry;
        this.mode = mode;
        this.cycleLength = cycleLength;
        this.refreshWindow = refreshWindow;
        this.refreshErrorProbability = refreshErrorProbability;
        this.baselineErrorProbability = baselineErrorProbability;
        this.potentialMatchProbability = potentialMatchProbability;
        LOG.info("Sanctions screening simulation: mode={}, cycle={}, refreshWindow={}, refreshErrorProbability={}",
                mode, cycleLength, refreshWindow, refreshErrorProbability);
    }

    /**
     * Screens a payment's beneficiary against the watchlist. Returns CLEAR (release) or POTENTIAL_MATCH (hold
     * for an analyst); if the engine can't be reached, the fallback holds the payment unscreened.
     */
    @Retry(name = SANCTIONS_SCREENING, fallbackMethod = "failClosedFallback")
    @CircuitBreaker(name = SANCTIONS_SCREENING)
    public Mono<String> screen(String paymentId, String beneficiaryName, String beneficiaryCountry) {
        return Mono.defer(() -> {
            int attempt = attemptsByPayment.computeIfAbsent(paymentId, id -> new AtomicInteger()).incrementAndGet();
            ThreadLocalRandom random = ThreadLocalRandom.current();
            if (random.nextDouble() < currentErrorProbability()) {
                return Mono.delay(Duration.ofMillis(random.nextLong(ERROR_MIN_MS, ERROR_MAX_MS + 1)))
                        .then(Mono.error(randomEngineError()));
            }
            long latencyMs = random.nextLong(SCREEN_MIN_MS, SCREEN_MAX_MS + 1);
            return Mono.delay(Duration.ofMillis(latencyMs)).map(tick -> {
                attemptsByPayment.remove(paymentId);
                String matchedEntry = findMatch(beneficiaryName);
                String result;
                String action;
                if (matchedEntry == null) {
                    clearedCount.incrementAndGet();
                    result = "CLEAR";
                    action = "RELEASE";
                } else {
                    potentialMatchCount.incrementAndGet();
                    result = "POTENTIAL_MATCH matchedEntry=\"" + matchedEntry + "\"";
                    action = "HOLD_FOR_ANALYST";
                }
                return String.format("SCREENED payment=%s beneficiary=\"%s\" country=%s result=%s action=%s list=%s attempts=%d latencyMs=%d",
                        paymentId, beneficiaryName, beneficiaryCountry, result, action, listVersion(), attempt, latencyMs);
            });
        });
    }

    /**
     * FAIL CLOSED: the engine kept failing after the retries, or the breaker is OPEN. The payment is held
     * for compliance review; it is never released without a screen.
     */
    private Mono<String> failClosedFallback(String paymentId, String beneficiaryName, String beneficiaryCountry,
                                            Exception ex) {
        AtomicInteger attempts = attemptsByPayment.remove(paymentId);
        String reason = ex instanceof CallNotPermittedException ? "sanctions-circuit-open" : "screening-unavailable";
        heldUnscreenedCount.incrementAndGet();
        return Mono.just(String.format(
                "HELD payment=%s beneficiary=\"%s\" country=%s result=UNSCREENED action=HOLD_FOR_COMPLIANCE attempts=%d reason=%s",
                paymentId, beneficiaryName, beneficiaryCountry, attempts == null ? 0 : attempts.get(), reason));
    }

    // ---- simulation helpers -------------------------------------------------------------------------

    public static String newPaymentId() {
        return "PAY-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase(Locale.ROOT);
    }

    /** Mostly benign beneficiaries; a few near-variants of watchlist entries, at potential-match-probability. */
    public String randomBeneficiary() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        return random.nextDouble() < potentialMatchProbability
                ? NEAR_MATCHES.get(random.nextInt(NEAR_MATCHES.size()))
                : BENIGN_BENEFICIARIES.get(random.nextInt(BENIGN_BENEFICIARIES.size()));
    }

    public String randomCountry() {
        return COUNTRIES.get(ThreadLocalRandom.current().nextInt(COUNTRIES.size()));
    }

    public ScreeningMode getMode() {
        return mode;
    }

    public void setMode(ScreeningMode mode) {
        LOG.info("Sanctions screening simulation mode changed: {} -> {}", this.mode, mode);
        this.mode = mode;
    }

    public String circuitBreakerState() {
        return circuitBreakerRegistry.circuitBreaker(SANCTIONS_SCREENING).getState().name();
    }

    public long clearedCount() {
        return clearedCount.get();
    }

    public long potentialMatchCount() {
        return potentialMatchCount.get();
    }

    public long heldUnscreenedCount() {
        return heldUnscreenedCount.get();
    }

    /** True while the SCHEDULED mode is inside a watchlist reload window. */
    public boolean inListRefresh() {
        return mode == ScreeningMode.SCHEDULED && positionInCycleMs() >= cycleLength.toMillis() - refreshWindow.toMillis();
    }

    /** Seconds until the current reload window ends, or until the next one starts. */
    public long secondsToNextRefreshChange() {
        long cycleMs = cycleLength.toMillis();
        long position = positionInCycleMs();
        long refreshStart = cycleMs - refreshWindow.toMillis();
        long remainingMs = position >= refreshStart ? cycleMs - position : refreshStart - position;
        return remainingMs / 1000;
    }

    /** The watchlist version changes after every reload window, e.g. DEMO-WATCHLIST-v1965. */
    public String listVersion() {
        return "DEMO-WATCHLIST-v" + (System.currentTimeMillis() / cycleLength.toMillis()) % 10_000;
    }

    private long positionInCycleMs() {
        return System.currentTimeMillis() % cycleLength.toMillis();
    }

    private double currentErrorProbability() {
        return switch (mode) {
            case HEALTHY -> baselineErrorProbability;
            case SCHEDULED -> inListRefresh() ? refreshErrorProbability : baselineErrorProbability;
            case DOWN -> 1.0;
        };
    }

    /** Returns the watchlist entry the name shares two or more significant words with, or null. */
    private static String findMatch(String beneficiaryName) {
        Set<String> nameWords = significantWords(beneficiaryName);
        for (String entry : WATCHLIST) {
            Set<String> shared = significantWords(entry);
            shared.retainAll(nameWords);
            if (shared.size() >= 2) {
                return entry;
            }
        }
        return null;
    }

    private static Set<String> significantWords(String name) {
        Set<String> words = new HashSet<>(Arrays.asList(
                name.toUpperCase(Locale.ROOT).replaceAll("[^A-Z0-9 ]", " ").trim().split("\\s+")));
        words.removeAll(NOISE_WORDS);
        // Treat British/American spellings the same (HARBOUR/HARBOR)
        if (words.remove("HARBOUR")) {
            words.add("HARBOR");
        }
        return words;
    }

    private static Exception randomEngineError() {
        return switch (ThreadLocalRandom.current().nextInt(3)) {
            case 0, 1 -> new HttpServerErrorException(HttpStatus.SERVICE_UNAVAILABLE, "Watchlist reload in progress");
            default -> new IOException("Connection reset by sanctions screening engine");
        };
    }
}
