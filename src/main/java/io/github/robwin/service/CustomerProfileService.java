package io.github.robwin.service;


import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpServerErrorException;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

/**
 * This Service shows how to use the CircuitBreaker annotation for a customer profile (KYC) backend
 * - failures to simulate: none by default; this is the healthy baseline
 *
 * <p>Simulates the customer master / KYC profile service that payment flows read to get a customer's
 * segment, KYC status, risk rating and preferred notification channel. In the default HEALTHY mode it is
 * fast and almost never fails (0.5% errors), so the "customer-profile" breaker stays CLOSED. That makes
 * it the control line in the dashboards: when the other breakers open, this one shows the app itself
 * is fine and the problem is in specific dependencies.</p>
 *
 * <p>Modes (property {@code customer-profile.simulation.mode}, or {@link #setMode} at runtime):</p>
 * <ul>
 *   <li>{@code HEALTHY}  - fast, 0.5% errors (default)</li>
 *   <li>{@code DEGRADED} - half of all calls fail; use it to show a manual incident on demand</li>
 *   <li>{@code DOWN}     - every call fails</li>
 * </ul>
 *
 * <p>Fallback ("serve from cache, then fail safe"): every successful read is cached. When the service
 * errors or the breaker is OPEN, the last cached profile is served if it is no older than
 * {@code max-cache-staleness}. If there is no usable cached copy, a default profile is returned with
 * segment and risk rating UNKNOWN, so the caller applies its most conservative limits rather than
 * assuming the customer is low risk.</p>
 *
 * <p>Only non-sensitive fields are returned, and the name is masked (first initial only), in line with
 * keeping PII out of logs and test data.</p>
 */
@Component(value = "customerProfileService")
public class CustomerProfileService {

    public static final String CUSTOMER_PROFILE = "customer-profile";

    public enum ProfileMode { HEALTHY, DEGRADED, DOWN }

    /** Non-sensitive profile fields. The name is masked before it ever leaves the service. */
    public record CustomerProfile(String customerId, String maskedName, String segment, String kycStatus,
                                  String riskRating, String preferredChannel) {
    }

    private record CachedProfile(CustomerProfile profile, Instant cachedAt) {
    }

    private static final Logger LOG = LoggerFactory.getLogger(CustomerProfileService.class);

    private static final long READ_MIN_MS = 20;
    private static final long READ_MAX_MS = 120;
    private static final long ERROR_MIN_MS = 10;
    private static final long ERROR_MAX_MS = 80;

    // 25 demo customers, so the cache warms up gradually and early fallbacks can miss it
    private static final List<String> DEMO_CUSTOMERS = IntStream.rangeClosed(10001, 10025)
            .mapToObj(n -> "CUS-" + n).toList();

    private static final List<String> FIRST_NAMES = List.of(
            "Alex", "Blake", "Casey", "Drew", "Elliot", "Frankie", "Gray", "Harper", "Jordan", "Kai");
    private static final List<String> LAST_NAMES = List.of(
            "Rivera", "Okafor", "Lindqvist", "Patel", "Moreau", "Nakamura", "Brennan", "Haddad");
    private static final List<String> SEGMENTS = List.of("RETAIL", "RETAIL", "RETAIL", "PRIORITY", "PRIVATE");
    private static final List<String> KYC_STATUSES = List.of("VERIFIED", "VERIFIED", "VERIFIED", "REFRESH_DUE");
    private static final List<String> RISK_RATINGS = List.of("LOW", "LOW", "LOW", "MEDIUM", "HIGH");
    private static final List<String> CHANNELS = List.of("PUSH", "EMAIL", "SMS");

    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final double baselineErrorProbability;
    private final double degradedErrorProbability;
    private final Duration maxCacheStaleness;
    private volatile ProfileMode mode;

    private final Map<String, CachedProfile> cache = new ConcurrentHashMap<>();
    private final AtomicLong servedFromService = new AtomicLong();
    private final AtomicLong servedFromCache = new AtomicLong();
    private final AtomicLong servedDefault = new AtomicLong();

    public CustomerProfileService(
            CircuitBreakerRegistry circuitBreakerRegistry,
            @Value("${customer-profile.simulation.mode:HEALTHY}") ProfileMode mode,
            @Value("${customer-profile.simulation.baseline-error-probability:0.005}") double baselineErrorProbability,
            @Value("${customer-profile.simulation.degraded-error-probability:0.5}") double degradedErrorProbability,
            @Value("${customer-profile.simulation.max-cache-staleness:30m}") Duration maxCacheStaleness) {
        this.circuitBreakerRegistry = circuitBreakerRegistry;
        this.mode = mode;
        this.baselineErrorProbability = baselineErrorProbability;
        this.degradedErrorProbability = degradedErrorProbability;
        this.maxCacheStaleness = maxCacheStaleness;
        LOG.info("Customer profile simulation: mode={}, baselineErrorProbability={}, degradedErrorProbability={}, maxCacheStaleness={}",
                mode, baselineErrorProbability, degradedErrorProbability, maxCacheStaleness);
    }

    /** Reads a customer's profile from the profile service, caching every successful read. */
    @CircuitBreaker(name = CUSTOMER_PROFILE, fallbackMethod = "cacheFallback")
    public Mono<String> getProfile(String customerId) {
        return Mono.defer(() -> {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            if (random.nextDouble() < currentErrorProbability()) {
                return Mono.delay(Duration.ofMillis(random.nextLong(ERROR_MIN_MS, ERROR_MAX_MS + 1)))
                        .then(Mono.error(randomServiceError()));
            }
            long latencyMs = random.nextLong(READ_MIN_MS, READ_MAX_MS + 1);
            return Mono.delay(Duration.ofMillis(latencyMs)).map(tick -> {
                CustomerProfile profile = lookUp(customerId);
                cache.put(customerId, new CachedProfile(profile, Instant.now()));
                servedFromService.incrementAndGet();
                return describe(profile) + String.format(" source=profile-service latencyMs=%d", latencyMs);
            });
        });
    }

    /** Service error or breaker OPEN: serve a recent cached profile, otherwise a fail-safe default. */
    private Mono<String> cacheFallback(String customerId, Exception ex) {
        String reason = ex instanceof CallNotPermittedException ? "customer-circuit-open" : "profile-service-error";
        CachedProfile cached = cache.get(customerId);
        if (cached != null) {
            long ageSeconds = Duration.between(cached.cachedAt(), Instant.now()).toSeconds();
            if (ageSeconds <= maxCacheStaleness.toSeconds()) {
                servedFromCache.incrementAndGet();
                return Mono.just(describe(cached.profile())
                        + String.format(" source=cache stale=true ageSeconds=%d reason=%s", ageSeconds, reason));
            }
        }
        servedDefault.incrementAndGet();
        CustomerProfile fallback = new CustomerProfile(customerId, "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN", "EMAIL");
        return Mono.just(describe(fallback) + " source=default limits=conservative reason=" + reason);
    }

    private static String describe(CustomerProfile profile) {
        return String.format("PROFILE customer=%s name=%s segment=%s kyc=%s risk=%s preferredChannel=%s",
                profile.customerId(), profile.maskedName(), profile.segment(), profile.kycStatus(),
                profile.riskRating(), profile.preferredChannel());
    }

    // ---- simulation helpers -------------------------------------------------------------------------

    public String randomDemoCustomer() {
        return DEMO_CUSTOMERS.get(ThreadLocalRandom.current().nextInt(DEMO_CUSTOMERS.size()));
    }

    public ProfileMode getMode() {
        return mode;
    }

    public void setMode(ProfileMode mode) {
        LOG.info("Customer profile simulation mode changed: {} -> {}", this.mode, mode);
        this.mode = mode;
    }

    public String circuitBreakerState() {
        return circuitBreakerRegistry.circuitBreaker(CUSTOMER_PROFILE).getState().name();
    }

    public int cachedProfileCount() {
        return cache.size();
    }

    public long servedFromServiceCount() {
        return servedFromService.get();
    }

    public long servedFromCacheCount() {
        return servedFromCache.get();
    }

    public long servedDefaultCount() {
        return servedDefault.get();
    }

    private double currentErrorProbability() {
        return switch (mode) {
            case HEALTHY -> baselineErrorProbability;
            case DEGRADED -> degradedErrorProbability;
            case DOWN -> 1.0;
        };
    }

    /** The same customer always gets the same profile (seeded by the customer id). */
    private static CustomerProfile lookUp(String customerId) {
        Random seeded = new Random(customerId.hashCode());
        String first = FIRST_NAMES.get(seeded.nextInt(FIRST_NAMES.size()));
        String last = LAST_NAMES.get(seeded.nextInt(LAST_NAMES.size()));
        return new CustomerProfile(
                customerId,
                mask(first) + " " + mask(last),
                SEGMENTS.get(seeded.nextInt(SEGMENTS.size())),
                KYC_STATUSES.get(seeded.nextInt(KYC_STATUSES.size())),
                RISK_RATINGS.get(seeded.nextInt(RISK_RATINGS.size())),
                CHANNELS.get(seeded.nextInt(CHANNELS.size())));
    }

    /** "Jordan" becomes "J*****". */
    private static String mask(String name) {
        return name.charAt(0) + "*".repeat(name.length() - 1);
    }

    private static Exception randomServiceError() {
        return switch (ThreadLocalRandom.current().nextInt(3)) {
            case 0 -> new HttpServerErrorException(HttpStatus.INTERNAL_SERVER_ERROR, "Customer profile service error");
            case 1 -> new HttpServerErrorException(HttpStatus.SERVICE_UNAVAILABLE, "Customer profile service unavailable");
            default -> new IOException("Connection reset by customer profile service");
        };
    }
}
