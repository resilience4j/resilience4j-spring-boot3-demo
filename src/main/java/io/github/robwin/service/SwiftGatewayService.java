package io.github.robwin.service;


import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import io.github.robwin.exception.BusinessException;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpServerErrorException;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

/**
 * This Service shows how to use the CircuitBreaker annotation, and the programmatic CircuitBreakerOperator,
 * for a SWIFT gateway backend
 * - failures to simulate: bursts of errors
 *
 * <p>Simulates the bank's gateway to the SWIFT network for cross-border payments (pacs.008 messages).
 * The gateway is normally reliable, but now and then it has a burst of errors (HTTP 503/502 or dropped
 * connections) lasting a minute or two. Bursts start at random times, unlike the clock-based cycles of the
 * ledger and fraud simulations, so the "swift-gateway" breaker opens at irregular intervals.</p>
 *
 * <p>Gateway modes (property {@code swift-gateway.simulation.mode}, or {@link #setMode} at runtime):</p>
 * <ul>
 *   <li>{@code HEALTHY} - no transient errors</li>
 *   <li>{@code BURSTY}  - a small baseline error rate, plus random error bursts (default)</li>
 *   <li>{@code OUTAGE}  - every call fails with a transient error</li>
 * </ul>
 *
 * <p>Three kinds of outcome feed three different circuit breaker metrics:</p>
 * <ul>
 *   <li>ACK - the gateway accepted the message (successful call)</li>
 *   <li>Transient error (HttpServerErrorException / IOException) - recorded as a failure</li>
 *   <li>Validation reject (BusinessException), e.g. a malformed beneficiary BIC - IGNORED by the breaker,
 *       because a bad message says nothing about the gateway's health. Returned to the caller as HTTP 422.</li>
 * </ul>
 *
 * <p>Fallback ("store-and-forward through an outbox"): a payment that hits a transient error, or arrives
 * while the breaker is OPEN, is written to an outbox and the caller gets QUEUED instead of an error.
 * A background loop replays the outbox every few seconds, but only while the breaker is CLOSED, sending each
 * message through the same "swift-gateway" breaker with the programmatic CircuitBreakerOperator. Each message
 * keeps its UETR (unique end-to-end transaction reference), so a replay can't create a duplicate payment.</p>
 *
 * <p>There is deliberately no synchronous @Retry: the outbox is the retry mechanism, and it backs off
 * automatically while the breaker is open.</p>
 */
@Component(value = "swiftGatewayService")
public class SwiftGatewayService {

    public static final String SWIFT_GATEWAY = "swift-gateway";

    public enum GatewayMode { HEALTHY, BURSTY, OUTAGE }

    /** A cross-border payment message. The UETR is a UUID that identifies the payment end to end. */
    public record SwiftPayment(String uetr, String accountId, BigDecimal amount, String currency,
                               String beneficiaryBic) {
        public static SwiftPayment of(String accountId, BigDecimal amount, String currency, String beneficiaryBic) {
            return new SwiftPayment(UUID.randomUUID().toString(), accountId, amount, currency, beneficiaryBic);
        }
    }

    private record OutboxEntry(SwiftPayment payment, int attempts, Instant queuedAt) {
        OutboxEntry retried() {
            return new OutboxEntry(payment, attempts + 1, queuedAt);
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(SwiftGatewayService.class);

    private static final String MESSAGE_TYPE = "pacs.008";

    // BIC: 4-letter institution, 2-letter country, 2-char location, optional 3-char branch
    private static final Pattern BIC_PATTERN = Pattern.compile("[A-Z]{4}[A-Z]{2}[A-Z0-9]{2}([A-Z0-9]{3})?");

    private static final long ACK_MIN_MS = 150;
    private static final long ACK_MAX_MS = 600;
    private static final long ERROR_MIN_MS = 50;
    private static final long ERROR_MAX_MS = 300;

    private static final Duration MIN_GAP_BETWEEN_BURSTS = Duration.ofSeconds(60);
    private static final int REPLAY_BATCH_SIZE = 5;

    private static final List<String> DEMO_ACCOUNTS = List.of(
            "CHK-100245", "CHK-100877", "SAV-200113", "BRK-300042");
    private static final List<String> CURRENCIES = List.of("EUR", "GBP", "JPY", "CAD", "MXN", "INR", "SGD");
    // Fictional demo BICs (DEMO + country + location)
    private static final List<String> DEMO_BICS = List.of(
            "DEMODEFF", "DEMOGB2L", "DEMOJPJT", "DEMOCATT", "DEMOMXMM", "DEMOINBB", "DEMOSGSG");

    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final Duration meanTimeBetweenBursts;
    private final Duration burstMin;
    private final Duration burstMax;
    private final double burstErrorProbability;
    private final double baselineErrorProbability;
    private final double rejectProbability;
    private final Duration replayInterval;
    private volatile GatewayMode mode;

    // Burst schedule for BURSTY mode. Guarded by "this".
    private long burstEndMs;
    private long nextBurstStartMs;

    private final Queue<OutboxEntry> outbox = new ConcurrentLinkedQueue<>();
    private final AtomicLong replayedCount = new AtomicLong();
    private final AtomicLong droppedOnReplayCount = new AtomicLong();
    private Disposable replayLoop;

    public SwiftGatewayService(
            CircuitBreakerRegistry circuitBreakerRegistry,
            @Value("${swift-gateway.simulation.mode:BURSTY}") GatewayMode mode,
            @Value("${swift-gateway.simulation.mean-time-between-bursts:4m}") Duration meanTimeBetweenBursts,
            @Value("${swift-gateway.simulation.burst-min:60s}") Duration burstMin,
            @Value("${swift-gateway.simulation.burst-max:150s}") Duration burstMax,
            @Value("${swift-gateway.simulation.burst-error-probability:0.85}") double burstErrorProbability,
            @Value("${swift-gateway.simulation.baseline-error-probability:0.02}") double baselineErrorProbability,
            @Value("${swift-gateway.simulation.reject-probability:0.03}") double rejectProbability,
            @Value("${swift-gateway.simulation.outbox-replay-interval:10s}") Duration replayInterval) {
        if (burstMin.compareTo(burstMax) > 0) {
            throw new IllegalArgumentException("swift-gateway.simulation.burst-min must not exceed burst-max");
        }
        this.circuitBreakerRegistry = circuitBreakerRegistry;
        this.mode = mode;
        this.meanTimeBetweenBursts = meanTimeBetweenBursts;
        this.burstMin = burstMin;
        this.burstMax = burstMax;
        this.burstErrorProbability = burstErrorProbability;
        this.baselineErrorProbability = baselineErrorProbability;
        this.rejectProbability = rejectProbability;
        this.replayInterval = replayInterval;
        this.nextBurstStartMs = System.currentTimeMillis() + nextGapMs();
        LOG.info("SWIFT gateway simulation: mode={}, meanTimeBetweenBursts={}, burst={}..{}, burstErrorProbability={}",
                mode, meanTimeBetweenBursts, burstMin, burstMax, burstErrorProbability);
    }

    /** Sends a payment message to the SWIFT gateway. Transient errors and an OPEN breaker queue it in the outbox. */
    @io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker(name = SWIFT_GATEWAY, fallbackMethod = "outboxFallback")
    public Mono<String> sendPayment(SwiftPayment payment) {
        return transmit(payment);
    }

    /** Gateway returned a 5xx: recorded as a failure, payment queued for replay. */
    private Mono<String> outboxFallback(SwiftPayment payment, HttpServerErrorException ex) {
        return queue(payment, "gateway-error-" + ex.getStatusCode().value());
    }

    /** Connection to the gateway dropped: recorded as a failure, payment queued for replay. */
    private Mono<String> outboxFallback(SwiftPayment payment, IOException ex) {
        return queue(payment, "gateway-connection-error");
    }

    /** Breaker OPEN: the gateway is not called at all, payment queued for replay. */
    private Mono<String> outboxFallback(SwiftPayment payment, CallNotPermittedException ex) {
        return queue(payment, "swift-circuit-open");
    }

    // BusinessException (validation reject) has no fallback on purpose: it propagates to the caller as a 422.

    private Mono<String> queue(SwiftPayment payment, String reason) {
        outbox.add(new OutboxEntry(payment, 1, Instant.now()));
        return Mono.just(String.format(
                "QUEUED uetr=%s msgType=%s account=%s amount=%s %s beneficiaryBic=%s outboxDepth=%d reason=%s",
                payment.uetr(), MESSAGE_TYPE, payment.accountId(), payment.amount(), payment.currency(),
                payment.beneficiaryBic(), outbox.size(), reason));
    }

    /** The simulated gateway call itself, shared by live sends and outbox replays. */
    private Mono<String> transmit(SwiftPayment payment) {
        return Mono.defer(() -> {
            if (payment.beneficiaryBic() == null || !BIC_PATTERN.matcher(payment.beneficiaryBic()).matches()) {
                return Mono.error(new BusinessException("Rejected by gateway validation: malformed beneficiary BIC '"
                        + payment.beneficiaryBic() + "' uetr=" + payment.uetr()));
            }
            ThreadLocalRandom random = ThreadLocalRandom.current();
            if (random.nextDouble() < currentErrorProbability()) {
                return Mono.delay(Duration.ofMillis(random.nextLong(ERROR_MIN_MS, ERROR_MAX_MS + 1)))
                        .then(Mono.error(randomTransientError()));
            }
            if (random.nextDouble() < rejectProbability) {
                return Mono.delay(Duration.ofMillis(random.nextLong(ERROR_MIN_MS, ERROR_MAX_MS + 1)))
                        .then(Mono.error(new BusinessException(
                                "Rejected by gateway validation: beneficiary account format uetr=" + payment.uetr())));
            }
            long latencyMs = random.nextLong(ACK_MIN_MS, ACK_MAX_MS + 1);
            return Mono.delay(Duration.ofMillis(latencyMs)).map(tick -> String.format(
                    "ACK uetr=%s msgType=%s account=%s amount=%s %s beneficiaryBic=%s latencyMs=%d",
                    payment.uetr(), MESSAGE_TYPE, payment.accountId(), payment.amount(), payment.currency(),
                    payment.beneficiaryBic(), latencyMs));
        });
    }

    // ---- outbox replay ------------------------------------------------------------------------------

    @PostConstruct
    void startOutboxReplay() {
        replayLoop = Flux.interval(replayInterval, replayInterval)
                .onBackpressureDrop()
                .concatMap(tick -> replayBatch().onErrorResume(e -> {
                    LOG.warn("Outbox replay batch failed", e);
                    return Mono.empty();
                }), 1)
                .subscribe();
    }

    @PreDestroy
    void stopOutboxReplay() {
        if (replayLoop != null) {
            replayLoop.dispose();
        }
    }

    /**
     * Replays up to REPLAY_BATCH_SIZE queued payments, only while the breaker is CLOSED. Each replay goes
     * through the "swift-gateway" breaker, so replays count in its metrics and stop if it opens mid-batch.
     */
    private Mono<Void> replayBatch() {
        io.github.resilience4j.circuitbreaker.CircuitBreaker circuitBreaker =
                circuitBreakerRegistry.circuitBreaker(SWIFT_GATEWAY);
        if (outbox.isEmpty()
                || circuitBreaker.getState() != io.github.resilience4j.circuitbreaker.CircuitBreaker.State.CLOSED) {
            return Mono.empty();
        }
        List<OutboxEntry> batch = new ArrayList<>(REPLAY_BATCH_SIZE);
        for (int i = 0; i < REPLAY_BATCH_SIZE; i++) {
            OutboxEntry entry = outbox.poll();
            if (entry == null) {
                break;
            }
            batch.add(entry);
        }
        return Flux.fromIterable(batch)
                .concatMap(entry -> transmit(entry.payment())
                        .transformDeferred(CircuitBreakerOperator.of(circuitBreaker))
                        .doOnNext(ack -> {
                            replayedCount.incrementAndGet();
                            LOG.info("Outbox replay released uetr={} after {} attempt(s), queued {}s ago",
                                    entry.payment().uetr(), entry.attempts() + 1,
                                    Duration.between(entry.queuedAt(), Instant.now()).toSeconds());
                        })
                        .onErrorResume(BusinessException.class, ex -> {
                            // A rejected message will never succeed: drop it rather than replaying it forever
                            droppedOnReplayCount.incrementAndGet();
                            LOG.warn("Outbox replay dropped uetr={}: {}", entry.payment().uetr(), ex.getMessage());
                            return Mono.empty();
                        })
                        .onErrorResume(ex -> {
                            outbox.add(entry.retried());
                            LOG.debug("Outbox replay failed uetr={}, requeued: {}", entry.payment().uetr(), ex.toString());
                            return Mono.empty();
                        }))
                .then();
    }

    // ---- simulation helpers -------------------------------------------------------------------------

    public String randomDemoAccount() {
        return DEMO_ACCOUNTS.get(ThreadLocalRandom.current().nextInt(DEMO_ACCOUNTS.size()));
    }

    /** Random cross-border amount between 1,000.00 and 250,000.00. */
    public BigDecimal randomAmount() {
        double value = ThreadLocalRandom.current().nextDouble(1_000.0, 250_000.0);
        return BigDecimal.valueOf(value).setScale(2, RoundingMode.HALF_UP);
    }

    public String randomCurrency() {
        return CURRENCIES.get(ThreadLocalRandom.current().nextInt(CURRENCIES.size()));
    }

    public String randomBeneficiaryBic() {
        return DEMO_BICS.get(ThreadLocalRandom.current().nextInt(DEMO_BICS.size()));
    }

    public GatewayMode getMode() {
        return mode;
    }

    public void setMode(GatewayMode mode) {
        LOG.info("SWIFT gateway simulation mode changed: {} -> {}", this.mode, mode);
        this.mode = mode;
    }

    public String circuitBreakerState() {
        return circuitBreakerRegistry.circuitBreaker(SWIFT_GATEWAY).getState().name();
    }

    public int outboxDepth() {
        return outbox.size();
    }

    public long replayedCount() {
        return replayedCount.get();
    }

    public long droppedOnReplayCount() {
        return droppedOnReplayCount.get();
    }

    /** True while an error burst is in progress (always true in OUTAGE, never in HEALTHY). */
    public synchronized boolean inBurst() {
        return switch (mode) {
            case HEALTHY -> false;
            case OUTAGE -> true;
            case BURSTY -> {
                long now = System.currentTimeMillis();
                if (now < burstEndMs) {
                    yield true;
                }
                if (now >= nextBurstStartMs) {
                    long burstLengthMs = ThreadLocalRandom.current()
                            .nextLong(burstMin.toMillis(), burstMax.toMillis() + 1);
                    burstEndMs = now + burstLengthMs;
                    nextBurstStartMs = burstEndMs + nextGapMs();
                    LOG.info("SWIFT gateway error burst started, lasting {}s", burstLengthMs / 1000);
                    yield true;
                }
                yield false;
            }
        };
    }

    /** Seconds left in the current burst, or seconds until the next one starts (BURSTY mode). */
    public synchronized long secondsToNextBurstChange() {
        long now = System.currentTimeMillis();
        long target = now < burstEndMs ? burstEndMs : nextBurstStartMs;
        return Math.max(0, (target - now) / 1000);
    }

    private double currentErrorProbability() {
        return switch (mode) {
            case HEALTHY -> 0.0;
            case OUTAGE -> 1.0;
            case BURSTY -> inBurst() ? burstErrorProbability : baselineErrorProbability;
        };
    }

    /** Random gap between bursts: exponentially distributed around the mean, with a floor. */
    private long nextGapMs() {
        double u = ThreadLocalRandom.current().nextDouble();
        long gap = Math.round(-meanTimeBetweenBursts.toMillis() * Math.log(1.0 - u));
        return Math.max(MIN_GAP_BETWEEN_BURSTS.toMillis(), gap);
    }

    private static Exception randomTransientError() {
        return switch (ThreadLocalRandom.current().nextInt(3)) {
            case 0 -> new HttpServerErrorException(HttpStatus.SERVICE_UNAVAILABLE, "SWIFT gateway unavailable");
            case 1 -> new HttpServerErrorException(HttpStatus.BAD_GATEWAY, "SWIFT gateway upstream error");
            default -> new IOException("Connection reset by SWIFT gateway");
        };
    }
}
