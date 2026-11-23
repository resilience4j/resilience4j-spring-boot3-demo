package io.github.robwin.service;


import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.timelimiter.annotation.TimeLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This Service shows how to use the CircuitBreaker and TimeLimiter annotations for a fraud scoring backend
 * - failures to simulate: timeouts under load
 *
 * <p>Simulates a real-time fraud risk model that every payment must pass through before it is released.
 * The model's response time grows with the load on it. Once a score takes longer than the
 * "fraud-scoring" TimeLimiter allows (1s in application.yml), the call fails with a TimeoutException,
 * which the "fraud-scoring" circuit breaker records as a failure. Unlike the core banking ledger, which
 * trips on its slow-call rate, this breaker trips on its failure rate.</p>
 *
 * <p>Load modes (property {@code fraud-scoring.simulation.mode}, or {@link #setMode} at runtime):</p>
 * <ul>
 *   <li>{@code NORMAL}    - no background load; scores come back quickly</li>
 *   <li>{@code PEAK}      - constant peak load; most scores time out</li>
 *   <li>{@code SCHEDULED} - a recurring traffic peak (default: the last 4 minutes of every 12). Load ramps
 *       up to its peak and back down, so timeouts build gradually, the breaker opens, and then it flaps
 *       between HALF_OPEN and CLOSED as the peak drains. The 12-minute cycle is deliberately offset from
 *       the ledger's 10-minute cycle so the two breakers don't always open at the same time.</li>
 * </ul>
 *
 * <p>Fallback ("degrade to rules"): when the model times out or the breaker is OPEN, a simple rules check
 * decides instead. Small, non-wire payments are approved by rules; everything else goes to the manual
 * review queue. Payments are never auto-approved on a big-ticket or wire transfer without a model score.</p>
 *
 * <p>There is deliberately no @Retry here: retrying a model that is timing out because it is overloaded
 * only adds more load to it.</p>
 */
@Component(value = "fraudScoringService")
public class FraudScoringService {

    public static final String FRAUD_SCORING = "fraud-scoring";

    public enum LoadMode { NORMAL, PEAK, SCHEDULED }

    private static final Logger LOG = LoggerFactory.getLogger(FraudScoringService.class);

    // Model inference time with no load
    private static final long BASE_MIN_MS = 60;
    private static final long BASE_MAX_MS = 180;

    // Extra latency per unit of load. With a 1s TimeLimiter, timeouts start at a load of roughly 14.
    private static final long MS_PER_LOAD_UNIT = 60;

    // Score bands (0-999)
    private static final int REVIEW_THRESHOLD = 600;
    private static final int DECLINE_THRESHOLD = 850;

    // Rules fallback: auto-approve only small, non-wire payments
    private static final BigDecimal RULES_AUTO_APPROVE_LIMIT = new BigDecimal("250.00");

    private static final List<String> DEMO_ACCOUNTS = List.of(
            "CHK-100245", "CHK-100877", "SAV-200113", "SAV-200569", "BRK-300042", "CRD-400918");
    private static final List<String> CHANNELS = List.of("MOBILE", "WEB", "CARD", "BRANCH", "WIRE");

    private final Duration cycleLength;
    private final Duration peakWindow;
    private final int peakLoad;
    private volatile LoadMode mode;

    /** Scoring requests currently in flight; real concurrency adds to the simulated background load. */
    private final AtomicInteger inFlight = new AtomicInteger();

    /** Payments sent to manual review by the fallback. */
    private final AtomicLong manualReviewCount = new AtomicLong();

    public FraudScoringService(
            @Value("${fraud-scoring.simulation.mode:SCHEDULED}") LoadMode mode,
            @Value("${fraud-scoring.simulation.cycle-length:12m}") Duration cycleLength,
            @Value("${fraud-scoring.simulation.peak-window:4m}") Duration peakWindow,
            @Value("${fraud-scoring.simulation.peak-load:40}") int peakLoad) {
        if (peakWindow.compareTo(cycleLength) > 0) {
            throw new IllegalArgumentException("fraud-scoring.simulation.peak-window must not exceed cycle-length");
        }
        this.mode = mode;
        this.cycleLength = cycleLength;
        this.peakWindow = peakWindow;
        this.peakLoad = peakLoad;
        LOG.info("Fraud scoring simulation: mode={}, cycle={}, peakWindow={}, peakLoad={}",
                mode, cycleLength, peakWindow, peakLoad);
    }

    /** Scores a payment with the fraud model. Times out (TimeLimiter) when the model is overloaded. */
    @TimeLimiter(name = FRAUD_SCORING)
    @CircuitBreaker(name = FRAUD_SCORING, fallbackMethod = "rulesFallback")
    public Mono<String> scorePayment(String accountId, BigDecimal amount, String channel) {
        return Mono.defer(() -> {
            int load = currentBackgroundLoad() + inFlight.incrementAndGet();
            Duration latency = latencyFor(load);
            return Mono.delay(latency)
                    .map(tick -> {
                        int score = modelScore(amount, channel);
                        return String.format(
                                "SCORE txn=%s account=%s amount=%s channel=%s score=%d decision=%s source=model load=%d latencyMs=%d",
                                newTransactionId(), accountId, amount, channel, score, decisionFor(score), load,
                                latency.toMillis());
                    })
                    .doFinally(signal -> inFlight.decrementAndGet());
        });
    }

    /** Model timed out (recorded as a failure by the breaker): decide with rules instead. */
    private Mono<String> rulesFallback(String accountId, BigDecimal amount, String channel, TimeoutException ex) {
        return Mono.just(rulesDecision(accountId, amount, channel, "model-timeout"));
    }

    /** Breaker OPEN: don't call the model at all, decide with rules instead. */
    private Mono<String> rulesFallback(String accountId, BigDecimal amount, String channel, CallNotPermittedException ex) {
        return Mono.just(rulesDecision(accountId, amount, channel, "fraud-circuit-open"));
    }

    private String rulesDecision(String accountId, BigDecimal amount, String channel, String reason) {
        boolean autoApprove = amount.abs().compareTo(RULES_AUTO_APPROVE_LIMIT) <= 0 && !"WIRE".equals(channel);
        String decision;
        if (autoApprove) {
            decision = "APPROVE";
        } else {
            decision = "MANUAL_REVIEW";
            manualReviewCount.incrementAndGet();
        }
        return String.format("SCORE txn=%s account=%s amount=%s channel=%s decision=%s source=rules-fallback reason=%s",
                newTransactionId(), accountId, amount, channel, decision, reason);
    }

    // ---- simulation helpers -------------------------------------------------------------------------

    /** Picks a demo account so a controller or script can call the model without passing one. */
    public String randomDemoAccount() {
        return DEMO_ACCOUNTS.get(ThreadLocalRandom.current().nextInt(DEMO_ACCOUNTS.size()));
    }

    /** Random payment amount between 5.00 and 5,000.00; most payments are small. */
    public BigDecimal randomPaymentAmount() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        double value = random.nextDouble() < 0.7 ? random.nextDouble(5.0, 250.0) : random.nextDouble(250.0, 5_000.0);
        return BigDecimal.valueOf(value).setScale(2, RoundingMode.HALF_UP);
    }

    public String randomChannel() {
        return CHANNELS.get(ThreadLocalRandom.current().nextInt(CHANNELS.size()));
    }

    /** Normalizes a channel name, rejecting unknown values. */
    public String normalizeChannel(String channel) {
        String normalized = channel.trim().toUpperCase(Locale.ROOT);
        if (!CHANNELS.contains(normalized)) {
            throw new IllegalArgumentException("Unknown channel '" + channel + "'. Use one of " + CHANNELS);
        }
        return normalized;
    }

    public LoadMode getMode() {
        return mode;
    }

    public void setMode(LoadMode mode) {
        LOG.info("Fraud scoring simulation mode changed: {} -> {}", this.mode, mode);
        this.mode = mode;
    }

    public long manualReviewCount() {
        return manualReviewCount.get();
    }

    public int inFlightCount() {
        return inFlight.get();
    }

    /** True while the SCHEDULED mode is inside its traffic peak. */
    public boolean inPeakWindow() {
        return positionInPeak() >= 0;
    }

    /**
     * Simulated load from other callers. In SCHEDULED mode it ramps linearly from 0 up to peakLoad at the
     * middle of the peak window and back down to 0 at its end.
     */
    public int currentBackgroundLoad() {
        return switch (mode) {
            case NORMAL -> 0;
            case PEAK -> peakLoad;
            case SCHEDULED -> {
                double position = positionInPeak();
                yield position < 0 ? 0 : (int) Math.round(peakLoad * (1.0 - Math.abs(2.0 * position - 1.0)));
            }
        };
    }

    /** Position inside the peak window from 0.0 (start) to 1.0 (end), or -1 when outside it. */
    private double positionInPeak() {
        long cycleMs = cycleLength.toMillis();
        long peakMs = peakWindow.toMillis();
        long positionInCycle = System.currentTimeMillis() % cycleMs;
        long peakStart = cycleMs - peakMs;
        return positionInCycle < peakStart || peakMs == 0 ? -1 : (double) (positionInCycle - peakStart) / peakMs;
    }

    private Duration latencyFor(int load) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        long base = random.nextLong(BASE_MIN_MS, BASE_MAX_MS + 1);
        double jitter = random.nextDouble(0.7, 1.3);
        long latencyMs = base + Math.round(load * MS_PER_LOAD_UNIT * jitter);
        LOG.debug("fraud-scoring mode={} load={} latencyMs={}", mode, load, latencyMs);
        return Duration.ofMillis(latencyMs);
    }

    /** Toy risk score: larger amounts and wire transfers score higher, plus noise. */
    private static int modelScore(BigDecimal amount, String channel) {
        double amountRisk = Math.min(amount.abs().doubleValue() / 5_000.0, 1.0) * 450;
        double channelRisk = switch (channel) {
            case "WIRE" -> 250;
            case "WEB" -> 120;
            case "CARD" -> 100;
            case "MOBILE" -> 80;
            default -> 30;
        };
        double noise = ThreadLocalRandom.current().nextDouble(0, 300);
        return (int) Math.min(999, Math.round(amountRisk + channelRisk + noise));
    }

    private static String decisionFor(int score) {
        if (score >= DECLINE_THRESHOLD) {
            return "DECLINE";
        }
        return score >= REVIEW_THRESHOLD ? "REVIEW" : "APPROVE";
    }

    private static String newTransactionId() {
        return "PAY-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
    }
}
