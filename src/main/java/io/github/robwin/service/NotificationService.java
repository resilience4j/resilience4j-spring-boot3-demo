package io.github.robwin.service;


import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This Service shows how to use the CircuitBreaker annotation for a non-critical notification backend
 * - failures to simulate: fails often, but doesn't matter
 *
 * <p>Simulates the customer notification hub that sends SMS, email and push messages (payment confirmations,
 * low-balance alerts, statements) through third-party providers. The providers are flaky all the time:
 * rate limiting (HTTP 429), provider errors (HTTP 503) and dropped connections. Roughly a third of
 * deliveries fail even on a normal day. Now and then the whole hub has an outage.</p>
 *
 * <p>The point of this breaker is the contrast with the critical ones. Its failureRateThreshold is 80%
 * (application.yml), so the everyday 25-45% failure rate never opens it; only a real outage does. A
 * notification is never worth failing or slowing down a payment for, so every failure is simply dropped.</p>
 *
 * <p>Note that HTTP 429 is a 4xx, so the "notification-service" instance lists
 * {@code HttpClientErrorException$TooManyRequests} in its own recordExceptions. Without that, Resilience4j
 * would count a rate-limited call as a success.</p>
 *
 * <p>Modes (property {@code notification-service.simulation.mode}, or {@link #setMode} at runtime):</p>
 * <ul>
 *   <li>{@code HEALTHY} - every delivery succeeds</li>
 *   <li>{@code FLAKY}   - everyday per-channel failure rates, plus random hub outages (default)</li>
 *   <li>{@code OUTAGE}  - every delivery fails</li>
 * </ul>
 *
 * <p>Fallback ("fire-and-forget, so drop it"): any failure, including an OPEN breaker, returns DROPPED and
 * increments a counter. No retry, no queue. In a real payment flow these calls would also be subscribed to
 * off the payment's critical path, so the customer's payment never waits on them.</p>
 */
@Component(value = "notificationService")
public class NotificationService {

    public static final String NOTIFICATION_SERVICE = "notification-service";

    public enum NotificationMode { HEALTHY, FLAKY, OUTAGE }

    public enum Channel { SMS, EMAIL, PUSH }

    public enum Template { PAYMENT_CONFIRMATION, LOW_BALANCE_ALERT, STATEMENT_READY }

    public enum Status { SENT, DROPPED }

    /** The outcome of one delivery attempt on one channel. */
    public record NotificationResult(String notificationId, Channel channel, Template template, String accountId,
                                     Status status, String detail) {
        /** One-line description, e.g. "SENT id=NTF-1A2B3C4D channel=SMS ... provider=sms-gateway latencyMs=120". */
        public String describe() {
            return String.format("%s id=%s channel=%s template=%s account=%s %s",
                    status, notificationId, channel, template, accountId, detail);
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(NotificationService.class);

    private static final long SEND_MIN_MS = 40;
    private static final long SEND_MAX_MS = 200;
    private static final long ERROR_MIN_MS = 20;
    private static final long ERROR_MAX_MS = 120;

    private static final Duration MIN_GAP_BETWEEN_OUTAGES = Duration.ofMinutes(2);

    private static final List<String> DEMO_ACCOUNTS = List.of(
            "CHK-100245", "CHK-100877", "SAV-200113", "SAV-200569", "BRK-300042", "CRD-400918");

    private static final Map<Channel, String> PROVIDERS = Map.of(
            Channel.SMS, "sms-gateway", Channel.EMAIL, "email-relay", Channel.PUSH, "push-service");

    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final Map<Channel, Double> failureProbability;
    private final double rateLimitShare;
    private final Duration meanTimeBetweenOutages;
    private final Duration outageMin;
    private final Duration outageMax;
    private volatile NotificationMode mode;

    // Outage schedule for FLAKY mode. Guarded by "this".
    private long outageEndMs;
    private long nextOutageStartMs;

    private final AtomicLong sentCount = new AtomicLong();
    private final Map<String, AtomicLong> droppedByReason = new ConcurrentHashMap<>();

    public NotificationService(
            CircuitBreakerRegistry circuitBreakerRegistry,
            @Value("${notification-service.simulation.mode:FLAKY}") NotificationMode mode,
            @Value("${notification-service.simulation.sms-failure-probability:0.45}") double smsFailureProbability,
            @Value("${notification-service.simulation.email-failure-probability:0.25}") double emailFailureProbability,
            @Value("${notification-service.simulation.push-failure-probability:0.30}") double pushFailureProbability,
            @Value("${notification-service.simulation.rate-limit-share:0.6}") double rateLimitShare,
            @Value("${notification-service.simulation.mean-time-between-outages:8m}") Duration meanTimeBetweenOutages,
            @Value("${notification-service.simulation.outage-min:90s}") Duration outageMin,
            @Value("${notification-service.simulation.outage-max:180s}") Duration outageMax) {
        if (outageMin.compareTo(outageMax) > 0) {
            throw new IllegalArgumentException("notification-service.simulation.outage-min must not exceed outage-max");
        }
        this.circuitBreakerRegistry = circuitBreakerRegistry;
        this.mode = mode;
        this.failureProbability = Map.of(
                Channel.SMS, smsFailureProbability,
                Channel.EMAIL, emailFailureProbability,
                Channel.PUSH, pushFailureProbability);
        this.rateLimitShare = rateLimitShare;
        this.meanTimeBetweenOutages = meanTimeBetweenOutages;
        this.outageMin = outageMin;
        this.outageMax = outageMax;
        this.nextOutageStartMs = System.currentTimeMillis() + nextGapMs();
        LOG.info("Notification simulation: mode={}, failureProbability={}, meanTimeBetweenOutages={}, outage={}..{}",
                mode, failureProbability, meanTimeBetweenOutages, outageMin, outageMax);
    }

    /** Delivers one notification on one channel. Never fails: any error is dropped by the fallback. */
    @CircuitBreaker(name = NOTIFICATION_SERVICE, fallbackMethod = "dropFallback")
    public Mono<NotificationResult> send(String notificationId, Channel channel, Template template, String accountId) {
        return Mono.defer(() -> {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            if (random.nextDouble() < currentFailureProbability(channel)) {
                return Mono.delay(Duration.ofMillis(random.nextLong(ERROR_MIN_MS, ERROR_MAX_MS + 1)))
                        .then(Mono.error(randomProviderError(channel)));
            }
            long latencyMs = random.nextLong(SEND_MIN_MS, SEND_MAX_MS + 1);
            return Mono.delay(Duration.ofMillis(latencyMs)).map(tick -> {
                sentCount.incrementAndGet();
                return new NotificationResult(notificationId, channel, template, accountId, Status.SENT,
                        String.format("provider=%s latencyMs=%d", PROVIDERS.get(channel), latencyMs));
            });
        });
    }

    /** Any failure, or an OPEN breaker: drop the notification. It isn't worth retrying or queueing. */
    private Mono<NotificationResult> dropFallback(String notificationId, Channel channel, Template template,
                                                  String accountId, Exception ex) {
        String reason = dropReason(ex);
        droppedByReason.computeIfAbsent(reason, r -> new AtomicLong()).incrementAndGet();
        return Mono.just(new NotificationResult(notificationId, channel, template, accountId, Status.DROPPED,
                "reason=" + reason));
    }

    private static String dropReason(Exception ex) {
        if (ex instanceof CallNotPermittedException) {
            return "notification-circuit-open";
        }
        if (ex instanceof HttpClientErrorException.TooManyRequests) {
            return "rate-limited";
        }
        if (ex instanceof HttpServerErrorException) {
            return "provider-error";
        }
        if (ex instanceof IOException) {
            return "connection-error";
        }
        return "error-" + ex.getClass().getSimpleName();
    }

    // ---- simulation helpers -------------------------------------------------------------------------

    public static String newNotificationId() {
        return "NTF-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase(Locale.ROOT);
    }

    public String randomDemoAccount() {
        return DEMO_ACCOUNTS.get(ThreadLocalRandom.current().nextInt(DEMO_ACCOUNTS.size()));
    }

    /** Mostly payment confirmations, sometimes alerts and statements. */
    public Template randomTemplate() {
        double roll = ThreadLocalRandom.current().nextDouble();
        return roll < 0.7 ? Template.PAYMENT_CONFIRMATION : roll < 0.9 ? Template.LOW_BALANCE_ALERT : Template.STATEMENT_READY;
    }

    public NotificationMode getMode() {
        return mode;
    }

    public void setMode(NotificationMode mode) {
        LOG.info("Notification simulation mode changed: {} -> {}", this.mode, mode);
        this.mode = mode;
    }

    public String circuitBreakerState() {
        return circuitBreakerRegistry.circuitBreaker(NOTIFICATION_SERVICE).getState().name();
    }

    public long sentCount() {
        return sentCount.get();
    }

    /** Dropped notifications per reason, sorted by reason. */
    public Map<String, Long> droppedByReason() {
        Map<String, Long> snapshot = new TreeMap<>();
        droppedByReason.forEach((reason, count) -> snapshot.put(reason, count.get()));
        return snapshot;
    }

    /** True while a hub outage is in progress (always true in OUTAGE, never in HEALTHY). */
    public synchronized boolean inOutage() {
        return switch (mode) {
            case HEALTHY -> false;
            case OUTAGE -> true;
            case FLAKY -> {
                long now = System.currentTimeMillis();
                if (now < outageEndMs) {
                    yield true;
                }
                if (now >= nextOutageStartMs) {
                    long outageLengthMs = ThreadLocalRandom.current()
                            .nextLong(outageMin.toMillis(), outageMax.toMillis() + 1);
                    outageEndMs = now + outageLengthMs;
                    nextOutageStartMs = outageEndMs + nextGapMs();
                    LOG.info("Notification hub outage started, lasting {}s", outageLengthMs / 1000);
                    yield true;
                }
                yield false;
            }
        };
    }

    /** Seconds left in the current outage, or seconds until the next one starts (FLAKY mode). */
    public synchronized long secondsToNextOutageChange() {
        long now = System.currentTimeMillis();
        long target = now < outageEndMs ? outageEndMs : nextOutageStartMs;
        return Math.max(0, (target - now) / 1000);
    }

    private double currentFailureProbability(Channel channel) {
        return switch (mode) {
            case HEALTHY -> 0.0;
            case OUTAGE -> 1.0;
            case FLAKY -> inOutage() ? 1.0 : failureProbability.get(channel);
        };
    }

    private Exception randomProviderError(Channel channel) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        String provider = PROVIDERS.get(channel);
        if (random.nextDouble() < rateLimitShare) {
            // Returns the HttpClientErrorException.TooManyRequests subclass
            return HttpClientErrorException.create(HttpStatus.TOO_MANY_REQUESTS,
                    provider + " rate limit exceeded", HttpHeaders.EMPTY, new byte[0], StandardCharsets.UTF_8);
        }
        return random.nextBoolean()
                ? new HttpServerErrorException(HttpStatus.SERVICE_UNAVAILABLE, provider + " unavailable")
                : new IOException("Connection reset by " + provider);
    }

    /** Random gap between outages: exponentially distributed around the mean, with a floor. */
    private long nextGapMs() {
        double u = ThreadLocalRandom.current().nextDouble();
        long gap = Math.round(-meanTimeBetweenOutages.toMillis() * Math.log(1.0 - u));
        return Math.max(MIN_GAP_BETWEEN_OUTAGES.toMillis(), gap);
    }
}
