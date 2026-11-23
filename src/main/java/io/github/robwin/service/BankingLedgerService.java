package io.github.robwin.service;


import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;

/**
 * This Service shows how to use the CircuitBreaker annotation for a core banking ledger backend
 * - failures to simulate: slow calls
 *
 * <p>Simulates the mainframe system of record for account balances and postings. The ledger never
 * returns errors here; it only gets slow. The "core-banking-ledger" circuit breaker therefore trips on
 * its slow-call rate rather than its failure rate, which is the typical failure shape of a mainframe
 * under load or during its batch processing window.</p>
 *
 * <p>Latency modes (property {@code banking-ledger.simulation.mode}, or {@link #setMode} at runtime):</p>
 * <ul>
 *   <li>{@code HEALTHY}   - every call is fast</li>
 *   <li>{@code DEGRADED}  - most calls are slow, all the time</li>
 *   <li>{@code SCHEDULED} - fast most of the time, slow during a recurring "batch window" at the end of
 *       each cycle (default: the last 3 minutes of every 10). This makes the breaker open and close on
 *       its own, so the metrics show repeated state changes without anyone touching the app.</li>
 * </ul>
 *
 * <p>Fallbacks run only when the breaker is OPEN (CallNotPermittedException):</p>
 * <ul>
 *   <li>Balance inquiries return the last known balance, flagged as stale.</li>
 *   <li>Postings are queued as PENDING (store-and-forward) instead of being rejected.</li>
 * </ul>
 */
@Component(value = "bankingLedgerService")
public class BankingLedgerService {

    public static final String CORE_BANKING_LEDGER = "core-banking-ledger";

    public enum LatencyMode { HEALTHY, DEGRADED, SCHEDULED }

    private static final Logger LOG = LoggerFactory.getLogger(BankingLedgerService.class);

    // Normal mainframe response time
    private static final long FAST_MIN_MS = 50;
    private static final long FAST_MAX_MS = 250;

    // Degraded response time. Keep the minimum above slowCallDurationThreshold in application.yml (1500ms)
    // so every slow call is recorded as slow, and the maximum below the demo script's 10s client timeout.
    private static final long SLOW_MIN_MS = 1_800;
    private static final long SLOW_MAX_MS = 4_000;

    private static final List<String> DEMO_ACCOUNTS = List.of(
            "CHK-100245", "CHK-100877", "SAV-200113", "SAV-200569", "BRK-300042", "CRD-400918");

    private final Duration cycleLength;
    private final Duration batchWindow;
    private final double slowCallProbability;
    private volatile LatencyMode mode;

    /** Simulated ledger balances, the "system of record". */
    private final Map<String, BigDecimal> ledgerBalances = new ConcurrentHashMap<>();

    /** Last balance successfully read per account, served by the fallback while the breaker is open. */
    private final Map<String, BigDecimal> lastKnownBalances = new ConcurrentHashMap<>();

    /** Postings accepted while the ledger was unavailable, waiting to be replayed. */
    private final Queue<PendingPosting> pendingPostings = new ConcurrentLinkedQueue<>();

    public BankingLedgerService(
            @Value("${banking-ledger.simulation.mode:SCHEDULED}") LatencyMode mode,
            @Value("${banking-ledger.simulation.cycle-length:10m}") Duration cycleLength,
            @Value("${banking-ledger.simulation.batch-window:3m}") Duration batchWindow,
            @Value("${banking-ledger.simulation.slow-call-probability:0.8}") double slowCallProbability) {
        if (batchWindow.compareTo(cycleLength) > 0) {
            throw new IllegalArgumentException("banking-ledger.simulation.batch-window must not exceed cycle-length");
        }
        this.mode = mode;
        this.cycleLength = cycleLength;
        this.batchWindow = batchWindow;
        this.slowCallProbability = slowCallProbability;
        LOG.info("Core banking ledger simulation: mode={}, cycle={}, batchWindow={}, slowCallProbability={}",
                mode, cycleLength, batchWindow, slowCallProbability);
    }

    /** Balance inquiry against the system of record. */
    @CircuitBreaker(name = CORE_BANKING_LEDGER, fallbackMethod = "balanceFallback")
    public Mono<String> getAccountBalance(String accountId) {
        Duration latency = nextLatency();
        return Mono.delay(latency).map(tick -> {
            BigDecimal balance = balanceOf(accountId);
            lastKnownBalances.put(accountId, balance);
            return String.format("BALANCE account=%s balance=%s source=ledger latencyMs=%d",
                    accountId, balance, latency.toMillis());
        });
    }

    /** Posts a debit (negative amount) or credit (positive amount) to the ledger. */
    @CircuitBreaker(name = CORE_BANKING_LEDGER, fallbackMethod = "postingFallback")
    public Mono<String> postTransaction(String accountId, BigDecimal amount) {
        Duration latency = nextLatency();
        return Mono.delay(latency).map(tick -> {
            BigDecimal newBalance = ledgerBalances.merge(accountId, amount, BigDecimal::add);
            lastKnownBalances.put(accountId, newBalance);
            return String.format("POSTED txn=%s account=%s amount=%s newBalance=%s latencyMs=%d",
                    newTransactionId(), accountId, amount, newBalance, latency.toMillis());
        });
    }

    /** Breaker OPEN: serve the last known balance, flagged as stale, instead of failing the inquiry. */
    private Mono<String> balanceFallback(String accountId, CallNotPermittedException ex) {
        BigDecimal cached = lastKnownBalances.get(accountId);
        if (cached == null) {
            return Mono.just(String.format("BALANCE_UNAVAILABLE account=%s reason=ledger-circuit-open", accountId));
        }
        return Mono.just(String.format("BALANCE account=%s balance=%s source=cache stale=true reason=ledger-circuit-open",
                accountId, cached));
    }

    /** Breaker OPEN: accept the posting as PENDING (store-and-forward) so the payment isn't lost. */
    private Mono<String> postingFallback(String accountId, BigDecimal amount, CallNotPermittedException ex) {
        PendingPosting posting = new PendingPosting(newTransactionId(), accountId, amount, Instant.now());
        pendingPostings.add(posting);
        return Mono.just(String.format("PENDING txn=%s account=%s amount=%s queueDepth=%d reason=ledger-circuit-open",
                posting.transactionId(), accountId, amount, pendingPostings.size()));
    }

    // ---- simulation helpers -------------------------------------------------------------------------

    /** Picks a demo account so a controller or script can call the ledger without passing an account. */
    public String randomDemoAccount() {
        return DEMO_ACCOUNTS.get(ThreadLocalRandom.current().nextInt(DEMO_ACCOUNTS.size()));
    }

    /** Random posting amount between -500.00 and +500.00. */
    public BigDecimal randomPostingAmount() {
        double value = ThreadLocalRandom.current().nextDouble(-500.0, 500.0);
        return BigDecimal.valueOf(value).setScale(2, RoundingMode.HALF_UP);
    }

    public LatencyMode getMode() {
        return mode;
    }

    public void setMode(LatencyMode mode) {
        LOG.info("Core banking ledger simulation mode changed: {} -> {}", this.mode, mode);
        this.mode = mode;
    }

    public int pendingPostingCount() {
        return pendingPostings.size();
    }

    /** True while the SCHEDULED mode is inside its slow batch window. */
    public boolean inBatchWindow() {
        long cycleMs = cycleLength.toMillis();
        long positionInCycle = System.currentTimeMillis() % cycleMs;
        return positionInCycle >= cycleMs - batchWindow.toMillis();
    }

    private Duration nextLatency() {
        boolean degraded = switch (mode) {
            case HEALTHY -> false;
            case DEGRADED -> true;
            case SCHEDULED -> inBatchWindow();
        };
        ThreadLocalRandom random = ThreadLocalRandom.current();
        long latencyMs = degraded && random.nextDouble() < slowCallProbability
                ? random.nextLong(SLOW_MIN_MS, SLOW_MAX_MS + 1)
                : random.nextLong(FAST_MIN_MS, FAST_MAX_MS + 1);
        LOG.debug("core-banking-ledger mode={} degraded={} latencyMs={}", mode, degraded, latencyMs);
        return Duration.ofMillis(latencyMs);
    }

    private BigDecimal balanceOf(String accountId) {
        return ledgerBalances.computeIfAbsent(accountId, id -> BigDecimal
                .valueOf(ThreadLocalRandom.current().nextDouble(1_000.0, 25_000.0))
                .setScale(2, RoundingMode.HALF_UP));
    }

    private static String newTransactionId() {
        return "LDG-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
    }

    private record PendingPosting(String transactionId, String accountId, BigDecimal amount, Instant receivedAt) {
    }
}
