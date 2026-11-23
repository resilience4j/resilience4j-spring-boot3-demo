package io.github.robwin.service;


import com.dynatrace.openkit.util.json.objects.JSONBooleanValue;
import com.dynatrace.openkit.util.json.objects.JSONNumberValue;
import com.dynatrace.openkit.util.json.objects.JSONStringValue;
import com.dynatrace.openkit.util.json.objects.JSONValue;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.bulkhead.BulkheadFullException;
import io.github.resilience4j.bulkhead.annotation.Bulkhead;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import io.github.resilience4j.timelimiter.annotation.TimeLimiter;
import io.github.robwin.exception.BusinessException;
import io.vavr.control.Try;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;

import static io.github.resilience4j.bulkhead.annotation.Bulkhead.Type;

/**
 * This Service shows how to use the CircuitBreaker annotation.
 */
@Component(value = "backendAService")
public class BackendAService implements Service {

    private static final String BACKEND_A = "backendA";
    private static final String CLAIMS_DATASET = "data/mock_pharmacy_claims_dataset.json";

    //@Autowired
    //private BizEventAgent bizeventAgent;

    /** The "records" array from the claims dataset, loaded once at startup. */
    private final JsonNode claimRecords;

    /** Record indexes grouped by transaction_id, which is the only field used for the duplicate check. */
    private final Map<String, List<Integer>> recordIndexesByTransactionId = new LinkedHashMap<>();

    /** transaction_ids not yet sent in the current cycle, in random order. Guarded by "this". */
    private final Deque<String> unusedTransactionIds = new ArrayDeque<>();

    public BackendAService(ObjectMapper objectMapper) {
        try (InputStream in = new ClassPathResource(CLAIMS_DATASET).getInputStream()) {
            JsonNode records = objectMapper.readTree(in).path("records");
            if (!records.isArray() || records.isEmpty()) {
                throw new IllegalStateException("No records array found in " + CLAIMS_DATASET);
            }
            this.claimRecords = records;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load " + CLAIMS_DATASET, e);
        }

        for (int i = 0; i < claimRecords.size(); i++) {
            JsonNode id = claimRecords.get(i).path("transaction_meta").path("transaction_id");
            if (id.isTextual() && !id.asText().isBlank()) {
                recordIndexesByTransactionId.computeIfAbsent(id.asText(), k -> new ArrayList<>()).add(i);
            }
        }
        if (recordIndexesByTransactionId.isEmpty()) {
            throw new IllegalStateException("No records with a transaction_id found in " + CLAIMS_DATASET);
        }
    }

    /**
     * Returns a random claim record whose transaction_id hasn't been sent yet in the current cycle.
     * If several records share a transaction_id, only one of them is used per cycle.
     * Once every transaction_id has been used, the cycle resets.
     */
    private synchronized JsonNode nextClaimRecord() {
        if (unusedTransactionIds.isEmpty()) {
            List<String> ids = new ArrayList<>(recordIndexesByTransactionId.keySet());
            Collections.shuffle(ids);
            unusedTransactionIds.addAll(ids);
        }
        List<Integer> candidates = recordIndexesByTransactionId.get(unusedTransactionIds.poll());
        return claimRecords.get(candidates.get(ThreadLocalRandom.current().nextInt(candidates.size())));
    }

    @Override
    @CircuitBreaker(name = BACKEND_A)
    @Bulkhead(name = BACKEND_A)
    @Retry(name = BACKEND_A)
    public String failure() {
        throw new HttpServerErrorException(HttpStatus.INTERNAL_SERVER_ERROR, "This is a remote exception");
    }

    @Override
    @CircuitBreaker(name = BACKEND_A)
    @Bulkhead(name = BACKEND_A)
    public String ignoreException() {
        throw new BusinessException("This exception is ignored by the CircuitBreaker of backend A");
    }

    @Override
    @CircuitBreaker(name = BACKEND_A)
    @Bulkhead(name = BACKEND_A)
    @Retry(name = BACKEND_A)
    public String success() {
        JsonNode claimRecord = nextClaimRecord();
        JsonNode txn = claimRecord.path("transaction_meta");

        // Transaction metadata only, via an explicit allowlist so no PHI is sent.
        // date_of_service is intentionally excluded (a date tied to a patient's care is PHI under HIPAA).
        Map<String,JSONValue> businessData = new HashMap<>();
        putString(businessData, "transaction_id", txn);
        putString(businessData, "transaction_code", txn);
        putString(businessData, "transaction_code_description", txn);
        putString(businessData, "ncpdp_version_release", txn);
        putString(businessData, "time_of_transmission", txn);
        putString(businessData, "response_status", txn);
        if (txn.path("processing_duration_ms").isNumber()) {
            businessData.put("processing_duration_ms",
                    JSONNumberValue.fromLong(txn.path("processing_duration_ms").asLong()));
        }

        // NCPDP reject code(s) as a single string attribute: "83", or "75,76" when there are several; "" for paid claims
        List<String> rejectCodes = new ArrayList<>();
        for (JsonNode reject : claimRecord.path("step5_final_determination").path("reject_codes")) {
            JsonNode code = reject.path("ncpdp_reject_code");
            if (!code.isMissingNode() && !code.isNull()) {
                rejectCodes.add(code.asText());
            }
        }
        businessData.put("reject_code", JSONStringValue.fromString(String.join(",", rejectCodes)));

        // Whether the claim required prior authorization (false if the field is missing)
        boolean priorAuthRequired = claimRecord.path("step3_safety_clinical_edits")
                .path("clinical_requirements").path("prior_authorization_required").asBoolean(false);
        businessData.put("prior_auth_required", JSONBooleanValue.fromValue(priorAuthRequired));

        // National Drug Code (11-digit, kept as a string to preserve leading zeros)
        JsonNode ndc = claimRecord.path("step1_claim_submission").path("prescription").path("product_service_id_ndc");
        if (!ndc.isMissingNode() && !ndc.isNull()) {
            businessData.put("ndc_code", JSONStringValue.fromString(ndc.asText()));
        }



        return "Hello World from backend A";
    }

    /** Copies a string field from the source node into the event map, skipping missing or null values. */
    private static void putString(Map<String, JSONValue> target, String field, JsonNode source) {
        JsonNode value = source.path(field);
        if (!value.isMissingNode() && !value.isNull()) {
            target.put(field, JSONStringValue.fromString(value.asText()));
        }
    }

    @Override
    @CircuitBreaker(name = BACKEND_A)
    @Bulkhead(name = BACKEND_A)
    public String successException() {
        throw new HttpClientErrorException(HttpStatus.BAD_REQUEST, "This is a remote client exception");
    }

    @Override
    @CircuitBreaker(name = BACKEND_A)
    @Bulkhead(name = BACKEND_A)
    @Retry(name = BACKEND_A)
    public Flux<String> fluxFailure() {
        return Flux.error(new IOException("BAM!"));
    }

    @Override
    @TimeLimiter(name = BACKEND_A)
    @CircuitBreaker(name = BACKEND_A, fallbackMethod = "fluxFallback")
    public Flux<String> fluxTimeout() {
        return Flux.
                just("Hello World from backend A")
                .delayElements(Duration.ofSeconds(10));
    }

    @Override
    @TimeLimiter(name = BACKEND_A)
    @CircuitBreaker(name = BACKEND_A)
    @Bulkhead(name = BACKEND_A)
    @Retry(name = BACKEND_A)
    public Mono<String> monoSuccess() {
        return Mono.just("Hello World from backend A");
    }

    @Override
    @CircuitBreaker(name = BACKEND_A)
    @Bulkhead(name = BACKEND_A)
    @Retry(name = BACKEND_A)
    public Mono<String> monoFailure() {
        return Mono.error(new IOException("BAM!"));
    }

    @Override
    @TimeLimiter(name = BACKEND_A)
    @Bulkhead(name = BACKEND_A)
    @CircuitBreaker(name = BACKEND_A, fallbackMethod = "monoFallback")
    public Mono<String> monoTimeout() {
        return Mono.just("Hello World from backend A")
                .delayElement(Duration.ofSeconds(10));
    }

    @Override
    @TimeLimiter(name = BACKEND_A)
    @CircuitBreaker(name = BACKEND_A)
    @Retry(name = BACKEND_A)
    public Flux<String> fluxSuccess() {
        return Flux.just("Hello", "World");
    }

    @Override
    @CircuitBreaker(name = BACKEND_A, fallbackMethod = "fallback")
    public String failureWithFallback() {
        return failure();
    }

    @Override
    @Bulkhead(name = BACKEND_A, type = Type.THREADPOOL)
    @TimeLimiter(name = BACKEND_A)
    @CircuitBreaker(name = BACKEND_A)
    @Retry(name = BACKEND_A)
    public CompletableFuture<String> futureSuccess() {
        return CompletableFuture.completedFuture("Hello World from backend A");
    }

    @Override
    @Bulkhead(name = BACKEND_A, type = Type.THREADPOOL)
    @TimeLimiter(name = BACKEND_A)
    @CircuitBreaker(name = BACKEND_A)
    @Retry(name = BACKEND_A)
    public CompletableFuture<String> futureFailure() {
        CompletableFuture<String> future = new CompletableFuture<>();
        future.completeExceptionally(new IOException("BAM!"));
        return future;
    }

    @Override
    @Bulkhead(name = BACKEND_A, type = Type.THREADPOOL)
    @TimeLimiter(name = BACKEND_A)
    @CircuitBreaker(name =BACKEND_A, fallbackMethod = "futureFallback")
    public CompletableFuture<String> futureTimeout() {
        Try.run(() -> Thread.sleep(5000));
        return CompletableFuture.completedFuture("Hello World from backend A");
    }

    private String fallback(HttpServerErrorException ex) {
        return "Recovered HttpServerErrorException: " + ex.getMessage();
    }

    private String fallback(Exception ex) {
        return "Recovered: " + ex.toString();
    }

    private CompletableFuture<String> futureFallback(TimeoutException ex) {
        return CompletableFuture.completedFuture("Recovered specific TimeoutException: " + ex.toString());
    }

    private CompletableFuture<String> futureFallback(BulkheadFullException ex) {
        return CompletableFuture.completedFuture("Recovered specific BulkheadFullException: " + ex.toString());
    }

    private CompletableFuture<String> futureFallback(CallNotPermittedException ex) {
        return CompletableFuture.completedFuture("Recovered specific CallNotPermittedException: " + ex.toString());
    }

    private Mono<String> monoFallback(Exception ex) {
        return Mono.just("Recovered: " + ex.toString());
    }

    private Flux<String> fluxFallback(Exception ex) {
        return Flux.just("Recovered: " + ex.toString());
    }
}
