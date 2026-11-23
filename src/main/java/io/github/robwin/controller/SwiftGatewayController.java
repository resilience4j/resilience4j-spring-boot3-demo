package io.github.robwin.controller;

import io.github.robwin.exception.BusinessException;
import io.github.robwin.service.SwiftGatewayService;
import io.github.robwin.service.SwiftGatewayService.GatewayMode;
import io.github.robwin.service.SwiftGatewayService.SwiftPayment;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Endpoints for the simulated SWIFT gateway, protected by the "swift-gateway" circuit breaker.
 *
 * <pre>
 * POST /swift/payments?accountId=CHK-100245&amp;amount=15000.00&amp;currency=EUR&amp;beneficiaryBic=DEMODEFF
 *                                               send a cross-border payment (random values if omitted)
 *                                               beneficiaryBic=BAD shows a validation reject (422, ignored by the breaker)
 * GET  /swift/status                            mode, burst timing, breaker state, outbox depth
 * PUT  /swift/mode/{HEALTHY|BURSTY|OUTAGE}      change the gateway mode at runtime
 * </pre>
 */
@RestController
@RequestMapping(value = "/swift")
public class SwiftGatewayController {

    private final SwiftGatewayService swiftGatewayService;

    public SwiftGatewayController(SwiftGatewayService swiftGatewayService) {
        this.swiftGatewayService = swiftGatewayService;
    }

    @PostMapping("payments")
    public Mono<String> sendPayment(@RequestParam(name = "accountId", required = false) String accountId,
                                    @RequestParam(name = "amount", required = false) BigDecimal amount,
                                    @RequestParam(name = "currency", required = false) String currency,
                                    @RequestParam(name = "beneficiaryBic", required = false) String beneficiaryBic) {
        if (amount != null && amount.signum() <= 0) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "amount must be positive");
        }
        if (currency != null && !currency.isBlank() && !currency.trim().matches("[A-Za-z]{3}")) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "currency must be a 3-letter ISO 4217 code");
        }
        SwiftPayment payment = SwiftPayment.of(
                isBlank(accountId) ? swiftGatewayService.randomDemoAccount() : accountId,
                amount != null ? amount : swiftGatewayService.randomAmount(),
                isBlank(currency) ? swiftGatewayService.randomCurrency() : currency.trim().toUpperCase(Locale.ROOT),
                isBlank(beneficiaryBic) ? swiftGatewayService.randomBeneficiaryBic()
                        : beneficiaryBic.trim().toUpperCase(Locale.ROOT));
        return swiftGatewayService.sendPayment(payment);
    }

    @GetMapping("status")
    public Map<String, Object> status() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("circuitBreaker", SwiftGatewayService.SWIFT_GATEWAY);
        status.put("circuitBreakerState", swiftGatewayService.circuitBreakerState());
        status.put("mode", swiftGatewayService.getMode());
        boolean inBurst = swiftGatewayService.inBurst();
        status.put("inBurst", inBurst);
        if (swiftGatewayService.getMode() == GatewayMode.BURSTY) {
            status.put(inBurst ? "burstEndsInSeconds" : "nextBurstInSeconds",
                    swiftGatewayService.secondsToNextBurstChange());
        }
        status.put("outboxDepth", swiftGatewayService.outboxDepth());
        status.put("replayedFromOutbox", swiftGatewayService.replayedCount());
        status.put("droppedOnReplay", swiftGatewayService.droppedOnReplayCount());
        return status;
    }

    @PutMapping("mode/{mode}")
    public Map<String, Object> setMode(@PathVariable("mode") String mode) {
        try {
            swiftGatewayService.setMode(GatewayMode.valueOf(mode.trim().toUpperCase(Locale.ROOT)));
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "Unknown mode '" + mode + "'. Use one of " + Arrays.toString(GatewayMode.values()));
        }
        return status();
    }

    /** Validation rejects are the caller's problem, not the gateway's: 422, and ignored by the breaker. */
    @ExceptionHandler(BusinessException.class)
    public ResponseEntity<String> handleReject(BusinessException ex) {
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).body("REJECTED " + ex.getMessage());
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
