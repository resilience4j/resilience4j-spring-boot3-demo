package io.github.robwin.controller;

import io.github.robwin.service.FraudScoringService;
import io.github.robwin.service.FraudScoringService.LoadMode;
import org.springframework.http.HttpStatus;
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
 * Endpoints for the simulated fraud scoring model, protected by the "fraud-scoring" circuit breaker
 * and time limiter.
 *
 * <pre>
 * POST /fraud/score?accountId=CHK-100245&amp;amount=129.99&amp;channel=MOBILE   score a payment (random values if omitted)
 * GET  /fraud/status                                                 load mode, current load, manual reviews
 * PUT  /fraud/mode/{NORMAL|PEAK|SCHEDULED}                           change the load mode at runtime
 * </pre>
 */
@RestController
@RequestMapping(value = "/fraud")
public class FraudScoringController {

    private final FraudScoringService fraudScoringService;

    public FraudScoringController(FraudScoringService fraudScoringService) {
        this.fraudScoringService = fraudScoringService;
    }

    @PostMapping("score")
    public Mono<String> score(@RequestParam(name = "accountId", required = false) String accountId,
                              @RequestParam(name = "amount", required = false) BigDecimal amount,
                              @RequestParam(name = "channel", required = false) String channel) {
        String resolvedChannel;
        try {
            resolvedChannel = channel != null && !channel.isBlank()
                    ? fraudScoringService.normalizeChannel(channel)
                    : fraudScoringService.randomChannel();
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage());
        }
        return fraudScoringService.scorePayment(
                accountId != null && !accountId.isBlank() ? accountId : fraudScoringService.randomDemoAccount(),
                amount != null ? amount : fraudScoringService.randomPaymentAmount(),
                resolvedChannel);
    }

    @GetMapping("status")
    public Map<String, Object> status() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("circuitBreaker", FraudScoringService.FRAUD_SCORING);
        status.put("mode", fraudScoringService.getMode());
        status.put("inPeakWindow", fraudScoringService.inPeakWindow());
        status.put("backgroundLoad", fraudScoringService.currentBackgroundLoad());
        status.put("inFlight", fraudScoringService.inFlightCount());
        status.put("manualReviews", fraudScoringService.manualReviewCount());
        return status;
    }

    @PutMapping("mode/{mode}")
    public Map<String, Object> setMode(@PathVariable("mode") String mode) {
        try {
            fraudScoringService.setMode(LoadMode.valueOf(mode.trim().toUpperCase(Locale.ROOT)));
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "Unknown mode '" + mode + "'. Use one of " + Arrays.toString(LoadMode.values()));
        }
        return status();
    }
}
