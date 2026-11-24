package io.github.robwin.controller;

import io.github.robwin.service.SactionsScreeningService;
import io.github.robwin.service.SactionsScreeningService.ScreeningMode;
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

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Endpoints for the simulated sanctions screening engine, protected by the "sanctions-screening" retry and
 * circuit breaker.
 *
 * <pre>
 * POST /sanctions/screen?beneficiaryName=OBSIDIAN%20HARBOUR%20TRADING%20LTD&amp;country=SG
 *                                                screen a payment's beneficiary (random values if omitted)
 * GET  /sanctions/status                         mode, reload window, list version, breaker state, outcome counts
 * PUT  /sanctions/mode/{HEALTHY|SCHEDULED|DOWN}  change the mode at runtime
 * </pre>
 */
@RestController
@RequestMapping(value = "/sanctions")
public class SanctionsScreeningController {

    private final SactionsScreeningService sanctionsScreeningService;

    public SanctionsScreeningController(SactionsScreeningService sanctionsScreeningService) {
        this.sanctionsScreeningService = sanctionsScreeningService;
    }

    @PostMapping("screen")
    public Mono<String> screen(@RequestParam(name = "beneficiaryName", required = false) String beneficiaryName,
                               @RequestParam(name = "country", required = false) String country) {
        if (country != null && !country.isBlank() && !country.trim().matches("[A-Za-z]{2}")) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "country must be a 2-letter ISO 3166 code");
        }
        return sanctionsScreeningService.screen(
                SactionsScreeningService.newPaymentId(),
                isBlank(beneficiaryName) ? sanctionsScreeningService.randomBeneficiary()
                        : beneficiaryName.trim().toUpperCase(Locale.ROOT),
                isBlank(country) ? sanctionsScreeningService.randomCountry() : country.trim().toUpperCase(Locale.ROOT));
    }

    @GetMapping("status")
    public Map<String, Object> status() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("circuitBreaker", SactionsScreeningService.SANCTIONS_SCREENING);
        status.put("circuitBreakerState", sanctionsScreeningService.circuitBreakerState());
        status.put("mode", sanctionsScreeningService.getMode());
        boolean inListRefresh = sanctionsScreeningService.inListRefresh();
        status.put("inListRefresh", inListRefresh);
        if (sanctionsScreeningService.getMode() == ScreeningMode.SCHEDULED) {
            status.put(inListRefresh ? "refreshEndsInSeconds" : "nextRefreshInSeconds",
                    sanctionsScreeningService.secondsToNextRefreshChange());
        }
        status.put("listVersion", sanctionsScreeningService.listVersion());
        status.put("cleared", sanctionsScreeningService.clearedCount());
        status.put("potentialMatches", sanctionsScreeningService.potentialMatchCount());
        status.put("heldUnscreened", sanctionsScreeningService.heldUnscreenedCount());
        return status;
    }

    @PutMapping("mode/{mode}")
    public Map<String, Object> setMode(@PathVariable("mode") String mode) {
        try {
            sanctionsScreeningService.setMode(ScreeningMode.valueOf(mode.trim().toUpperCase(Locale.ROOT)));
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "Unknown mode '" + mode + "'. Use one of " + Arrays.toString(ScreeningMode.values()));
        }
        return status();
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
