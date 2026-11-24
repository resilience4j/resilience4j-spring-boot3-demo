package io.github.robwin.controller;

import io.github.robwin.service.ForeignExchangeRateService;
import io.github.robwin.service.ForeignExchangeRateService.FxMode;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
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
 * Endpoints for the simulated FX market data provider, protected by the "fx-rates" circuit breaker.
 *
 * <pre>
 * GET /fx/quote?currency=EUR&amp;amount=2500.00            price a USD amount in another currency (random values if omitted)
 * GET /fx/status                                       mode, feed freeze timing, breaker state, outcome counts
 * PUT /fx/mode/{HEALTHY|FLAKY|FROZEN|DOWN}             change the mode at runtime
 * </pre>
 */
@RestController
@RequestMapping(value = "/fx")
public class ForeignExchangeRateController {

    private final ForeignExchangeRateService fxService;

    public ForeignExchangeRateController(ForeignExchangeRateService fxService) {
        this.fxService = fxService;
    }

    @GetMapping("quote")
    public Mono<String> quote(@RequestParam(name = "currency", required = false) String currency,
                              @RequestParam(name = "amount", required = false) BigDecimal amount) {
        if (amount != null && amount.signum() <= 0) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "amount must be positive");
        }
        String resolvedCurrency;
        try {
            resolvedCurrency = currency != null && !currency.isBlank()
                    ? fxService.normalizeCurrency(currency)
                    : fxService.randomCurrency();
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage());
        }
        return fxService.quote(resolvedCurrency, amount != null ? amount : fxService.randomUsdAmount());
    }

    @GetMapping("status")
    public Map<String, Object> status() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("circuitBreaker", ForeignExchangeRateService.FX_RATES);
        status.put("circuitBreakerState", fxService.circuitBreakerState());
        status.put("mode", fxService.getMode());
        boolean frozen = fxService.feedFrozen();
        status.put("feedFrozen", frozen);
        if (fxService.getMode() == FxMode.FLAKY) {
            status.put(frozen ? "freezeEndsInSeconds" : "nextFreezeInSeconds", fxService.secondsToNextFreezeChange());
        }
        status.put("liveQuotes", fxService.liveCount());
        status.put("cachedQuotes", fxService.cachedCount());
        status.put("refusedQuotes", fxService.refusedCount());
        status.put("invalidQuotesByReason", fxService.invalidQuotesByReason());
        return status;
    }

    @PutMapping("mode/{mode}")
    public Map<String, Object> setMode(@PathVariable("mode") String mode) {
        try {
            fxService.setMode(FxMode.valueOf(mode.trim().toUpperCase(Locale.ROOT)));
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "Unknown mode '" + mode + "'. Use one of " + Arrays.toString(FxMode.values()));
        }
        return status();
    }
}
