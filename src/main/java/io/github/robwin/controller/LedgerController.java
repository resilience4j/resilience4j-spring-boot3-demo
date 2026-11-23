package io.github.robwin.controller;

import io.github.robwin.service.BankingLedgerService;
import io.github.robwin.service.BankingLedgerService.LatencyMode;
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
 * Endpoints for the simulated core banking ledger, protected by the "core-banking-ledger" circuit breaker.
 *
 * <pre>
 * GET  /ledger/balance?accountId=CHK-100245          balance inquiry (random demo account if omitted)
 * POST /ledger/postings?accountId=...&amp;amount=-42.10   debit/credit posting (random values if omitted)
 * GET  /ledger/status                                 simulation mode, batch window, pending postings
 * PUT  /ledger/mode/{HEALTHY|DEGRADED|SCHEDULED}      change the latency mode at runtime
 * </pre>
 */
@RestController
@RequestMapping(value = "/ledger")
public class LedgerController {

    private final BankingLedgerService ledgerService;

    public LedgerController(BankingLedgerService ledgerService) {
        this.ledgerService = ledgerService;
    }

    @GetMapping("balance")
    public Mono<String> balance(@RequestParam(name = "accountId", required = false) String accountId) {
        return ledgerService.getAccountBalance(accountOrRandom(accountId));
    }

    @PostMapping("postings")
    public Mono<String> postTransaction(@RequestParam(name = "accountId", required = false) String accountId,
                                        @RequestParam(name = "amount", required = false) BigDecimal amount) {
        return ledgerService.postTransaction(accountOrRandom(accountId),
                amount != null ? amount : ledgerService.randomPostingAmount());
    }

    @GetMapping("status")
    public Map<String, Object> status() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("circuitBreaker", BankingLedgerService.CORE_BANKING_LEDGER);
        status.put("mode", ledgerService.getMode());
        status.put("inBatchWindow", ledgerService.inBatchWindow());
        status.put("pendingPostings", ledgerService.pendingPostingCount());
        return status;
    }

    @PutMapping("mode/{mode}")
    public Map<String, Object> setMode(@PathVariable("mode") String mode) {
        try {
            ledgerService.setMode(LatencyMode.valueOf(mode.trim().toUpperCase(Locale.ROOT)));
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "Unknown mode '" + mode + "'. Use one of " + Arrays.toString(LatencyMode.values()));
        }
        return status();
    }

    private String accountOrRandom(String accountId) {
        return accountId != null && !accountId.isBlank() ? accountId : ledgerService.randomDemoAccount();
    }
}
