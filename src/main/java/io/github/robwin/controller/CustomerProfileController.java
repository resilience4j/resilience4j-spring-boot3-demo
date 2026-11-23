package io.github.robwin.controller;

import io.github.robwin.service.CustomerProfileService;
import io.github.robwin.service.CustomerProfileService.ProfileMode;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
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
 * Endpoints for the simulated customer profile service, protected by the "customer-profile" circuit breaker.
 *
 * <pre>
 * GET /customers/profile?customerId=CUS-10007     read a profile (random demo customer if omitted)
 * GET /customers/status                           mode, breaker state, cache size, where profiles were served from
 * PUT /customers/mode/{HEALTHY|DEGRADED|DOWN}     change the mode at runtime
 * </pre>
 */
@RestController
@RequestMapping(value = "/customers")
public class CustomerProfileController {

    private final CustomerProfileService customerProfileService;

    public CustomerProfileController(CustomerProfileService customerProfileService) {
        this.customerProfileService = customerProfileService;
    }

    @GetMapping("profile")
    public Mono<String> profile(@RequestParam(name = "customerId", required = false) String customerId) {
        return customerProfileService.getProfile(customerId != null && !customerId.isBlank()
                ? customerId.trim().toUpperCase(Locale.ROOT)
                : customerProfileService.randomDemoCustomer());
    }

    @GetMapping("status")
    public Map<String, Object> status() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("circuitBreaker", CustomerProfileService.CUSTOMER_PROFILE);
        status.put("circuitBreakerState", customerProfileService.circuitBreakerState());
        status.put("mode", customerProfileService.getMode());
        status.put("cachedProfiles", customerProfileService.cachedProfileCount());
        status.put("servedFromService", customerProfileService.servedFromServiceCount());
        status.put("servedFromCache", customerProfileService.servedFromCacheCount());
        status.put("servedDefault", customerProfileService.servedDefaultCount());
        return status;
    }

    @PutMapping("mode/{mode}")
    public Map<String, Object> setMode(@PathVariable("mode") String mode) {
        try {
            customerProfileService.setMode(ProfileMode.valueOf(mode.trim().toUpperCase(Locale.ROOT)));
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "Unknown mode '" + mode + "'. Use one of " + Arrays.toString(ProfileMode.values()));
        }
        return status();
    }
}
