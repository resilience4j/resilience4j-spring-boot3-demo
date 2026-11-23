package io.github.robwin.controller;

import io.github.robwin.service.NotificationService;
import io.github.robwin.service.NotificationService.Channel;
import io.github.robwin.service.NotificationService.NotificationMode;
import io.github.robwin.service.NotificationService.NotificationResult;
import io.github.robwin.service.NotificationService.Template;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Endpoints for the simulated notification hub, protected by the "notification-service" circuit breaker.
 *
 * <pre>
 * POST /notifications?accountId=CHK-100245&amp;template=PAYMENT_CONFIRMATION
 *                                     fan out one notification to SMS, EMAIL and PUSH (random values if omitted)
 * POST /notifications/{SMS|EMAIL|PUSH}?accountId=...&amp;template=...   send on a single channel
 * GET  /notifications/status          mode, outage timing, breaker state, sent and dropped counts
 * PUT  /notifications/mode/{HEALTHY|FLAKY|OUTAGE}                  change the mode at runtime
 * </pre>
 *
 * <p>Each channel is a separate call through the breaker, so one fan-out records three calls.</p>
 */
@RestController
@RequestMapping(value = "/notifications")
public class NotificationController {

    private final NotificationService notificationService;

    public NotificationController(NotificationService notificationService) {
        this.notificationService = notificationService;
    }

    /** Fans out to every channel concurrently and returns a one-line summary, e.g. "NOTIFY ... sms=SENT email=DROPPED:rate-limited push=SENT". */
    @PostMapping
    public Mono<String> notifyAllChannels(@RequestParam(name = "accountId", required = false) String accountId,
                                          @RequestParam(name = "template", required = false) String template) {
        String resolvedAccount = resolveAccount(accountId);
        Template resolvedTemplate = resolveTemplate(template);
        String notificationId = NotificationService.newNotificationId();
        return Flux.fromArray(Channel.values())
                .flatMapSequential(channel -> notificationService.send(notificationId, channel, resolvedTemplate, resolvedAccount))
                .collectList()
                .map(results -> String.format("NOTIFY id=%s template=%s account=%s %s",
                        notificationId, resolvedTemplate, resolvedAccount,
                        results.stream().map(NotificationController::channelSummary).collect(Collectors.joining(" "))));
    }

    @PostMapping("{channel}")
    public Mono<String> notifySingleChannel(@PathVariable("channel") String channel,
                                            @RequestParam(name = "accountId", required = false) String accountId,
                                            @RequestParam(name = "template", required = false) String template) {
        Channel resolvedChannel = parseEnum(Channel.class, channel, "channel");
        return notificationService.send(NotificationService.newNotificationId(), resolvedChannel,
                        resolveTemplate(template), resolveAccount(accountId))
                .map(NotificationResult::describe);
    }

    @GetMapping("status")
    public Map<String, Object> status() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("circuitBreaker", NotificationService.NOTIFICATION_SERVICE);
        status.put("circuitBreakerState", notificationService.circuitBreakerState());
        status.put("mode", notificationService.getMode());
        boolean inOutage = notificationService.inOutage();
        status.put("inOutage", inOutage);
        if (notificationService.getMode() == NotificationMode.FLAKY) {
            status.put(inOutage ? "outageEndsInSeconds" : "nextOutageInSeconds",
                    notificationService.secondsToNextOutageChange());
        }
        status.put("sent", notificationService.sentCount());
        status.put("droppedByReason", notificationService.droppedByReason());
        return status;
    }

    @PutMapping("mode/{mode}")
    public Map<String, Object> setMode(@PathVariable("mode") String mode) {
        notificationService.setMode(parseEnum(NotificationMode.class, mode, "mode"));
        return status();
    }

    /** "sms=SENT" or "email=DROPPED:rate-limited" */
    private static String channelSummary(NotificationResult result) {
        String summary = result.channel().name().toLowerCase(Locale.ROOT) + "=" + result.status();
        if (result.status() == NotificationService.Status.DROPPED) {
            summary += ":" + result.detail().replace("reason=", "");
        }
        return summary;
    }

    private String resolveAccount(String accountId) {
        return accountId != null && !accountId.isBlank() ? accountId : notificationService.randomDemoAccount();
    }

    private Template resolveTemplate(String template) {
        return template != null && !template.isBlank()
                ? parseEnum(Template.class, template, "template")
                : notificationService.randomTemplate();
    }

    private static <E extends Enum<E>> E parseEnum(Class<E> type, String value, String name) {
        try {
            return Enum.valueOf(type, value.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "Unknown " + name + " '" + value + "'. Use one of " + Arrays.toString(type.getEnumConstants()));
        }
    }
}
