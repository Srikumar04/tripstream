package com.tripstream.reliability;

import java.util.Map;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/**
 * Operator endpoint: POST /admin/redrive/{queue}?max=1000. No authentication in this project; in AWS it must stay
 * on a private network or behind IAM auth (see docs/failure-modes.md).
 */
@RestController
@Profile("api")
class RedriveController {

    private final RedriveService redrive;

    RedriveController(RedriveService redrive) {
        this.redrive = redrive;
    }

    @PostMapping("/admin/redrive/{queue}")
    Map<String, Object> redrive(@PathVariable String queue, @RequestParam(defaultValue = "1000") int max) {
        if (!RedriveService.QUEUES.contains(queue)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "queue must be one of " + RedriveService.QUEUES);
        }
        return Map.of("queue", queue, "moved", redrive.redrive(queue, max));
    }
}
