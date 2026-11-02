package com.tripstream.jobs;

import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Profile("api")
class UploadController {

    private final UploadService uploads;

    UploadController(UploadService uploads) {
        this.uploads = uploads;
    }

    @PostMapping("/uploads")
    @ResponseStatus(HttpStatus.CREATED)
    UploadService.Upload create() {
        return uploads.createUpload();
    }
}
