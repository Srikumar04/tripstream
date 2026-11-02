package com.tripstream.chunk;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.Set;

/**
 * Parses and validates one CSV line. Line-oriented, comma-separated, no quoting
 * (the synthetic format never contains embedded commas or newlines).
 */
public final class RowValidator {

    public static final String HEADER = "trip_id,vehicle_id,driver_id,start_time,end_time,distance_km,status";
    private static final Set<String> STATUSES = Set.of("COMPLETED", "CANCELLED", "IN_PROGRESS");
    private static final BigDecimal MAX_DISTANCE = new BigDecimal("99999999.99"); // NUMERIC(10,2)

    /** Exactly one of trip / error is non-null. */
    public record Result(TripRow trip, String error) {
        public boolean valid() {
            return trip != null;
        }
    }

    private RowValidator() {
    }

    public static Result validate(String line) {
        String[] f = line.split(",", -1);
        if (f.length != 7) {
            return bad("expected 7 columns but found " + f.length);
        }
        for (int i = 0; i < 3; i++) {
            String v = f[i].trim();
            if (v.isEmpty()) {
                return bad(columnName(i) + " is required");
            }
            if (v.length() > 64) {
                return bad(columnName(i) + " exceeds 64 characters");
            }
            f[i] = v;
        }
        OffsetDateTime start;
        OffsetDateTime end;
        try {
            start = OffsetDateTime.ofInstant(Instant.parse(f[3].trim()), ZoneOffset.UTC);
        } catch (DateTimeParseException e) {
            return bad("start_time is not an ISO-8601 instant");
        }
        try {
            end = OffsetDateTime.ofInstant(Instant.parse(f[4].trim()), ZoneOffset.UTC);
        } catch (DateTimeParseException e) {
            return bad("end_time is not an ISO-8601 instant");
        }
        if (end.isBefore(start)) {
            return bad("end_time is before start_time");
        }
        BigDecimal distance;
        try {
            distance = new BigDecimal(f[5].trim());
        } catch (NumberFormatException e) {
            return bad("distance_km is not a number");
        }
        if (distance.signum() < 0) {
            return bad("distance_km must be >= 0");
        }
        if (distance.compareTo(MAX_DISTANCE) > 0) {
            return bad("distance_km is too large");
        }
        String status = f[6].trim();
        if (!STATUSES.contains(status)) {
            return bad("status must be one of " + STATUSES);
        }
        return new Result(new TripRow(f[0], f[1], f[2], start, end, distance, status), null);
    }

    private static Result bad(String reason) {
        return new Result(null, reason);
    }

    private static String columnName(int i) {
        return switch (i) {
            case 0 -> "trip_id";
            case 1 -> "vehicle_id";
            default -> "driver_id";
        };
    }
}
