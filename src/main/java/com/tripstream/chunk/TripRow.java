package com.tripstream.chunk;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

public record TripRow(String tripId, String vehicleId, String driverId, OffsetDateTime startTime,
        OffsetDateTime endTime, BigDecimal distanceKm, String status) {
}
