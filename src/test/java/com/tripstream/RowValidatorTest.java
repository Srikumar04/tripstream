package com.tripstream;

import static org.assertj.core.api.Assertions.assertThat;

import com.tripstream.chunk.RowValidator;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.api.Test;

class RowValidatorTest {

    @Test
    void acceptsAWellFormedRow() {
        var r = RowValidator.validate("T1,V1,D1,2025-01-01T10:00:00Z,2025-01-01T11:00:00Z,12.50,COMPLETED");
        assertThat(r.valid()).isTrue();
        assertThat(r.trip().distanceKm()).isEqualByComparingTo("12.50");
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "T1,,D1,2025-01-01T10:00:00Z,2025-01-01T11:00:00Z,1.0,COMPLETED|vehicle_id is required",
            "T1,V1,D1,nope,2025-01-01T11:00:00Z,1.0,COMPLETED|start_time is not",
            "T1,V1,D1,2025-01-01T12:00:00Z,2025-01-01T11:00:00Z,1.0,COMPLETED|before start_time",
            "T1,V1,D1,2025-01-01T10:00:00Z,2025-01-01T11:00:00Z,-1,COMPLETED|must be >= 0",
            "T1,V1,D1,2025-01-01T10:00:00Z,2025-01-01T11:00:00Z,abc,COMPLETED|not a number",
            "T1,V1,D1,2025-01-01T10:00:00Z,2025-01-01T11:00:00Z,1.0,UNKNOWN|status must be",
            "T1,V1,D1,2025-01-01T10:00:00Z|expected 7 columns"})
    void rejectsBadRowsWithAReason(String line, String reasonFragment) {
        var r = RowValidator.validate(line);
        assertThat(r.valid()).isFalse();
        assertThat(r.error()).contains(reasonFragment);
    }
}
