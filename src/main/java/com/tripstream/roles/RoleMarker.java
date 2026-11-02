package com.tripstream.roles;

/** Identifies which role this process is running. Exactly one exists per running profile. */
public record RoleMarker(String role) {
}
