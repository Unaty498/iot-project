package com.iot.shared;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents the historical statistics for a Src-Dst pair.
 * Stored in S3 bucket-state.
 */
public record TrafficState(
        @JsonProperty("src_ip") String srcIp,
        @JsonProperty("dst_ip") String dstIp,

        // N: How many days/updates have we seen?
        @JsonProperty("count") long count,

        // Statistics for Flow Duration
        @JsonProperty("sum_duration") double sumDuration,
        @JsonProperty("sum_sq_duration") double sumSqDuration,

        // Statistics for Forward Packets
        @JsonProperty("sum_packets") double sumPackets,
        @JsonProperty("sum_sq_packets") double sumSqPackets
) {
    // Helper to create an empty state for a new IP pair
    public static TrafficState empty(String src, String dst) {
        return new TrafficState(src, dst, 0, 0.0, 0.0, 0.0, 0.0);
    }
}