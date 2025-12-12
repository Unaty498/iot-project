package com.iot.ingestion;

import com.iot.shared.ConfigLoader;
import com.iot.shared.IntermediateSummary;
import com.iot.shared.JsonUtils;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.eventnotifications.s3.model.S3EventNotification;
import software.amazon.awssdk.eventnotifications.s3.model.S3EventNotificationRecord;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class SummarizeWorker {

    private static final String QUEUE_URL = ConfigLoader.getQueueSummarize();
    private static final String INTERIM_BUCKET = ConfigLoader.getBucketInterim();
    private static final String NEXT_QUEUE_URL = ConfigLoader.getQueueConsolidate();

    // Required Column Names (Exact match to your schema)
    private static final String COL_TIMESTAMP = "Timestamp";
    private static final String COL_SRC_IP = "Src IP";
    private static final String COL_DST_IP = "Dst IP";
    private static final String COL_FLOW_DURATION = "Flow Duration";
    private static final String COL_FWD_PKTS = "Tot Fwd Pkts";

    private final S3Client s3;
    private final SqsClient sqs;

    public SummarizeWorker() {
        Region region = Region.of(ConfigLoader.getAwsRegion());
        this.s3 = S3Client.builder().region(region).build();
        this.sqs = SqsClient.builder().region(region).build();
    }

    public void start() {
        System.out.println("Summarize Worker Started. Polling SQS...");
        while (true) {
            try {
                ReceiveMessageResponse response = sqs.receiveMessage(ReceiveMessageRequest.builder()
                        .queueUrl(QUEUE_URL)
                        .maxNumberOfMessages(1)
                        .waitTimeSeconds(20)
                        .build());

                for (Message message : response.messages()) {
                    processMessage(message);
                    sqs.deleteMessage(req -> req.queueUrl(QUEUE_URL).receiptHandle(message.receiptHandle()));
                }
            } catch (Exception e) {
                System.err.println("Error processing messages: " + e.getMessage());
            }
        }
    }

    private void processMessage(Message sqsMessage) {
        try {
            S3EventNotification notification = S3EventNotification.fromJson(sqsMessage.body());
            if (notification.getRecords() == null) return;

            for (S3EventNotificationRecord record : notification.getRecords()) {
                String bucket = record.getS3().getBucket().getName();
                String key = java.net.URLDecoder.decode(record.getS3().getObject().getKey(), StandardCharsets.UTF_8);
                System.out.println("Processing file: " + key);
                processCsvFile(bucket, key);
            }
        } catch (Exception e) {
            System.err.println("Error parsing SQS message: " + e.getMessage());
        }
    }

    private void processCsvFile(String bucket, String key) {
        try (ResponseInputStream<GetObjectResponse> s3Stream = s3.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build());
             BufferedReader reader = new BufferedReader(new InputStreamReader(s3Stream, StandardCharsets.UTF_8))) {

            // 1. Parse Header to find indices
            String headerLine = reader.readLine();
            if (headerLine == null) return;

            Map<String, Integer> colMap = mapHeaders(headerLine);
            if (!validateHeaders(colMap)) {
                System.err.println("Skipping file " + key + ": Missing required columns.");
                return;
            }

            // 2. Process Rows
            Map<String, IntermediateSummary> aggregations = new HashMap<>();
            String line;

            while ((line = reader.readLine()) != null) {
                try {
                    // Handle CSV splitting (including potential quotes if necessary, but simple split for now)
                    String[] cols = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

                    String rawDate = cols[colMap.get(COL_TIMESTAMP)];
                    String srcIp = cols[colMap.get(COL_SRC_IP)];
                    String dstIp = cols[colMap.get(COL_DST_IP)];
                    String rawDur = cols[colMap.get(COL_FLOW_DURATION)];
                    String rawPkts = cols[colMap.get(COL_FWD_PKTS)];

                    // Normalize Date (e.g., "01/11/2023 10:00" -> "2023-11-01")
                    String dateKey = normalizeDate(rawDate);

                    // Parse Numbers safely
                    long duration = parseLongSafe(rawDur);
                    long packets = parseLongSafe(rawPkts);

                    String mapKey = srcIp + ":" + dstIp + ":" + dateKey;

                    aggregations.compute(mapKey, (k, v) -> {
                        if (v == null) return new IntermediateSummary(srcIp, dstIp, dateKey, duration, packets);
                        return new IntermediateSummary(srcIp, dstIp, dateKey,
                                v.totalFlowDuration() + duration,
                                v.totalFwdPackets() + packets);
                    });

                } catch (Exception e) {
                    // Log but don't stop processing the whole file
                    // System.err.println("Skipping bad line: " + e.getMessage());
                }
            }

            // 3. Upload Results
            for (IntermediateSummary summary : aggregations.values()) {
                uploadAndNotify(summary);
            }

        } catch (Exception e) {
            System.err.println("Failed to process file " + key + ": " + e.getMessage());
        }
    }

    private Map<String, Integer> mapHeaders(String headerLine) {
        Map<String, Integer> map = new HashMap<>();
        String[] headers = headerLine.split(",");
        for (int i = 0; i < headers.length; i++) {
            map.put(headers[i].trim(), i);
        }
        return map;
    }

    private boolean validateHeaders(Map<String, Integer> map) {
        return map.containsKey(COL_TIMESTAMP) && map.containsKey(COL_SRC_IP) &&
                map.containsKey(COL_DST_IP) && map.containsKey(COL_FLOW_DURATION) &&
                map.containsKey(COL_FWD_PKTS);
    }

    private String normalizeDate(String timestamp) {
        // Attempt to parse standard formats.
        // VARIoT often uses: dd/MM/yyyy hh:mm:ss a OR yyyy-MM-dd HH:mm:ss
        // Simple approach: Split by space to get Date part, then reformat if needed.
        try {
            String datePart = timestamp.split(" ")[0]; // "2023-11-01" or "01/11/2023"
            if (datePart.contains("/")) {
                String[] parts = datePart.split("/");
                // Assuming dd/MM/yyyy -> yyyy-MM-dd
                if (parts.length == 3) {
                    return parts[2] + "-" + parts[1] + "-" + parts[0];
                }
            }
            return datePart;
        } catch (Exception e) {
            return "unknown-date";
        }
    }

    private long parseLongSafe(String value) {
        try {
            if (value == null || value.trim().isEmpty()) return 0;
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private void uploadAndNotify(IntermediateSummary summary) {
        String jsonFileName = "summary-" + UUID.randomUUID() + ".json";
        String jsonBody = JsonUtils.toJson(summary);

        s3.putObject(PutObjectRequest.builder().bucket(INTERIM_BUCKET).key(jsonFileName).build(),
                software.amazon.awssdk.core.sync.RequestBody.fromString(jsonBody));

        sqs.sendMessage(SendMessageRequest.builder()
                .queueUrl(NEXT_QUEUE_URL)
                .messageBody(jsonFileName)
                .messageGroupId(summary.srcIp())
                .messageDeduplicationId(jsonFileName)
                .build());
    }

    public static void main(String[] args) {
        new SummarizeWorker().start();
    }
}