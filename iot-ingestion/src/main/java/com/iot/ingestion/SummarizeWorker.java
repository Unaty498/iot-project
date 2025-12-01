package com.iot.ingestion;

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
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class SummarizeWorker {

    // --- CONFIGURATION ---
    // REPLACE THESE WITH YOUR ACTUAL RESOURCE NAMES
    private static final String QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/YOUR_ACCOUNT_ID/queue-summarize";
    private static final String INTERIM_BUCKET = "iot-lab-interim-grp13-1";
    private static final String NEXT_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/YOUR_ACCOUNT_ID/queue-consolidate.fifo";

    private final S3Client s3;
    private final SqsClient sqs;

    public SummarizeWorker() {
        // Clients automatically use the EC2 Instance Profile (LabRole)
        this.s3 = S3Client.builder().region(Region.US_EAST_1).build();
        this.sqs = SqsClient.builder().region(Region.US_EAST_1).build();
    }

    public void start() {
        System.out.println("Summarize Worker Started. Polling SQS...");

        while (true) {
            try {
                // 1. Long Poll for messages (Wait up to 20s for a message)
                ReceiveMessageRequest listenReq = ReceiveMessageRequest.builder()
                        .queueUrl(QUEUE_URL)
                        .maxNumberOfMessages(1)
                        .waitTimeSeconds(20)
                        .build();

                ReceiveMessageResponse response = sqs.receiveMessage(listenReq);

                for (Message message : response.messages()) {
                    System.out.println("Received message: " + message.messageId());
                    processMessage(message);

                    // 2. Delete message only if processed successfully
                    sqs.deleteMessage(req -> req.queueUrl(QUEUE_URL).receiptHandle(message.receiptHandle()));
                    System.out.println("Message deleted.");
                }
            } catch (Exception e) {
                e.printStackTrace();
                // Sleep a bit to avoid hammering AWS if something is broken
                try { Thread.sleep(5000); } catch (InterruptedException ignored) {}
            }
        }
    }

    private void processMessage(Message sqsMessage) {
        // 3. Parse the S3 Event from the SQS Body
        S3EventNotification notification = S3EventNotification.fromJson(sqsMessage.body());

        // The event might contain multiple records (files), usually just one
        if (notification.getRecords() == null) return;

        for (S3EventNotificationRecord record : notification.getRecords()) {
            String bucketName = record.getS3().getBucket().getName();
            String fileKey = record.getS3().getObject().getKey();

            System.out.println("Processing file: " + fileKey + " from bucket: " + bucketName);
            processCsvFile(bucketName, fileKey);
        }
    }

    private void processCsvFile(String bucket, String key) {
        // 4. Download CSV Stream (Don't load the whole file into RAM!)
        try (ResponseInputStream<GetObjectResponse> s3Stream = s3.getObject(GetObjectRequest.builder()
                .bucket(bucket).key(key).build());
             BufferedReader reader = new BufferedReader(new InputStreamReader(s3Stream, StandardCharsets.UTF_8))) {

            // Map Key: "SrcIP:DstIP:Date" -> Summary Object
            Map<String, IntermediateSummary> aggregations = new HashMap<>();

            String line;
            // Skip header if your CSV has one
            // reader.readLine();

            while ((line = reader.readLine()) != null) {
                // ASSUMPTION: CSV Format = Date, SrcIP, DstIP, FlowDuration, FwdPackets
                // Adjust indices based on your ACTUAL CSV format
                String[] cols = line.split(",");
                if (cols.length < 5) continue;

                String date = cols[0]; // e.g., 2023-10-27
                String src = cols[1];
                String dst = cols[2];
                long duration = Long.parseLong(cols[3]);
                long packets = Long.parseLong(cols[4]);

                String mapKey = src + ":" + dst + ":" + date;

                aggregations.compute(mapKey, (k, v) -> {
                    if (v == null) return new IntermediateSummary(src, dst, date, duration, packets);
                    return new IntermediateSummary(src, dst, date,
                            v.totalFlowDuration() + duration,
                            v.totalFwdPackets() + packets);
                });
            }

            // 5. Upload Results & Notify Next Worker
            for (IntermediateSummary summary : aggregations.values()) {
                uploadAndNotify(summary);
            }

        } catch (Exception e) {
            System.err.println("Failed to process file " + key + ": " + e.getMessage());
            throw new RuntimeException(e); // Throwing ensures the SQS message isn't deleted, so it retries
        }
    }

    private void uploadAndNotify(IntermediateSummary summary) {
        // Generate a unique filename for the interim bucket
        String jsonFileName = "summary-" + UUID.randomUUID() + ".json";
        String jsonBody = JsonUtils.toJson(summary);

        // A. Upload JSON to Interim Bucket
        s3.putObject(PutObjectRequest.builder()
                        .bucket(INTERIM_BUCKET)
                        .key(jsonFileName)
                        .build(),
                software.amazon.awssdk.core.sync.RequestBody.fromString(jsonBody));

        // B. Send Message to Consolidate Queue (FIFO)
        // CRITICAL: MessageGroupId ensures sequential processing for this IP pair
        sqs.sendMessage(SendMessageRequest.builder()
                .queueUrl(NEXT_QUEUE_URL)
                .messageBody(jsonFileName) // Send just the reference (key)
                .messageGroupId(summary.srcIp()) // Group by Source IP
                .messageDeduplicationId(jsonFileName) // Prevent duplicates
                .build());

        System.out.println("Exported summary for " + summary.srcIp() + " -> " + summary.dstIp());
    }

    public static void main(String[] args) {
        new SummarizeWorker().start();
    }
}