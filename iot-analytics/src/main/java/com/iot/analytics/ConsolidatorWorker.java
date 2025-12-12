package com.iot.analytics;

import com.iot.shared.ConfigLoader;
import com.iot.shared.IntermediateSummary;
import com.iot.shared.JsonUtils;
import com.iot.shared.TrafficState;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ConsolidatorWorker {

    private static final String QUEUE_URL = ConfigLoader.getQueueConsolidate();
    private static final String BUCKET_INTERIM = ConfigLoader.getBucketInterim();
    private static final String BUCKET_STATE = ConfigLoader.getBucketState();

    private final SqsClient sqs;
    private final S3Client s3;

    public ConsolidatorWorker() {
        Region region = Region.of(ConfigLoader.getAwsRegion());
        this.sqs = SqsClient.builder().region(region).build();
        this.s3 = S3Client.builder().region(region).build();
    }

    public void start() {
        System.out.println("Consolidator Worker Started (FIFO Mode).");

        while (true) {
            try {
                // Ensure we respect the FIFO order
                List<Message> messages = sqs.receiveMessage(ReceiveMessageRequest.builder()
                        .queueUrl(QUEUE_URL)
                        .maxNumberOfMessages(1)
                        .waitTimeSeconds(20)
                        .build()).messages();

                for (Message msg : messages) {
                    processMessage(msg);
                }
            } catch (Exception e) {
                System.err.println("Main loop error: " + e.getMessage());
                try { Thread.sleep(5000); } catch (InterruptedException ignored) {}
            }
        }
    }

    private void processMessage(Message msg) {
        String interimKey = msg.body();
        System.out.println("Consolidating summary: " + interimKey);

        try {
            // 1. Fetch Interim Summary
            InputStream s3Stream = s3.getObject(b -> b.bucket(BUCKET_INTERIM).key(interimKey), ResponseTransformer.toInputStream());
            IntermediateSummary summary = JsonUtils.fromJson(s3Stream, IntermediateSummary.class);

            // 2. Load History
            String stateKey = "state/" + summary.srcIp() + "_" + summary.dstIp() + ".json";
            TrafficState currentState = loadState(stateKey, summary.srcIp(), summary.dstIp());

            // 3. Update Math (Welford's Logic / Sum of Squares)
            TrafficState newState = updateState(currentState, summary);

            // 4. Save & Clean
            saveState(stateKey, newState);

            // Delete interim file to satisfy "Least Storage" requirement
            s3.deleteObject(b -> b.bucket(BUCKET_INTERIM).key(interimKey));

            // Acknowledge message
            sqs.deleteMessage(b -> b.queueUrl(QUEUE_URL).receiptHandle(msg.receiptHandle()));

        } catch (Exception e) {
            System.err.println("Failed to consolidate " + interimKey + ": " + e.getMessage());
            // We do NOT delete the message here, so it retries later (Resilience)
        }
    }

    private TrafficState loadState(String key, String src, String dst) {
        try {
            InputStream stream = s3.getObject(b -> b.bucket(BUCKET_STATE).key(key), ResponseTransformer.toInputStream());
            return JsonUtils.fromJson(stream, TrafficState.class);
        } catch (NoSuchKeyException e) {
            return TrafficState.empty(src, dst);
        }
    }

    private void saveState(String key, TrafficState state) {
        s3.putObject(b -> b.bucket(BUCKET_STATE).key(key),
                RequestBody.fromString(JsonUtils.toJson(state), StandardCharsets.UTF_8));
    }

    private TrafficState updateState(TrafficState current, IntermediateSummary input) {
        // Protect against bad data
        long duration = Math.max(0, input.totalFlowDuration());
        long packets = Math.max(0, input.totalFwdPackets());

        return new TrafficState(
                current.srcIp(),
                current.dstIp(),
                current.count() + 1,
                current.sumDuration() + duration,
                current.sumSqDuration() + Math.pow(duration, 2),
                current.sumPackets() + packets,
                current.sumSqPackets() + Math.pow(packets, 2)
        );
    }

    public static void main(String[] args) {
        new ConsolidatorWorker().start();
    }
}