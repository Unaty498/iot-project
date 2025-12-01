package com.iot.analytics;

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

    // --- CONFIGURATION ---
    private static final String QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/710771987572/queue-consolidate.fifo";
    private static final String BUCKET_INTERIM = "iot-interim-grp13-1";
    private static final String BUCKET_STATE = "iot-state-grp13-1";

    private final SqsClient sqs;
    private final S3Client s3;

    public ConsolidatorWorker() {
        this.sqs = SqsClient.builder().region(Region.US_EAST_1).build();
        this.s3 = S3Client.builder().region(Region.US_EAST_1).build();
    }

    public void start() {
        System.out.println("Consolidator Worker Démarré. En attente de messages...");

        while (true) {
            try {
                List<Message> messages = sqs.receiveMessage(ReceiveMessageRequest.builder()
                        .queueUrl(QUEUE_URL)
                        .maxNumberOfMessages(1)
                        .waitTimeSeconds(20)
                        .build()).messages();

                for (Message msg : messages) {
                    processMessage(msg);
                }

            } catch (Exception e) {
                System.err.println("Erreur dans la boucle principale : " + e.getMessage());
                try { Thread.sleep(5000); } catch (InterruptedException ignored) {}
            }
        }
    }

    private void processMessage(Message msg) {
        String interimFileKey = msg.body();
        System.out.println("Traitement du fichier intermédiaire : " + interimFileKey);

        try {
            InputStream s3Stream = s3.getObject(b -> b.bucket(BUCKET_INTERIM).key(interimFileKey),
                    ResponseTransformer.toInputStream());
            IntermediateSummary summary = JsonUtils.fromJson(s3Stream, IntermediateSummary.class);

            String stateKey = summary.srcIp() + "-" + summary.dstIp() + ".json";
            TrafficState currentState = loadState(stateKey, summary.srcIp(), summary.dstIp());

            TrafficState newState = updateState(currentState, summary);

            saveState(stateKey, newState);

            s3.deleteObject(b -> b.bucket(BUCKET_INTERIM).key(interimFileKey));
            sqs.deleteMessage(b -> b.queueUrl(QUEUE_URL).receiptHandle(msg.receiptHandle()));

            System.out.println("Mise à jour réussie pour : " + stateKey);

        } catch (Exception e) {
            System.err.println("Echec du traitement pour " + interimFileKey + ": " + e.getMessage());
            e.printStackTrace();
        }
    }

    private TrafficState loadState(String key, String srcIp, String dstIp) {
        try {
            InputStream stream = s3.getObject(b -> b.bucket(BUCKET_STATE).key(key),
                    ResponseTransformer.toInputStream());
            return JsonUtils.fromJson(stream, TrafficState.class);
        } catch (NoSuchKeyException e) {
            return TrafficState.empty(srcIp, dstIp);
        }
    }

    private void saveState(String key, TrafficState state) {
        s3.putObject(b -> b.bucket(BUCKET_STATE).key(key),
                RequestBody.fromString(JsonUtils.toJson(state), StandardCharsets.UTF_8));
    }

    private TrafficState updateState(TrafficState current, IntermediateSummary input) {
        return new TrafficState(
                current.srcIp(),
                current.dstIp(),
                current.count() + 1,
                current.sumDuration() + input.totalFlowDuration(),
                current.sumSqDuration() + Math.pow(input.totalFlowDuration(), 2),
                current.sumPackets() + input.totalFwdPackets(),
                current.sumSqPackets() + Math.pow(input.totalFwdPackets(), 2)
        );
    }

    public static void main(String[] args) {
        new ConsolidatorWorker().start();
    }
}