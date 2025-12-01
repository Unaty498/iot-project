package com.iot.analytics;

import com.iot.shared.JsonUtils;
import com.iot.shared.TrafficState;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;

public class ExportClient {

    // --- CONFIGURATION ---
    private static final String BUCKET_STATE = "iot-state-grp13-1";
    private static final String OUTPUT_FILE = "report.csv";

    private final S3Client s3;

    public ExportClient() {
        this.s3 = S3Client.builder().region(Region.US_EAST_1).build();
    }

    public static void main(String[] args) {
        new ExportClient().generateReport();
    }

    public void generateReport() {
        System.out.println("Début de la génération du rapport...");

        try (PrintWriter writer = new PrintWriter(new FileWriter(OUTPUT_FILE))) {
            writer.println("Date,SrcIP,DstIP,Avg_FlowDuration,StdDev_FlowDuration,Avg_FwdPackets,StdDev_FwdPackets,Total_Updates");

            ListObjectsV2Request listReq = ListObjectsV2Request.builder()
                    .bucket(BUCKET_STATE)
                    .build();

            ListObjectsV2Response listRes = s3.listObjectsV2(listReq);

            for (S3Object s3Obj : listRes.contents()) {
                System.out.println("Lecture de : " + s3Obj.key());
                TrafficState state = loadState(s3Obj.key());

                String csvLine = convertToCsv(state);
                writer.println(csvLine);
            }

            System.out.println("Rapport généré avec succès : " + OUTPUT_FILE);

        } catch (IOException e) {
            System.err.println("Erreur d'écriture du fichier : " + e.getMessage());
        }
    }

    private TrafficState loadState(String key) {
        InputStream stream = s3.getObject(b -> b.bucket(BUCKET_STATE).key(key),
                ResponseTransformer.toInputStream());
        return JsonUtils.fromJson(stream, TrafficState.class);
    }

    private String convertToCsv(TrafficState state) {
        long N = state.count();
        if (N == 0) return "0,0,0,0,0,0,0,0";

        double avgDuration = state.sumDuration() / N;
        double avgPackets = state.sumPackets() / N;

        double varDuration = (state.sumSqDuration() / N) - Math.pow(avgDuration, 2);
        double varPackets = (state.sumSqPackets() / N) - Math.pow(avgPackets, 2);

        double stdDevDuration = Math.sqrt(Math.max(0, varDuration));
        double stdDevPackets = Math.sqrt(Math.max(0, varPackets));

        String dateReport = java.time.LocalDate.now().toString();

        return String.format("%s,%s,%s,%.2f,%.2f,%.2f,%.2f,%d",
                dateReport,
                state.srcIp(),
                state.dstIp(),
                avgDuration,
                stdDevDuration,
                avgPackets,
                stdDevPackets,
                N
        );
    }
}