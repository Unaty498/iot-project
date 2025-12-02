package com.iot.analytics;

import com.iot.shared.JsonUtils;
import com.iot.shared.TrafficState;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.InputStream;

public class ExportClient {

    // Utilisez une variable d'environnement ou la valeur par défaut
    private static final String STATE_BUCKET = System.getenv().getOrDefault("BUCKET_STATE", "iot-state-grp13-1");

    public static void main(String[] args) {
        // Le client hérite des droits du LabRole ou de votre profil local
        S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build();

        // En-tête du CSV final
        System.out.println("SrcIP,DstIP,Count,AvgDuration,StdDevDuration,AvgPackets,StdDevPackets");

        try {
            ListObjectsV2Response listing = s3.listObjectsV2(req -> req.bucket(STATE_BUCKET).prefix("state/"));

            for (S3Object obj : listing.contents()) {
                // Ignorez les dossiers ou fichiers vides
                if (obj.size() == 0) continue;

                try (InputStream is = s3.getObject(req -> req.bucket(STATE_BUCKET).key(obj.key()))) {
                    TrafficState state = JsonUtils.fromJson(is, TrafficState.class);
                    printStats(state);
                } catch (Exception e) {
                    System.err.println("Erreur de lecture pour " + obj.key() + " : " + e.getMessage());
                }
            }
        } catch (Exception e) {
            System.err.println("Impossible de lister le bucket " + STATE_BUCKET + " : " + e.getMessage());
        }
    }

    private static void printStats(TrafficState s) {
        if (s.count() <= 0) return; // Eviter la division par zéro

        // 1. Calcul des Moyennes
        double avgDur = s.sumDuration() / s.count();
        double avgPkt = s.sumPackets() / s.count();

        // 2. Calcul des Ecart-Types (Standard Deviation)
        // Formule : Variance = E[X^2] - (E[X])^2
        double meanSqDur = s.sumSqDuration() / s.count();
        double varianceDur = meanSqDur - (avgDur * avgDur);

        // Math.max(0, ...) est CRITIQUE ici pour éviter le NaN sur des variances type -0.00000001
        double stdDevDur = Math.sqrt(Math.max(0, varianceDur));

        double meanSqPkt = s.sumSqPackets() / s.count();
        double variancePkt = meanSqPkt - (avgPkt * avgPkt);
        double stdDevPkt = Math.sqrt(Math.max(0, variancePkt));

        // 3. Sortie formatée (Locale US pour avoir des points et non des virgules)
        System.out.printf(java.util.Locale.US, "%s,%s,%d,%.2f,%.2f,%.2f,%.2f%n",
                s.srcIp(),
                s.dstIp(),
                s.count(),
                avgDur,
                stdDevDur,
                avgPkt,
                stdDevPkt);
    }
}