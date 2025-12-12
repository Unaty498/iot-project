package com.iot.ingestion;

import com.iot.shared.ConfigLoader;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.nio.file.Paths;
import java.util.UUID;

public class UploadClient {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java -cp ... UploadClient <path-to-csv>");
            System.exit(1);
        }

        String bucketName = ConfigLoader.getBucketRaw();
        Region region = Region.of(ConfigLoader.getAwsRegion());

        String filePath = args[0];
        String key = "traffic-data-" + UUID.randomUUID() + ".csv"; // Unique name

        System.out.println("Uploading " + filePath + " to " + bucketName + "...");

        try (S3Client s3 = S3Client.builder().region(region).build()) {

            s3.putObject(PutObjectRequest.builder()
                            .bucket(bucketName)
                            .key(key)
                            .build(),
                    Paths.get(filePath));

            System.out.println("Success! File uploaded as: " + key);

        } catch (Exception e) {
            System.err.println("Upload failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}