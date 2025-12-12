package com.iot.shared;

import java.io.InputStream;
import java.util.Properties;

/**
 * Centralized configuration loader for the IoT project.
 * Loads configuration from config.properties in the classpath.
 */
public class ConfigLoader {

    private static final Properties props = new Properties();
    private static boolean loaded = false;

    static {
        loadConfig();
    }

    private static void loadConfig() {
        if (loaded) return;

        try (InputStream in = ConfigLoader.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (in == null) {
                System.err.println("AVERTISSEMENT: Fichier config.properties introuvable. Utilisation des variables d'environnement uniquement.");
            } else {
                props.load(in);
                System.out.println("Configuration chargée depuis config.properties");
            }
            loaded = true;
        } catch (Exception e) {
            System.err.println("Erreur lors du chargement de config.properties: " + e.getMessage());
        }
    }

    /**
     * Get a configuration value. Priority: Environment Variable > config.properties > default value
     */
    private static String get(String key, String defaultValue) {
        String envValue = System.getenv(key.toUpperCase().replace('.', '_'));
        if (envValue != null && !envValue.isEmpty()) {
            return envValue;
        }
        return props.getProperty(key, defaultValue);
    }

    /**
     * Get a required configuration value. Throws exception if not found.
     */
    private static String getRequired(String key) {
        String value = get(key, null);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalStateException("Configuration requise manquante: " + key);
        }
        return value;
    }

    // AWS Region
    public static String getAwsRegion() {
        return get("aws.region", "us-east-1");
    }

    // S3 Buckets
    public static String getBucketRaw() {
        return getRequired("bucket.raw");
    }

    public static String getBucketInterim() {
        return getRequired("bucket.interim");
    }

    public static String getBucketState() {
        return getRequired("bucket.state");
    }

    // SQS Queues
    public static String getQueueSummarize() {
        return getRequired("queue.summarize");
    }

    public static String getQueueConsolidate() {
        return getRequired("queue.consolidate");
    }

    // Utility method to display all loaded config (for debugging)
    public static void printConfig() {
        System.out.println("=== Configuration IoT ===");
        System.out.println("AWS Region: " + getAwsRegion());
        System.out.println("Bucket Raw: " + getBucketRaw());
        System.out.println("Bucket Interim: " + getBucketInterim());
        System.out.println("Bucket State: " + getBucketState());
        System.out.println("Queue Summarize: " + getQueueSummarize());
        System.out.println("Queue Consolidate: " + getQueueConsolidate());
        System.out.println("========================");
    }
}

