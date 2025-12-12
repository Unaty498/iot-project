# Configuration du Projet IoT

## Fichier de Configuration

Le projet utilise un fichier de configuration centralisé pour gérer les noms des buckets S3 et des queues SQS.

### Emplacement
Le fichier de configuration se trouve dans : 
```
iot-shared/src/main/resources/config.properties
```

### Contenu du fichier

```properties
# Configuration IoT Project
# ========================

# AWS Region
aws.region=us-east-1

# S3 Buckets
bucket.raw=iot-raw-grp13-1
bucket.interim=iot-interim-grp13-1
bucket.state=iot-state-grp13-1

# SQS Queues
queue.summarize=https://sqs.us-east-1.amazonaws.com/710771987572/queue-summarize
queue.consolidate=https://sqs.us-east-1.amazonaws.com/710771987572/queue-consolidate.fifo
```

## Utilisation

La classe `ConfigLoader` dans le module `iot-shared` permet de charger ces valeurs de manière centralisée.

### Priorité de chargement

Les valeurs de configuration sont chargées avec la priorité suivante :
1. **Variables d'environnement** (nom en majuscules avec `_` au lieu de `.`)
2. **Fichier config.properties**
3. **Valeur par défaut** (si définie)

### Exemples

Pour utiliser une configuration différente via les variables d'environnement :

```bash
export BUCKET_RAW=mon-bucket-raw
export BUCKET_INTERIM=mon-bucket-interim
export BUCKET_STATE=mon-bucket-state
export AWS_REGION=eu-west-1
export QUEUE_SUMMARIZE=https://sqs.eu-west-1.amazonaws.com/123456789/ma-queue-summarize
export QUEUE_CONSOLIDATE=https://sqs.eu-west-1.amazonaws.com/123456789/ma-queue-consolidate.fifo
```

### Méthodes disponibles

- `ConfigLoader.getAwsRegion()` - Région AWS
- `ConfigLoader.getBucketRaw()` - Bucket pour données brutes
- `ConfigLoader.getBucketInterim()` - Bucket pour données intermédiaires
- `ConfigLoader.getBucketState()` - Bucket pour l'état consolidé
- `ConfigLoader.getQueueSummarize()` - Queue pour la summarisation
- `ConfigLoader.getQueueConsolidate()` - Queue pour la consolidation
- `ConfigLoader.printConfig()` - Affiche toute la configuration (pour le débogage)

## Migration depuis les anciennes versions

Les anciennes versions utilisaient des valeurs hardcodées dans chaque classe. Avec cette nouvelle architecture :

1. ✅ **Centralisation** : Toute la configuration est au même endroit
2. ✅ **Flexibilité** : Possibilité de surcharger via variables d'environnement
3. ✅ **Maintenance** : Plus besoin de modifier le code pour changer un nom de bucket
4. ✅ **Sécurité** : Validation des valeurs requises au démarrage

## Compilation

Après avoir modifié le fichier de configuration, recompilez le projet :

```bash
mvn clean package
```

Le fichier `config.properties` sera automatiquement inclus dans le JAR du module `iot-shared`, qui est lui-même inclus dans les JARs finaux via le maven-shade-plugin.

