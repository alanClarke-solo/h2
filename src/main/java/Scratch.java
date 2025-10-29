import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Scratch file: Kafka Producer version of the original JDBC-based cache put() snippet.
 *
 * This snippet "fixes the snippet assuming we are dealing with Kafka Producer" by replacing
 * JDBC upsert/index maintenance with a Kafka send that carries:
 *  - uniqueStringId (derived from key/id/params)
 *  - key (primary unique key)
 *  - id (optional)
 *  - parameters (hierarchical param patterns)
 *  - ttlMillis
 *  - payload bytes
 *
 * Notes:
 *  - This is a standalone scratch demonstrating how the operation would look with Kafka.
 *  - It is not wired into the project's build; adjust as needed in your application.
 */
public class Scratch {

    // Example lightweight SearchParameter to keep this scratch self-contained.
    public static final class SearchParameter {
        private final String name;
        private final String value;
        private final int level;

        public SearchParameter(String name, String value, int level) {
            this.name = Objects.requireNonNull(name, "name");
            this.value = Objects.requireNonNull(value, "value");
            this.level = level;
        }

        public String name() { return name; }
        public String value() { return value; }
        public int level() { return level; }
    }

    /**
     * Minimal serializer hook. Replace with your preferred object serializer.
     * In production you might use Kryo, FST, or a custom scheme. Here we use UTF-8 for demo.
     */
    static byte[] serializeValue(Object value) {
        // Replace this with your Kryo5 serialization if needed.
        return String.valueOf(value).getBytes(StandardCharsets.UTF_8);
    }

    static byte[] serializeParameters(List<SearchParameter> parameters) {
        // Serialize as a compact string; replace with more robust serialization for production
        StringBuilder sb = new StringBuilder();
        for (SearchParameter p : parameters) {
            if (sb.length() > 0) sb.append('|');
            sb.append('L').append(p.level()).append(':')
              .append(p.name()).append('=').append(p.value());
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    static Set<String> generateHierarchicalPatterns(List<SearchParameter> params) {
        // Create patterns like L0:region=US, L0:region=US>L1:category=electronics, etc.
        // Order by level, then progressively combine prefixes.
        List<SearchParameter> sorted = new ArrayList<>(params);
        sorted.sort(Comparator.comparingInt(SearchParameter::level));
        List<String> tokens = new ArrayList<>();
        for (SearchParameter p : sorted) {
            tokens.add("L" + p.level() + ":" + p.name() + "=" + p.value());
        }
        Set<String> patterns = new LinkedHashSet<>();
        for (int i = 0; i < tokens.size(); i++) {
            StringBuilder prefix = new StringBuilder();
            for (int j = 0; j <= i; j++) {
                if (j > 0) prefix.append('>');
                prefix.append(tokens.get(j));
            }
            patterns.add(prefix.toString());
        }
        // Also add single tokens to allow non-prefix combinations
        patterns.addAll(tokens);
        return patterns;
    }

    static String generateUniqueId(String key, Long id, List<SearchParameter> parameters) {
        StringBuilder sb = new StringBuilder();
        sb.append(Objects.toString(key, ""))
          .append('|').append(Objects.toString(id, ""))
          .append('|');
        parameters.stream()
                .sorted(Comparator.comparingInt(SearchParameter::level)
                        .thenComparing(SearchParameter::name)
                        .thenComparing(SearchParameter::value))
                .forEach(p -> sb.append('[')
                        .append('L').append(p.level())
                        .append(':').append(p.name())
                        .append('=').append(p.value())
                        .append(']'));
        return UUID.nameUUIDFromBytes(sb.toString().getBytes(StandardCharsets.UTF_8)).toString();
    }

    public static final class KafkaCacheProducer implements AutoCloseable {
        private final KafkaProducer<String, byte[]> producer;
        private final String topic;

        public KafkaCacheProducer(String bootstrapServers, String topic) {
            this.topic = Objects.requireNonNull(topic, "topic");
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Objects.requireNonNull(bootstrapServers, "bootstrapServers"));
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            // Safer defaults similar to an upsert in DB: ensure delivery at least once with idempotence
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.RETRIES_CONFIG, 5);
            props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
            props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
            props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32_768); // 32KB
            this.producer = new KafkaProducer<>(props);
        }

        /**
         * Kafka version of:
         *   put(String key, Long id, List<SearchParameter> parameters, T value, long ttlMillis)
         *
         * It produces a message that can be consumed by a downstream cache indexer or materializer.
         * If key or parameters are null, IllegalArgumentException is thrown (same contract as original).
         */
        public <T> RecordMetadata put(String key, Long id, List<SearchParameter> parameters, T value, long ttlMillis) {
            if (key == null || parameters == null || value == null) {
                throw new IllegalArgumentException("Key, parameters, and value cannot be null");
            }
            String uniqueStringId = generateUniqueId(key, id, parameters);
            byte[] serializedValue = serializeValue(value);
            byte[] serializedParameters = serializeParameters(parameters);
            Set<String> patterns = generateHierarchicalPatterns(parameters);

            // Build headers to carry metadata
            List<Header> headers = new ArrayList<>();
            headers.add(new RecordHeader("uniqueStringId", uniqueStringId.getBytes(StandardCharsets.UTF_8)));
            headers.add(new RecordHeader("valueType", value.getClass().getName().getBytes(StandardCharsets.UTF_8)));
            headers.add(new RecordHeader("ttlMillis", Long.toString(ttlMillis).getBytes(StandardCharsets.UTF_8)));
            if (id != null) {
                headers.add(new RecordHeader("id", Long.toString(id).getBytes(StandardCharsets.UTF_8)));
            }
            headers.add(new RecordHeader("parameters", serializedParameters));
            headers.add(new RecordHeader("patterns", String.join(",", patterns).getBytes(StandardCharsets.UTF_8)));

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, null, System.currentTimeMillis(), key, serializedValue, headers);
            CompletableFuture<RecordMetadata> fut = new CompletableFuture<>();
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        fut.completeExceptionally(exception);
                    } else {
                        fut.complete(metadata);
                    }
                }
            });
            try {
                return fut.get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException("Failed to send Kafka message", e);
            }
        }

        /**
         * getOrCompute variant that first tries to produce an upsert request; if needed, computes via supplier.
         * In a pure producer context, this returns the computed value after ensuring the produce step succeeded.
         */
        public <T> T getOrComputeAndPut(String key, Long id, List<SearchParameter> parameters, long ttlMillis, Supplier<T> supplier) {
            T value = Objects.requireNonNull(supplier, "supplier").get();
            put(key, id, parameters, value, ttlMillis);
            return value;
        }

        @Override
        public void close() {
            producer.flush();
            producer.close(Duration.ofSeconds(5));
        }
    }

    // Simple demo usage (not executed here):
    public static void main(String[] args) {
        List<SearchParameter> params = List.of(
                new SearchParameter("region", "US", 0),
                new SearchParameter("category", "electronics", 1),
                new SearchParameter("brand", "apple", 2),
                new SearchParameter("product", "cellphone", 3)
        );
        try (KafkaCacheProducer kp = new KafkaCacheProducer("localhost:9092", "cache-upserts")) {
            kp.put("iphone17s", 2371L, params, "specification sheet for iphone17s", 86_400_000L);
        }
    }
}
