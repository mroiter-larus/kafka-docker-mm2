import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class TestOffsetMm2 {
    public static void main(String[] args) {

        final Properties props = new Properties();

        /*
            Use one of the following urls depending on the the broker you want to connect to:
                * broker1A-> localhost:9092
                * broker2A-> localhost:9093
                * broker3A-> localhost:9094
                * clusterA-> localhost:9092,localhost:9093,localhost:9094
                * broker1B-> localhost:8092
                * broker2B-> localhost:8093
                * broker3B-> localhost:8094
                * clusterB-> localhost:8092,localhost:8093,localhost:8094
         */
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "mm2_group");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "mm2_consumer_topic1");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final Consumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        /*
            Change topic name and partition accordingly to the topic you want to consume from
         */
        TopicPartition partitionZero = new TopicPartition("topic1", 0);
        List<TopicPartition> partitions = Collections.singletonList(partitionZero);
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);


        final int giveUp = 100;
        int noRecordsCount = 0;

        while (true) {
            final ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));

            if (consumerRecords.count() == 0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp)
                    break;
                else
                    continue;
            }

            consumerRecords.forEach(record -> {

                System.out.println("Record Key " + record.key());
                System.out.println("Record value " + record.value());
                System.out.println("Record partition " + record.partition());
                System.out.println("Record offset " + record.offset());
            });
            consumer.commitAsync();
        }
        consumer.close();
    }
}
