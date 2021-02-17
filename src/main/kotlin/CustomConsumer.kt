
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import scala.util.parsing.combinator.testing.Str

import java.util.*


class CustomConsumer {


    fun createConsumer(): KafkaConsumer<String, String> {
        var props: Properties = Properties()
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BROKERS)
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaConstants.KAFKA_DESERIALIZER)
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaConstants.KAFKA_DESERIALIZER)
        props.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaConstants.GROUP)
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        var kafkaConsumer = KafkaConsumer<String, String>(props)
        kafkaConsumer.subscribe(listOf(KafkaConstants.TOPIC_NAME))
        return  kafkaConsumer
    }


}