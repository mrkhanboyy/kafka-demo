import kafka.Kafka
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.producer.ProducerRecord

class App{



    fun consume(){
        println("consumer running")
        var consumer = CustomConsumer().createConsumer()
        try{
        while(true){
                    var consumeRecords =  consumer.poll(1000)
                    for(record in consumeRecords){
                        println("offset = ${record.offset()} : key = ${record.key()} : value = ${record.value()}")
                        println("consumed ${record.value()}")
                    }
        }
    }catch (e: Exception){
        e.printStackTrace()
    }
        println("consumer finished")
    }

    fun produce(){
        println("producer running")
        try {
            var producer = CustomProducer().createProducer()
            for (i in 1..20) {
                producer.send(ProducerRecord(KafkaConstants.TOPIC_NAME, "production begin $i", "produced $i"))
                println("produced $i")
                Thread.sleep(1000)
            }
        }catch (e: Exception){
            e.printStackTrace()
        }
        println("producer finished")

    }


}

fun main() {

    val main = App()
    main.consume()


}
