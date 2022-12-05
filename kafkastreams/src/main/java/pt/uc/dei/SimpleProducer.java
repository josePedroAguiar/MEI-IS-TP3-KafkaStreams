package pt.uc.dei;

import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SimpleProducer {
    public static void main(String[] args) throws Exception{ //Assign topicName to string variable

        String topicName = args[0].toString();

        // create instance for properties to access producer configs
        Properties props = new Properties(); //Assign localhost id
        props.put("bootstrap.servers", "broker1:9092");
        //Set acknowledgements for producer requests. props.put("acks", "all");
        //If the request fails, the producer can automatically retry,
        props.put("retries", 0);
        //Specify buffer size in config
        props.put("batch.size", 16384);
        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);
        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        
        Producer<String, Long> producer = new KafkaProducer<>(props);
        for(int i = 0; i < 1000; i++) {
            {	"Distrito de Beja"	,
            "Distrito de Évora"	,
            "Distrito de Santarém"	,
            "Distrito de Castelo Branco"	,
            "Distrito de Bragança"	,
            "Distrito de Portalegre",	
            "Distrito da Guarda",	
            "Distrito de Setúbal",	
            "Distrito de Vise",	
            "Distrito de Faro","Distrito de Vila Real","Distrito de Coimbra","Distrito de Leiria",	"Distrito de Aveiro",	"Distrito de Lisboa",	"Distrito de Braga","Distrito do Porto","Região Autónoma dos Açores",	"Distrito de Viana do Castelo",	"Região Autónoma da Madeira",}
            producer.send(new ProducerRecord<String, Long>(topicName, Integer.toString(i),
            (long) i));
            if (i % 100 == 0)
                System.out.println("Sending message " + (i + 1) + " to topic " + topicName);
        }
        producer.close();
    }
}