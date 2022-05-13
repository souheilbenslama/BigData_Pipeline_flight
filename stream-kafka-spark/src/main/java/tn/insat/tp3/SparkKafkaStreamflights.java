package tn.insat.tp3;


import kafka.serializer.StringDecoder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;

import org.json.JSONArray;
import org.json.JSONObject;
import scala.Tuple2;

import javax.xml.soap.Text;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

import static org.apache.spark.storage.StorageLevel.MEMORY_ONLY;

public class SparkKafkaStreamflights {
    private static final Pattern SPACE = Pattern.compile(" ");

    private SparkKafkaStreamflights() {
    }

    public static void main(String[] args) throws Exception {
        // Verifier que le topic est donne en argument
        if(args.length == 0){
            System.out.println("Entrer le nom du topic");
            return;
        }

        // Assigner topicName a une variable
        String topicName = args[4].toString();

        // Creer une instance de proprietes pour acceder aux configurations du producteur
        Properties props = new Properties();
        props.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "10485880");
        // Assigner l'identifiant du serveur kafka
        props.put("bootstrap.servers", "localhost:9092");

        // Definir un acquittement pour les requetes du producteur
        props.put("acks", "all");

        // Si la requete echoue, le producteur peut reessayer automatiquemt
        props.put("retries", 0);

        // Specifier la taille du buffer size dans la config
        props.put("batch.size", 16384);

        // buffer.memory controle le montant total de memoire disponible au producteur pour le buffering
        props.put("buffer.memory", 33554432);

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer
                <String, String>(props);

        if (args.length < 4) {
            System.err.println("Usage: SparkKafkaWordCount <zkQuorum> <group> <topics> <numThreads>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("SparkKafkaStreamflights");
        // Creer le contexte avec une taille de batch de 2 secondes
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,
                new Duration(2000));

        int numThreads = Integer.parseInt(args[3]);
        Map<String, Integer> topicMap = new HashMap<>();
        String[] topics = args[2].split(",");
        for (String topic: topics) {
            topicMap.put(topic, numThreads);
        }

        Map kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", args[0]);
        kafkaParams.put("group.id", args[1]);
        kafkaParams.put("zookeeper.connect", args[0]);
        kafkaParams.put("fetch.message.max.bytes", "1100000000");

        JavaPairReceiverInputDStream<String, String> messages=KafkaUtils.createStream(jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicMap,MEMORY_ONLY() );

        JavaDStream<String> res = messages.map(Tuple2::_2);


            // Stream processing

            JavaDStream<JSONObject> jsonObjectJavaDStream = res.map((x) -> new JSONObject(x));
            JavaDStream<JSONArray> states = jsonObjectJavaDStream.map((JSONObject x) -> new JSONArray(x.get("states").toString()));

            JavaDStream<List> filtredflights = states.map((x) -> {
                List<String> ll = new ArrayList<>();


                for (int i = 0; i < x.length(); i++) {
                    if (x.getJSONObject(i).has("longitude") && x.getJSONObject(i).has("latitude")) {
                        double longitude = Double.parseDouble(x.getJSONObject(i).get("longitude").toString());
                        double latitude = Double.parseDouble(x.getJSONObject(i).get("latitude").toString());
                        if (((latitude <= 51.019025) && (latitude >= 42.859)) && ((longitude > -4.6203) && (longitude <= 6.124652))) {
                            ll.add((x.getJSONObject(i)).toString());
                        }
                    }
                }

                return ll;
            });

            JavaDStream<List> flightCounts = filtredflights.map(e -> {
                List<String> ll = new ArrayList<>();
                ll.add(String.valueOf(e.size()));
                return ll;
            });

            JavaDStream<List> fastestflights = states.map((x) -> {
                String fastestflight = "null";
                double velocity = 0;

                List<String> ll = new ArrayList<>();


                for (int i = 0; i < x.length(); i++) {
                    if (x.getJSONObject(i).has("velocity")) {
                        double flightvelocity = Double.parseDouble(x.getJSONObject(i).get("velocity").toString());
                        if ((flightvelocity > velocity)) {
                            velocity = flightvelocity;
                            fastestflight = x.getJSONObject(i).get("callsign").toString();
                        }
                    }
                }
                ll.add(fastestflight);
                return ll;
            });

            JavaDStream<List> finalResult = filtredflights.union(flightCounts).union(fastestflights);
            //Here we iterrate over the JavaPairDStream to write words and their count into kafka
            finalResult.foreachRDD(new VoidFunction<JavaRDD<List>>() {
                @Override
                public void call(JavaRDD<List> listJavaRDD) throws Exception {
                    List<List> result = listJavaRDD.collect();
                    if (result.size() != 0){
                        producer.send(new ProducerRecord<String, String>("SResult",
                                "0", result.toString()));
                    }


                }
            });
        //jj.print();
        jssc.start();
        jssc.awaitTermination();

    }
}
