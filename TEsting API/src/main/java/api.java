import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opensky.model.OpenSkyStates;
import org.opensky.model.StateVector;
import java.util.Scanner;
import java.lang.instrument.Instrumentation;

 class ObjectSizeFetcher {
    private static Instrumentation instrumentation;

    public static void premain(String args, Instrumentation inst) {
        instrumentation = inst;
    }

    public static long getObjectSize(Object o) {
        return instrumentation.getObjectSize(o);
    }
}



public class api {
    public static void main(String[] args) throws IOException {

        // Verifier que le topic est donne en argument
        if(args.length == 0){
            System.out.println("Entrer le nom du topic");
            return;
        }

        // Assigner topicName a une variable
        String topicName = args[0].toString();

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

        /*for(int i = 0; i < 10; i++)
            producer.send(new ProducerRecord<String, String>(topicName,
                    Integer.toString(i), Integer.toString(i)));
        System.out.println("Message envoye avec succes");*/
       // producer.close();
        try {

            while(true){
                OpenSkyService osservice = new OpenSkyService("souheil","Sossbswakandaopensky123");
                OpenSkyStates result=osservice.getStates();
                // System.out.print(topicName);

                if(result!= null ){
               JSONObject jsonResult=openSkyStatesToJson(result);
               producer.send(new ProducerRecord<String, String>(topicName,
                                "0", jsonResult.toString()));
                    System.out.println("Message envoye avec succes");
                    //producer.close();

           //  producer.send(new ProducerRecord<String, String>(topicName, jsonResult.toString(), jsonResult.toString()));

             Thread.sleep(5000);}

            }


        } catch (Exception e) {
            System.out.println("Exception in NetClientGet:- " + e);
        }
    }

    public static JSONObject stateToJson (StateVector state){
        JSONObject json = new JSONObject()
                .put("geoAltitude", state.getGeoAltitude())
                .put("longitude", state.getLongitude())
                .put("latitude", state.getLatitude())
                .put("velocity",state.getVelocity())
                .put("heading",state.getHeading())
                .put("velocity",state.getVelocity())
                .put("verticalRate",state.getVerticalRate())
                .put("icao24",state.getIcao24())
                .put("callsign",state.getCallsign())
                .put("lastContact",state.getLastContact())
                .put("lastPositionUpdate",state.getLastPositionUpdate())
                .put("originCountry",state.getOriginCountry())
                .put("squawk",state.getSquawk())
                .put("baroAltitude",state.getBaroAltitude())
                .put("positionSource",state.getPositionSource())
                .put("serials",state.getSerials()) ;
        return json ;

    }

    public static JSONObject openSkyStatesToJson(OpenSkyStates state){
        JSONArray statesJsonArray= new JSONArray();
        try{

        Iterator<StateVector> statesIterator=  state.getStates().iterator() ;

        while(statesIterator.hasNext()) {
            Object element = statesIterator.next();
            statesJsonArray.put( stateToJson((StateVector) element));
        }

        }catch(Exception e){
            System.out.println(e.getStackTrace()) ; 

        }finally {


            JSONObject json = new JSONObject()
                    .put("timestamp", state.getTime())
                    .put("states",statesJsonArray) ;
            return json ;
            
        }
        

    }


}


