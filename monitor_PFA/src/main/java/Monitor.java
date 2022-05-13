

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.JSONObject;
import java.io.IOException;

import java.util.*;
import java.util.stream.Collectors;


public class Monitor {

    private Table table;
    private String tableName = "planes";
    private String family1 = "f1";
    private Connection connection ;

    public void createHbaseTable() throws IOException {
        Configuration config = HBaseConfiguration.create();
        connection = ConnectionFactory.createConnection(config);
        Admin admin = connection.getAdmin();

        HTableDescriptor ht = new HTableDescriptor(TableName.valueOf(tableName));
        ht.addFamily(new HColumnDescriptor(family1));
        System.out.println("connecting");

        System.out.println("Creating Table");
        createOrOverwrite(admin, ht);
        System.out.println("Done......");

        table = connection.getTable(TableName.valueOf(tableName));
    }

    private String listToString(List<String> list) {
        String result = list.stream()
                .map(n -> String.valueOf(n))
                .collect(Collectors.joining(",", "{", "}"));

        return result;
    }

    public void addData(String[][] data) throws IOException{
        try {

            System.out.println("Adding row");
            byte[] row = Bytes.toBytes(UUID.randomUUID().toString());
            Put p = new Put(row);
            String planes = listToString(Arrays.asList(data[0]));
            p.addColumn(family1.getBytes(), "names".getBytes(), Bytes.toBytes(planes));
            p.addColumn(family1.getBytes(), "number".getBytes(), Bytes.toBytes(data[1][0]));
            p.addColumn(family1.getBytes(), "highest".getBytes(), Bytes.toBytes(data[2][0]));
            table.put(p);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            table.close();
            connection.close();
        }
    }

    public static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
        if (!admin.tableExists(table.getTableName())) {
            admin.createTable(table);
        }
    }

    public static void main(String[] args) throws IOException {



        /* Monitor admin = new Monitor();
        admin.createHbaseTable();
        String[][] data = {{"wala","souhail","marouene"},{"123"},{"12"}};
        admin.addData(data);*/

        if(args.length == 0){
            System.out.println("Entrer le nom du topic");
            return;
        }

        String topicName = args[0].toString();

        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer
                <String, String>(props);

    // Kafka Consumer va souscrire a la liste de topics ici
    consumer.subscribe(Arrays.asList(topicName));

    // Afficher le nom du topic
    System.out.println("Souscris au topic " + topicName);
    int i = 0;
        String msg="" ;
        String[] tab = new String[3];
        String planes= null ;
        String[] tabplanes = null ;
    while (true) {
        try{
        ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records){


                // Afficher l'offset, clef et valeur des enregistrements du consommateur
                /* System.out.printf("offset = %d, key = %s, value = %s\n",
                        record.offset(), record.key(), record.value().getClass());String  msg=record.value() ; */
            msg= record.value() ;
            tab = msg.split("], \\[");
           // System.out.println(msg);
            if(tab.length==3)
            {
                planes = tab[0].replaceFirst("\\[\\[", "") ;
                tabplanes=planes.replaceAll("}, \\{","}|{").split("\\|") ;
                System.out.println(tabplanes[0]);
                //for(int k=0 ; k< tabplanes.length ; k++ ){

                //String plane = new JSONObject(tabplanes[ka]).get("callsign").toString();
                   // System.out.println(tabplanes[k]);
               // }

                //System.out.println("planes: "+);
                System.out.println("number of planes: "+ tab[1]);
                System.out.println("fastest plane: "+tab[2].replaceAll(" ","").replaceAll("]]","")); } }

        }catch (Exception e){
            System.out.println("error"+ e);
        }
    } }
}
