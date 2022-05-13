package tn.insat.tp3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;

import java.util.Arrays;
import java.util.Iterator;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class Write {

    private Table table;
    private String tableName = "planes";
    private String family1 = "f1";
    private Connection connection ;

    public void createHbaseTable() throws IOException{
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

    /*public static void main(String[] args) throws IOException {
        Write admin = new Write();
        admin.createHbaseTable();
        String[][] data = {{"wala","souhail","marouene"},{"123"},{"12"}};
        admin.addData(data);
    }*/
}

