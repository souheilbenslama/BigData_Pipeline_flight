package tn.insat.tp3;

import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;

public class DataLake {



    static public DataLakeServiceClient GetDataLakeServiceClient
            (String accountName, String accountKey){

        StorageSharedKeyCredential sharedKeyCredential =
                new StorageSharedKeyCredential( "tpbigd" , "EmlpoyM7LCGpxYIhooTn2ocw2nQMX1ZDMOkS0zT4GYSgkub7CuUi0GTANy4QcnzabTimU0o7d9A3+AStWY/fXw==");

        DataLakeServiceClientBuilder builder = new DataLakeServiceClientBuilder();

        builder.credential(sharedKeyCredential);
        builder.endpoint("https://" + "tpbigd" + ".dfs.core.windows.net");

        return builder.buildClient();
    }

    public DataLakeFileSystemClient CreateFileSystem
            (DataLakeServiceClient serviceClient){

        return serviceClient.createFileSystem("my-file-system");
    }


}
