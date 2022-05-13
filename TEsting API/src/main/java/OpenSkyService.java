
import org.opensky.api.OpenSkyApi;
import org.opensky.model.OpenSkyStates;
import java.io.IOException;

public class OpenSkyService {
        OpenSkyApi api ;
        public OpenSkyService(String username, String password) throws IOException {
            this.api = new OpenSkyApi(username,password);
        }
        public OpenSkyStates getStates() {
            try {
                // get all states of a current time stamp
                OpenSkyStates result = this.api.getStates( 0,null);
                // in case of need of a specefic area boxing
                // OpenSkyStates os = api.getStates(1651744518,null,new OpenSkyApi.BoundingBox(48.051967,49.514905,1.204102,4.302246));


                return result;
            } catch (IOException e) {
                System.out.println(e);
                return null ;
            }

        } }




