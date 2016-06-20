package rest;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/**
 * Created by youngbinkim on 6/17/16.
 */
@Path("/employee")
public class Example {

    @GET
    @Path("/getEmployee")
    @Produces(MediaType.APPLICATION_JSON)
    public String getEmployee() {
        return "sup";
    }
}

