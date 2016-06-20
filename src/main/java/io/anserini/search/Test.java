package io.anserini.search;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Created by youngbinkim on 6/12/16.
 */
@Path("/test")
public class Test {
    @GET
    @Path("hello")
    @Produces(MediaType.TEXT_PLAIN)
    public String helloWorld() {
        return "Hello, world!";
    }

    @GET
    @Produces("application/json")
    public Response convert2() {
        System.out.println("hhh");
        return Response.status(200).entity("whatevs").build();
    }

    @GET
    @Path("/get")
    @Produces("application/json")
    public Response convert() {
        System.out.println("hhh");
        return Response.status(200).entity("whatevs").build();
    }
}
