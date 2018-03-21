package org.uh.hulib.attx.services.ontology;

import static spark.Spark.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Random;

public class OntologyService {
    public static void main() {
        int maxThreads = 6;
        int minThreads = 5;
        int timeOutMillis = 30000;
        String apiVersion = "0.2";

        port(4306);
        threadPool(maxThreads, minThreads, timeOutMillis);

        get("/health", (request, response) -> "Hello World");


        post(String.format("/%s/infer", apiVersion), "application/json", (request, response) -> {

            String content = request.body();
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(content);

            SQLiteConnection data = SQLiteConnection.main();
            String result = null;
            if (jsonNode.has("ontologyGraph")) {
                // Load the main data model
                String schemaGraph = jsonNode.get("schemaGraph").asText();
                String dataGraph = jsonNode.get("dataGraph").asText();

                result = OntologyUtils.OntologyInfer(dataGraph, schemaGraph);
            } else {

            }
            Random rand = new Random();

            int n = rand.nextInt(500000) + 1;
            data.insert(n, result);

            response.status(200); // 200 Done
            response.type("text/turtle");
            return result;
        });


        post(String.format("/%s/validate", apiVersion), "application/json", (request, response) -> {

            String content = request.body();
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(content);

            SQLiteConnection data = SQLiteConnection.main();
            String result = null;
            if (jsonNode.has("ontologyGraph")) {
                // Load the main data model
                String schemaGraph = jsonNode.get("schemaGraph").asText();
                String dataGraph = jsonNode.get("dataGraph").asText();

                result = OntologyUtils.ValidityReport(dataGraph, schemaGraph);
            } else {

            }
            Random rand = new Random();

            int n = rand.nextInt(500000) + 1;
            data.insert(n, result);

            response.status(200); // 200 Done
            response.type("text/turtle");
            return result;
        });

    }

    public void run() {
        OntologyService.main();
    }
}