package org.uh.hulib.attx.services.ontology;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RPCServer {

    private static final String RPC_QUEUE_NAME = "attx.ontology.inbox";
    private static final String PROV_QUEUE_NAME = "provenance.inbox";

    private static String getPassword() {
            if(System.getenv("MPASS") != null) {
                return System.getenv("MPASS");
            } else {
                return "password";
            }
    }

    private static String getUsername() {
            if(System.getenv("MUSER") != null) {
                return System.getenv("MUSER");
            } else {
                return "user";
            }
    }

    private static String getBrokerURI() {
            if(System.getenv("MHOST") != null) {
                return System.getenv("MHOST");
            } else {
                return "localhost";
            }
    }

    private static String infer(JsonNode payload) {

        String result = null;
        if (payload.has("sourceData")) {
            // Load the main data model

            String schemaGraph = payload.get("sourceData").get("schemaGraph").asText();
            String dataGraph = payload.get("sourceData").get("dataGraph").asText();

            result = OntologyUtils.OntologyInfer(dataGraph, schemaGraph);
        } else {
            // FOR NOW DO NOTHING
        }

        return result;
    }

    private static String report(JsonNode payload) {

        String result = null;
        if (payload.has("sourceData")) {
            // Load the main data model

            String schemaGraph = payload.get("sourceData").get("schemaGraph").asText();
            String dataGraph = payload.get("sourceData").get("dataGraph").asText();

            result = OntologyUtils.ValidityReport(dataGraph, schemaGraph);
        } else {
            // FOR NOW DO NOTHING
        }

        return result;
    }

    private static String provenanceMessage(String activity, JsonNode provenance, String schemaInput, String dataInput, String dataOutput){

        String response = "{\n" +
                "    \"provenance\": {\n" +
                "        \"context\": {\n" +
                "          \"workflowID\": \"ingestionwf\",\n" +
                "          \"activityID\": \"1\"\n" +
                "        },\n" +
                "        \"agent\": {\n" +
                "          \"ID\": \"OntologyService\",\n" +
                "          \"role\": \"Inference\"\n" +
                "        },\n" +
                "        \"activity\": {\n" +
                "            \"title\": \"" + activity + "\",\n" +
                "            \"type\": \"ServiceExecution\",\n" +
                "            \"startTime\": \"2017-08-02T13:52:29+02:00\"\n" +
                "        },\n" +
                "        \"input\": [\n" +
                "          {\n" +
                "            \"key\": \"inputDataset\",\n" +
                "            \"role\": \"Dataset\"\n" +
                "          }\n" +
                "        ],\n" +
                "        \"output\": [\n" +
                "          {\n" +
                "            \"key\": \"outputDataset\",\n" +
                "            \"role\": \"Dataset\"\n" +
                "          }\n" +
                "        ]\n" +
                "    },\n" +
                "    \"payload\": {\n" +
                "        \"inputDataset\": \"http://dataset/1\",\n" +
                "        \"outputDataset\": \"http://dataset/2\"\n" +
                "    }\n" +
                "}\n";
        return response;
    }

    public void run() {

        OntologyService webApi = new OntologyService();
        webApi.run();

        ConnectionFactory factory = new ConnectionFactory();
        factory.setPassword(getPassword());
        factory.setUsername(getUsername());
        factory.setHost(getBrokerURI());
        factory.setNetworkRecoveryInterval(5000);
        factory.setAutomaticRecoveryEnabled(true);

        Connection connection = null;
        try {
            connection      = factory.newConnection();
            final Channel channel = connection.createChannel();
            final Channel channelProv = connection.createChannel();

            channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);

            channelProv.queueDeclare(PROV_QUEUE_NAME, false, false, false, null);

            channel.basicQos(1);

            System.out.println(" [x] Awaiting RPC requests");

            Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                            .Builder()
                            .correlationId(properties.getCorrelationId())
                            .build();

                    String response = "";
                    String provMessage = "";

                    try {
                        String message = new String(body,"UTF-8");
                        System.out.println("Message received: " + message);

                        ObjectMapper objectMapper = new ObjectMapper();
                        JsonNode jsonNode = objectMapper.readTree(message);


                        if (jsonNode.has("payload") && jsonNode.has("provenance")) {
                            JsonNode payload = jsonNode.get("payload").get("ontologyServiceInput");
                            JsonNode provenance = jsonNode.get("provenance");

                            String activityType = payload.get("activity").asText();

                            if( activityType.equals("infer")) {
                                System.out.println("Inference action received.");
                                response = infer(payload);
                            } else if (activityType.equals("report")) {
                                System.out.println("Validity report action received.");
                                response = report(payload);
                            } else {
                                // Send ERROR

                            }
                        }
                    }
                    catch (RuntimeException e){
                        String message = new String(body,"UTF-8");
                        ObjectMapper objectMapper = new ObjectMapper();
                        JsonNode jsonNode = objectMapper.readTree(message);

                        JsonNode context = jsonNode.get("provenance").get("context");
                        response = "{\n"
                                + "	\"provenance\": {\n"
                                + "		\"context\": {\n"
                                + "			\"workflowID\": \"" + context.get("workflowID").asText() + "\",\n"
                                + "			\"activityID\": \"" + context.get("activityID").asText()+ "\",\n"
                                + "			\"stepID\": \"" + context.get("stepID").asText()+ "\"\n"
                                + "		}\n"
                                + "	},"
                                + "    \"payload\": {\n"
                                + "        \"status\": \"error\",\n"
                                + "        \"statusMessage\": \"" + e.toString() + "\"\n"
                                + "   }\n"
                                + "}";

                        System.out.println(" [.] " + e.toString());
                    }
                    finally {
                        channel.basicPublish( "", properties.getReplyTo(), replyProps, response.getBytes("UTF-8"));
                        channel.basicAck(envelope.getDeliveryTag(), false);
                        // RabbitMq consumer worker thread notifies the RPC server owner thread
                        synchronized(this) {
                            this.notify();
                        }

                        channelProv.basicPublish( "", PROV_QUEUE_NAME, null, provMessage.getBytes());

                    }
                }
            };
            // channelProv.close();
            channel.basicConsume(RPC_QUEUE_NAME, false, consumer);
            // Wait and be prepared to consume the message from RPC client.
            while (true) {
                synchronized(consumer) {
                    try {
                        consumer.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
        finally {
            if (connection != null)
                try {
                    connection.close();
                } catch (IOException _ignore) {}
        }
    }
}