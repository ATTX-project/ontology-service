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
import java.io.File;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import org.apache.commons.io.FileUtils;
import org.uh.hulib.attx.wc.uv.common.pojos.OntologyServiceOutput;
import org.uh.hulib.attx.wc.uv.common.pojos.OntologyServiceResponseMessage;
import org.uh.hulib.attx.wc.uv.common.pojos.ProvenanceMessage;
import org.uh.hulib.attx.wc.uv.common.pojos.Source;
import org.uh.hulib.attx.wc.uv.common.pojos.prov.Activity;
import org.uh.hulib.attx.wc.uv.common.pojos.prov.Agent;
import org.uh.hulib.attx.wc.uv.common.pojos.prov.Context;
import org.uh.hulib.attx.wc.uv.common.pojos.prov.DataProperty;
import org.uh.hulib.attx.wc.uv.common.pojos.prov.Provenance;

public class RPCServer {

    private static final String RPC_QUEUE_NAME = "attx.ontology.inbox";
    private static final String PROV_QUEUE_NAME = "provenance.inbox";
    private static ObjectMapper mapper = new ObjectMapper();

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
        try {
            if (payload.has("sourceData")) {
                // Load the main data model

                String schemaGraph = payload.get("sourceData").get("schemaGraph").asText();
                String dataGraph = payload.get("sourceData").get("dataGraph").asText();

                result = OntologyUtils.OntologyInfer(dataGraph, schemaGraph);


            } else {
                throw new Exception("Something went very wrong and could not do the inference. ");
            }

        } catch (Exception e) {
            System.out.println("Something went very wrong and could not do the inference: " + e.toString());
        } finally {
            return result;
        }
    }

    private static String report(JsonNode payload) {

        String result = null;
        try {
            if (payload.has("sourceData")) {
                // Load the main data model

                String schemaGraph = payload.get("sourceData").get("schemaGraph").asText();
                String dataGraph = payload.get("sourceData").get("dataGraph").asText();

                result = OntologyUtils.ValidityReport(dataGraph, schemaGraph);


            } else {
                throw new Exception("Something went very wrong and could not get the report. ");
            }
        } catch (Exception e) {
            System.out.println("Something went very wrong and could not get the report: " + e.toString());
        } finally {
            return result;
        }
    }

   public static String getProvenanceMessage(Context ctx, String status, OffsetDateTime startTime, OffsetDateTime endTime, List<Source> sourceData, List<String> output) throws Exception {
        ProvenanceMessage m = new ProvenanceMessage();
        Provenance p = new Provenance();
        p.setContext(ctx);

        Agent a = new Agent();
        a.setID("ontologyservice");
        a.setRole("inference");
        p.setAgent(a);

        Activity act = new Activity();
        act.setTitle("Infer data");
        act.setType("ServiceExecution");
        act.setStatus(status);
        act.setStartTime(DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(startTime));
        act.setEndTime(DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(endTime));
        p.setActivity(act);

        Map<String, Object> payload = new HashMap<String, Object>();
        p.setInput(new ArrayList<DataProperty>());
        p.setOutput(new ArrayList<DataProperty>());
        for(int i = 0; i < sourceData.size(); i++) {
            Source s = sourceData.get(i);
            DataProperty dp = new DataProperty();
            dp.setKey("inputDataset" + i);
            dp.setRole("Dataset");
            p.getInput().add(dp);

            if("data".equalsIgnoreCase(s.getInputType())) {
                payload.put("inputDataset" + i, "http://data.hulib.helsinki.fi/attx/temp/" + s.getInput().hashCode() + "_"+ System.currentTimeMillis());
            }
            else {
                payload.put("inputDataset" + i, s.getInput());
            }

        }

        for(int i = 0; i < output.size(); i++) {
            String s = output.get(i);
            DataProperty dp = new DataProperty();
            dp.setKey("outputDataset" + i);
            dp.setRole("Dataset");
            p.getOutput().add(dp);
            payload.put("outputDataset" + i, s);
        }

        m.setProvenance(p);
        m.setPayload(payload);

        return mapper.writeValueAsString(m);
    }

    public void run() {

        //OntologyService webApi = new OntologyService();
        //webApi.run();

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
                    Context ctx = new Context();
                    OffsetDateTime startTime = OffsetDateTime.now();
                    List<Source> sourceData = new ArrayList<Source>();
                    List<String> outputData = new ArrayList<String>();

                    try {
                        String message = new String(body,"UTF-8");
                        System.out.println("Message received: " + message);

                        ObjectMapper objectMapper = new ObjectMapper();
                        JsonNode jsonNode = objectMapper.readTree(message);


                        if (jsonNode.has("payload") && jsonNode.has("provenance")) {
                            JsonNode payload = jsonNode.get("payload").get("ontologyServiceInput");

                            JsonNode originalContext = jsonNode.get("provenance").get("context");

                            ctx.setActivityID(originalContext.get("activityID").asText());
                            ctx.setWorkflowID(originalContext.get("workflowID").asText());
                            ctx.setStepID(originalContext.get("stepID").asText());

                            String activityType = payload.get("activity").asText();
                            String tempResponse = "";

                            if(activityType.equals("infer")) {
                                System.out.println("Inference action received.");
                                tempResponse = RPCServer.infer(payload);


                            } else if (activityType.equals("report")) {
                                System.out.println("Validity report action received.");
                                tempResponse = RPCServer.report(payload);

                            } else {
                               throw new Exception("Something went very wrong. Missing key in message ?");
                            }
                            System.out.println(tempResponse);
                            if(tempResponse != null) {
                                File outputDir = new File("/attx-sb-shared/ontologyservice/" + UUID.randomUUID());
                                outputDir.mkdir();
                                System.out.println("Created output dir");
                                File outputFile = new File(outputDir, "/result.ttl");
                                FileUtils.writeStringToFile(outputFile, tempResponse, "UTF-8");
                                OffsetDateTime endTime = OffsetDateTime.now();
                                String tempOutputFile = outputFile.toURI().toURL().toString();
                                System.out.println(tempOutputFile);

                                Source s1 = new Source();
                                s1.setInput(payload.get("sourceData").get("schemaGraph").asText());
                                s1.setContentType("turtle");
                                s1.setInputType("configuration");
                                Source s2 = new Source();
                                s2.setInput(payload.get("sourceData").get("dataGraph").asText());
                                s2.setContentType("turtle");
                                s2.setInputType("graph");
                                sourceData.add(s1);
                                sourceData.add(s2);


                                outputData.add(tempOutputFile);
                                provMessage = RPCServer.getProvenanceMessage(ctx, "SUCCESS", startTime, endTime, sourceData, outputData);

                                OntologyServiceResponseMessage responseMsg = new OntologyServiceResponseMessage();
                                Provenance provObj = new Provenance();
                                responseMsg.setProvenance(provObj);
                                provObj.setContext(ctx);
                                OntologyServiceOutput rout = new OntologyServiceOutput();
                                rout.setContentType("turtle");
                                rout.setOutput(tempOutputFile);
                                rout.setOutputType("file");
                                OntologyServiceResponseMessage.OntologyServiceResponseMessagePayload payload2 = responseMsg.new OntologyServiceResponseMessagePayload();
                                responseMsg.setPayload(payload2);
                                payload2.setStatus("SUCCESS");
                                payload2.setStatusMessage("All is peachy.");
                                payload2.setOntologyServiceOutput(tempOutputFile);

                                response = mapper.writeValueAsString(responseMsg);
                                System.out.println(response);
                            } else {
                                throw new Exception("Something went very wrong. Value of the result is null");
                            }

                        }
                    }
                    catch (Exception e){

                        OntologyServiceResponseMessage responseMsg = new OntologyServiceResponseMessage();
                        Provenance provObj = new Provenance();
                        responseMsg.setProvenance(provObj);
                        provObj.setContext(ctx);
                        OntologyServiceResponseMessage.OntologyServiceResponseMessagePayload payload3 = responseMsg.new OntologyServiceResponseMessagePayload();
                        responseMsg.setPayload(payload3);
                        payload3.setStatus("ERROR");
                        payload3.setStatusMessage(e.toString());

                        response = mapper.writeValueAsString(responseMsg);
                        OffsetDateTime endTime = OffsetDateTime.now();

                        try {
                            provMessage = RPCServer.getProvenanceMessage(ctx, "ERROR", startTime, endTime, sourceData, outputData);
                        } catch (Exception e1) {
                            e1.printStackTrace();
                        }

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
