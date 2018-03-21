package org.uh.hulib.attx.services.ontology;

import org.apache.jena.rdf.model.*;
import org.apache.jena.reasoner.Reasoner;
import org.apache.jena.reasoner.ReasonerRegistry;
import org.apache.jena.reasoner.ValidityReport;
import org.apache.jena.util.FileManager;

import java.io.ByteArrayOutputStream;
import java.util.Iterator;


public class OntologyUtils {
    public static String OntologyInfer(String dataGraph, String schemaGraph) {
        InfModel infmodel = null;
        Model result = null;
        try {
            // "/home/stenegru/dev/ontology-service/src/main/resources/owlDemoSchema.ttl"
            Model schema = FileManager.get().loadModel(schemaGraph);
            // "/home/stenegru/dev/ontology-service/src/main/resources/owlDemoData.ttl"
            Model data = FileManager.get().loadModel(dataGraph);
            Reasoner reasoner = ReasonerRegistry.getOWLReasoner();
            reasoner = reasoner.bindSchema(schema);
            infmodel = ModelFactory.createInfModel(reasoner, data);

            result = outputStatements(infmodel, null, null, null);

        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            ByteArrayOutputStream serialised = new ByteArrayOutputStream();
            result.write(serialised, "Turtle");
            return serialised.toString();

        }

    }

    public static String ValidityReport(String dataGraph, String schemaGraph){
        String result = "";
        try {
            // "/home/stenegru/dev/ontology-service/src/main/resources/owlDemoSchema.ttl"
            Model schema = FileManager.get().loadModel(schemaGraph);
            // "/home/stenegru/dev/ontology-service/src/main/resources/owlDemoData.ttl"
            Model data = FileManager.get().loadModel(dataGraph);
            Reasoner reasoner = ReasonerRegistry.getOWLReasoner();
            reasoner = reasoner.bindSchema(schema);
            InfModel infmodel = ModelFactory.createInfModel(reasoner, data);

            ValidityReport validity = infmodel.validate();

            if (validity.isValid()) {
                result = "OK";
                // System.out.println("OK");
            } else {
                // System.out.println("Conflicts");
                for (Iterator i = validity.getReports(); i.hasNext(); ) {
                    ValidityReport.Report report = (ValidityReport.Report) i.next();
                    result += report;
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            return result;
        }
    }

    private static Model outputStatements(Model m, Resource s, Property p, Resource o) {
        Model result = ModelFactory.createDefaultModel();
        for (StmtIterator i = m.listStatements(s,p,o); i.hasNext(); ) {
            Statement stmt = i.nextStatement();
            result.add(stmt);
        }
        return result;
    }
}
