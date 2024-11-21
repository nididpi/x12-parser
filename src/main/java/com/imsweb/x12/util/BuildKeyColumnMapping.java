package com.imsweb.x12.util;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.imsweb.x12.mapping.ElementDefinition;
import com.imsweb.x12.mapping.LoopDefinition;
import com.imsweb.x12.mapping.SegmentDefinition;
import com.imsweb.x12.mapping.TransactionDefinition;
import com.imsweb.x12.mapping.CompositeDefinition;
import com.imsweb.x12.reader.X12Reader;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BuildKeyColumnMapping {

    private TransactionDefinition transactionDef;

    public BuildKeyColumnMapping(TransactionDefinition transactionDef) {
        this.transactionDef = transactionDef;
    }

    public Map<String, Map<String, Object>> captureDefinitions() {
        Map<String, Map<String, Object>> loopsMap = new HashMap<>();
        captureLoopDefinitions(transactionDef.getLoop(), loopsMap);
        return loopsMap;
    }

    private void captureLoopDefinitions(LoopDefinition loopDef, Map<String, Map<String, Object>> loopsMap) {
        if (loopDef == null) {
            return;
        }

        Map<String, Object> loopMap = new HashMap<>();
        loopMap.put("name", loopDef.getName());

        // Handle segments and their elements
        List<SegmentDefinition> segmentDefs = loopDef.getSegment();
        if (segmentDefs != null) {
            for (SegmentDefinition segmentDef : segmentDefs) {
                Map<String, Object> segmentMap = new HashMap<>();
                segmentMap.put("name", segmentDef.getName());

//                if(segmentDef.getXid().equals("CLM")){
//                    String a = "1";
//                }

                Map<String, String> elementsMap = new HashMap<>();
                List<ElementDefinition> elementDefs = segmentDef.getElements();
                if (elementDefs != null) {
                    for (ElementDefinition elementDef : elementDefs) {
                        elementsMap.put(elementDef.getXid(), elementDef.getName());
                    }
                }
                List<CompositeDefinition> compDefs = segmentDef.getComposites();
                if (compDefs != null) {
                    for (CompositeDefinition compDef : compDefs) {
                        elementsMap.put(compDef.getXid(), compDef.getName());
                    }
                }

                segmentMap.put("elements", elementsMap);
                loopMap.put(segmentDef.getXid(), segmentMap);
            }
        }

        loopsMap.put(loopDef.getXid(), loopMap);

        // Process child loops in the same way, treating each loop independently
        List<LoopDefinition> childLoopDefs = loopDef.getLoop();
        if (childLoopDefs != null) {
            for (LoopDefinition childLoopDef : childLoopDefs) {
                captureLoopDefinitions(childLoopDef, loopsMap);
            }
        }
    }

    public void saveToJsonFile(String filePath, Map<String, Map<String, Object>> structure) throws IOException {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        String jsonOutput = gson.toJson(structure);

        try (FileWriter writer = new FileWriter(filePath)) {
            writer.write(jsonOutput);
        }
    }

    public static void main(String[] args) {
        try {
            X12Reader reader837 = new X12Reader(X12Reader.FileType.ANSI837_5010_X222, new File("837sample"));

            // Assuming you have a valid TransactionDefinition object
            TransactionDefinition transactionDef = reader837.getDefinition();
            BuildKeyColumnMapping mapper = new BuildKeyColumnMapping(transactionDef);

            // Capture the data in a flat structure
            Map<String, Map<String, Object>> structuredData = mapper.captureDefinitions();

            // Save JSON to file
            String filePath = "837definitions.json";
            mapper.saveToJsonFile(filePath, structuredData);
            System.out.println(structuredData);

            System.out.println("JSON saved to " + filePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}