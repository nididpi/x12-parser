package com.imsweb.x12.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.imsweb.x12.mapping.ElementDefinition;
import com.imsweb.x12.mapping.LoopDefinition;
import com.imsweb.x12.mapping.SegmentDefinition;
import com.imsweb.x12.mapping.TransactionDefinition;
import com.imsweb.x12.reader.X12Reader;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BuildJsonSampleData {

    private TransactionDefinition transactionDef;

    public BuildJsonSampleData(TransactionDefinition transactionDef) {
        this.transactionDef = transactionDef;
    }

    public Map<String, Object> captureDefinitions() {
        return captureLoopDefinitions(transactionDef.getLoop());
    }

    private Map<String, Object> captureLoopDefinitions(LoopDefinition loopDef) {
        return captureLoopDefinitions(loopDef, true);  // Start with the first loop
    }

    private List<Map<String, Object>> createErrorFields() {
        List<Map<String, Object>> errorFields = new ArrayList<>();

        // Define the Errors field
        Map<String, Object> errorsField = new HashMap<>();
        errorsField.put("Errors", new HashMap<>());

        errorFields.add(errorsField);

        // Define the FatalErrors field
        Map<String, Object> fatalErrorsField = new HashMap<>();
        fatalErrorsField.put("FatalErrors", new HashMap<>());

        errorFields.add(fatalErrorsField);

        return errorFields;
    }
    private Map<String, Object> captureLoopDefinitions(LoopDefinition loopDef, boolean isFirstLoop) {
        if (loopDef == null) {
            return null;
        }
        Map<String, Object> loopMap = new HashMap<>();
        loopMap.put("name", loopDef.getXid() + "_" + loopDef.getName().replace(' ', '_').replaceAll("[^a-zA-Z0-9_]", "").toLowerCase());

        List<Map<String, Object>> segmentList = new ArrayList<>();
        List<SegmentDefinition> segmentDefs = loopDef.getSegment();
        if (segmentDefs != null) {
            for (SegmentDefinition segmentDef : segmentDefs) {
                Map<String, Object> segmentMap = new HashMap<>();

                Map<String, Object> elementMap = new HashMap<>();
                List<ElementDefinition> elementDefs = segmentDef.getElements();
                if (elementDefs != null) {
                    for (ElementDefinition elementDef : elementDefs) {
                        elementMap.put(elementDef.getXid() + "_" + elementDef.getName().replace(' ', '_').replaceAll("[^a-zA-Z0-9_]", "").toLowerCase(), null);
                    }
                }

                segmentMap.put(segmentDef.getXid() + "_" + segmentDef.getName().replace(' ', '_').replaceAll("[^a-zA-Z0-9_]", "").toLowerCase(), elementMap);
                segmentList.add(segmentMap);
            }
        }

        List<Map<String, Object>> childLoops = new ArrayList<>();
        List<LoopDefinition> childLoopDefs = loopDef.getLoop();
        if (childLoopDefs != null) {
            for (LoopDefinition childLoopDef : childLoopDefs) {
                Map<String, Object> childLoopMap = captureLoopDefinitions(childLoopDef, false);
                childLoops.add(childLoopMap);
            }
        }

        List<Map<String, Object>> combinedFields = new ArrayList<>();
        combinedFields.addAll(segmentList);
        combinedFields.addAll(childLoops);
        if (isFirstLoop) {
            combinedFields.addAll(createErrorFields());
        }


        String datatype = (loopDef.getRepeat().equals("1") || isFirstLoop) ? "struct" : "array";

        Map<String, Object> typeMap = new HashMap<>();
        if ("struct".equals(datatype)) {
            typeMap.put("type", "struct");
            typeMap.put("fields", combinedFields);
        } else if ("array".equals(datatype)) {
            typeMap.put("type", "array");
            typeMap.put("containsNull", true);

            Map<String, Object> elementTypeMap = new HashMap<>();
            elementTypeMap.put("type", "struct");
            elementTypeMap.put("fields", combinedFields);

            typeMap.put("elementType", elementTypeMap);
        }

        loopMap.put("type", typeMap);
        return loopMap;
    }

    public void saveToJsonFile(String filePath, Map<String, Object> structure) throws IOException {
        Gson gson = new GsonBuilder().serializeNulls().setPrettyPrinting().create();
        String jsonOutput = gson.toJson(structure.get("type"));

        try (FileWriter writer = new FileWriter(filePath)) {
            writer.write(jsonOutput);
        }
    }

    public static void main(String[] args) {
        try {
            X12Reader reader837 = new X12Reader(X12Reader.FileType.ANSI837_5010_X222, new File("837sample"));

            // Assuming you have a valid TransactionDefinition object
            TransactionDefinition transactionDef = reader837.getDefinition();
            BuildJsonSampleData mapper = new BuildJsonSampleData(transactionDef);

            // Capture hierarchical data
            Map<String, Object> structuredData = mapper.captureDefinitions();

            // Save JSON to file
            String filePath = "837sampledata.json";
            mapper.saveToJsonFile(filePath, structuredData);
//            System.out.println(structuredData);

            System.out.println("JSON saved to " + filePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}