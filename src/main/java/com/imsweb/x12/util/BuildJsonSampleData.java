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

    public static Map<String, Object> captureDefinitions(TransactionDefinition transactionDef) {
        return captureLoopDefinitions(transactionDef.getLoop(), true);
    }

    private static Map<String, Object> captureLoopDefinitions(LoopDefinition loopDef, boolean isFirstLoop) {
        if (loopDef == null) {
            return null;
        }

        Map<String, Object> loopMap = new HashMap<>();
        List<Map<String, Object>> segmentList = new ArrayList<>();
        List<SegmentDefinition> segmentDefs = loopDef.getSegment();

        if (segmentDefs != null) {
            for (SegmentDefinition segmentDef : segmentDefs) {
                if (segmentDef.getUsage().name().equals("NOT_USED")) {
                    continue;
                }

                Map<String, Object> segmentMap = new HashMap<>();
                Map<String, Object> elementMap = new HashMap<>();
                List<ElementDefinition> elementDefs = segmentDef.getElements();

                if (elementDefs != null) {
                    for (ElementDefinition elementDef : elementDefs) {
                        if (elementDef.getUsage().name().equals("NOT_USED")) {
                            continue;
                        }
                        elementMap.put(elementDef.getXid() + "_" + elementDef.getName()
                                .replace(' ', '_').replaceAll("[^a-zA-Z0-9_]", "").toLowerCase(), null);
                    }
                }

                if ("1".equals(segmentDef.getMaxUse())) {
                    segmentMap.put(segmentDef.getXid() + "_" + segmentDef.getName()
                            .replace(' ', '_').replaceAll("[^a-zA-Z0-9_]", "").toLowerCase(), elementMap);
                } else {
                    List<Map<String, Object>> arrayWrapper = new ArrayList<>();
                    arrayWrapper.add(elementMap);
                    segmentMap.put(segmentDef.getXid() + "_" + segmentDef.getName()
                            .replace(' ', '_').replaceAll("[^a-zA-Z0-9_]", "").toLowerCase(), arrayWrapper);
                }

                segmentList.add(segmentMap);
            }
        }

        List<Map<String, Object>> childLoops = new ArrayList<>();
        List<LoopDefinition> childLoopDefs = loopDef.getLoop();

        if (childLoopDefs != null) {
            for (LoopDefinition childLoopDef : childLoopDefs) {
                if (childLoopDef.getUsage().name().equals("NOT_USED")) {
                    continue;
                }
                childLoops.add(captureLoopDefinitions(childLoopDef, false));
            }
        }

        List<Map<String, Object>> combinedFields = new ArrayList<>();
        combinedFields.addAll(segmentList);
        combinedFields.addAll(childLoops);
        if (isFirstLoop) {
            combinedFields.addAll(createErrorFields());
        }

        if ("1".equals(loopDef.getRepeat()) || isFirstLoop) {
            if (!combinedFields.isEmpty()) {
                Map<String, Object> combinedMap = new HashMap<>();
                for (Map<String, Object> map : combinedFields) {
                    combinedMap.putAll(map);
                }
                loopMap.put(loopDef.getXid() + "_" + loopDef.getName()
                        .replace(' ', '_').replaceAll("[^a-zA-Z0-9_]", "").toLowerCase(), combinedMap);
            }
        } else {
            loopMap.put(loopDef.getXid() + "_" + loopDef.getName()
                    .replace(' ', '_').replaceAll("[^a-zA-Z0-9_]", "").toLowerCase(), combinedFields);
        }

        return loopMap;
    }

    private static List<Map<String, Object>> createErrorFields() {
        List<Map<String, Object>> errorFields = new ArrayList<>();

        Map<String, Object> errorsField = new HashMap<>();
        errorsField.put("Errors", new ArrayList<>());
        errorFields.add(errorsField);

        Map<String, Object> fatalErrorsField = new HashMap<>();
        fatalErrorsField.put("FatalErrors", new ArrayList<>());
        errorFields.add(fatalErrorsField);

        return errorFields;
    }

    public static void saveToJsonFile(String filePath, Map<String, Object> structure) throws IOException {
        Gson gson = new GsonBuilder().serializeNulls().setPrettyPrinting().create();
        String jsonOutput = gson.toJson(structure.get("ISA_LOOP_interchange_control_header"));

        try (FileWriter writer = new FileWriter(filePath)) {
            writer.write(jsonOutput);
        }
    }

    public static void main(String[] args) {
        try {
            X12Reader reader837 = new X12Reader(X12Reader.FileType.ANSI837_5010_X222, new File("837sample"));

            TransactionDefinition transactionDef = reader837.getDefinition();

            Map<String, Object> structuredData = captureDefinitions(transactionDef);

            String filePath = "837sampledata.json";
            saveToJsonFile(filePath, structuredData);

            System.out.println("JSON saved to " + filePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}