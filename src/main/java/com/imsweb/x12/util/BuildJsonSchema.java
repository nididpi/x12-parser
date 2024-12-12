package com.imsweb.x12.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.imsweb.x12.udf.X12Parser;
import com.imsweb.x12.mapping.*;
import com.imsweb.x12.reader.X12Reader;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BuildJsonSchema {

    private TransactionDefinition transactionDef;

    public BuildJsonSchema(TransactionDefinition transactionDef) {
        this.transactionDef = transactionDef;
    }

    public BuildJsonSchema(String fileType) throws IOException {
        X12Reader.FileType fileTypeObject;
        String resourceName;

        try {
            if (fileType.contains("837_5010_X231")) {
                fileTypeObject = X12Reader.FileType.ANSI837_5010_X231;
                resourceName = "837X231sample";
            } else if (fileType.contains("837_5010_X223")) {
                fileTypeObject = X12Reader.FileType.ANSI837_5010_X223;
                resourceName = "837X223sample";
            } else if (fileType.contains("837_5010_X222")) {
                fileTypeObject = X12Reader.FileType.ANSI837_5010_X222;
                resourceName = "837X222sample";
            } else if (fileType.contains("837_4010_X098")) {
                fileTypeObject = X12Reader.FileType.ANSI837_4010_X098;
                resourceName = "837X222sample";
            } else if (fileType.contains("837_4010_X097")) {
                fileTypeObject = X12Reader.FileType.ANSI837_4010_X097;
                resourceName = "837X222sample";
            } else if (fileType.contains("837_4010_X096")) {
                fileTypeObject = X12Reader.FileType.ANSI837_4010_X096;
                resourceName = "837X222sample";
            } else if (fileType.contains("835_5010_X221")) {
                fileTypeObject = X12Reader.FileType.ANSI835_5010_X221;
                resourceName = "835X221sample";
            } else if (fileType.contains("835_4010_X091")) {
                fileTypeObject = X12Reader.FileType.ANSI835_4010_X091;
                resourceName = "835X221sample";
            } else if (fileType.contains("834_5010_X220")) {
                fileTypeObject = X12Reader.FileType.ANSI834_5010_X220;
                resourceName = "834sample";
            }  else if (fileType.contains("820_5010_X218")) {
                fileTypeObject = X12Reader.FileType.ANSI820_5010_X218;
                resourceName = "820sample";
            } else {
                throw new IllegalArgumentException("Unknown file type." + fileType);
            }
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("Invalid file type provided: " + fileType, e);
        }


        InputStream x12InputStream = loadSampleX12FromResource(resourceName);

        X12Reader reader = new X12Reader(fileTypeObject, x12InputStream);
        this.transactionDef = reader.getDefinition();
    }

    private static ByteArrayInputStream loadSampleX12FromResource(String resourceName) throws IOException {
        // Access the resource using the class loader
        try (InputStream inputStream = X12Parser.class.getClassLoader().getResourceAsStream(resourceName)) {
            if (inputStream == null) {
                throw new IOException("Resource not found: " + resourceName);
            }

            // Read the InputStream into a byte array
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                byteArrayOutputStream.write(buffer, 0, bytesRead);
            }

            // Convert the byte array to ByteArrayInputStream
            return new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        }
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
        errorsField.put("name", "Errors");

        Map<String, Object> errorsType = new HashMap<>();
        errorsType.put("type", "array");
        errorsType.put("elementType", "string");
        errorsType.put("containsNull", true);

        errorsField.put("type", errorsType);
        errorsField.put("nullable", true);
        errorsField.put("metadata", new HashMap<String, Object>());

        errorFields.add(errorsField);

        // Define the FatalErrors field
        Map<String, Object> fatalErrorsField = new HashMap<>();
        fatalErrorsField.put("name", "FatalErrors");

        Map<String, Object> fatalErrorsType = new HashMap<>();
        fatalErrorsType.put("type", "array");
        fatalErrorsType.put("elementType", "string");
        fatalErrorsType.put("containsNull", true);

        fatalErrorsField.put("type", fatalErrorsType);
        fatalErrorsField.put("nullable", true);
        fatalErrorsField.put("metadata", new HashMap<String, Object>());

        errorFields.add(fatalErrorsField);

        return errorFields;
    }
    private Map<String, Object> captureLoopDefinitions(LoopDefinition loopDef, boolean isFirstLoop) {
        if (loopDef == null) {
            return null;
        }
        Map<String, Object> loopMap = new HashMap<>();
        loopMap.put("name", loopDef.getXid() + "_" + loopDef.getName().replace(' ', '_').replaceAll("[^a-zA-Z0-9_]", "").toLowerCase());

        loopMap.put("nullable", true);
        loopMap.put("metadata", new HashMap<String, Object>());

        List<Map<String, Object>> segmentList = new ArrayList<>();
        List<SegmentDefinition> segmentDefs = loopDef.getSegment();
        if (segmentDefs != null) {
            for (SegmentDefinition segmentDef : segmentDefs) {
                if (segmentDef.getUsage().name().equals("NOT_USED")) {
                    continue;
                }

                Map<String, Object> segmentMap = new HashMap<>();
                segmentMap.put("name", segmentDef.getXid() + "_" + segmentDef.getName().replace(' ', '_').replaceAll("[^a-zA-Z0-9_]", "").toLowerCase());
                segmentMap.put("nullable", true);
                segmentMap.put("metadata", new HashMap<String, Object>());

                List<Map<String, Object>> elementList = new ArrayList<>();
                List<ElementDefinition> elementDefs = segmentDef.getElements();
                if (elementDefs != null) {
                    for (ElementDefinition elementDef : elementDefs) {
                        if (elementDef.getUsage().name().equals("NOT_USED")) {
                            continue;
                        }
                        Map<String, Object> elementMap = new HashMap<>();
                        elementMap.put("name", elementDef.getXid() + "_" + elementDef.getName().replace(' ', '_').replaceAll("[^a-zA-Z0-9_]", "").toLowerCase());
                        elementMap.put("type", "string");
                        elementMap.put("nullable", true);
                        elementMap.put("metadata", new HashMap<String, Object>());
                        elementList.add(elementMap);
                    }
                }

                List<CompositeDefinition> comDefs = segmentDef.getComposites();
                if (comDefs != null) {
                    for (CompositeDefinition comDef : comDefs) {
                        if (comDef.getUsage().toString().equals("N")) {
                            continue;
                        }
                        Map<String, Object> elementMap = new HashMap<>();
                        elementMap.put("name", comDef.getXid() + "_" + comDef.getName().replace(' ', '_').replaceAll("[^a-zA-Z0-9_]", "").toLowerCase());
                        elementMap.put("type", "string");
                        elementMap.put("nullable", true);
                        elementMap.put("metadata", new HashMap<String, Object>());
                        elementList.add(elementMap);
                    }
                }

                Map<String, Object> typeMap = new HashMap<>();
                if (segmentDef.getMaxUse().equals("1")) {
                    // struct
                    typeMap.put("type", "struct");
                    typeMap.put("fields", elementList);
                } else {
                    // array
                    typeMap.put("type", "array");
                    typeMap.put("containsNull", true);

                    Map<String, Object> elementTypeMap = new HashMap<>();
                    elementTypeMap.put("type", "struct");
                    elementTypeMap.put("fields", elementList);

                    typeMap.put("elementType", elementTypeMap);
                }

                segmentMap.put("type", typeMap);
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
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        String jsonOutput = gson.toJson(structure.get("type"));

        try (FileWriter writer = new FileWriter(filePath)) {
            writer.write(jsonOutput);
        }
    }

    public String getJsonSchema() {
        try {
            Map<String, Object> structuredData = captureDefinitions();
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            return gson.toJson(structuredData.get("type"));
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to get schema", e);
        }
    }


//    public static void main(String[] args) {
//        try {
//            X12Reader reader837 = new X12Reader(X12Reader.FileType.ANSI837_5010_X222, new File("837X222sample"));
//
//            // Assuming you have a valid TransactionDefinition object
//            TransactionDefinition transactionDef = reader837.getDefinition();
//            BuildJsonSchema mapper = new BuildJsonSchema(transactionDef);
//
//            // Capture hierarchical data
//            Map<String, Object> structuredData = mapper.captureDefinitions();
//
//            // Save JSON to file
//            String filePath = "837schema.json";
//            mapper.saveToJsonFile(filePath, structuredData);
////            System.out.println(structuredData);
//
//            System.out.println("JSON saved to " + filePath);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
}