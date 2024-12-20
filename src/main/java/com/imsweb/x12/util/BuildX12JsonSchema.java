package com.imsweb.x12.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.imsweb.x12.mapping.*;
import com.imsweb.x12.reader.X12Reader;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Enum to represent supported X12 file types and their associated sample resources.
 */
enum X12FileType {
    ANSI837_5010_X231(X12Reader.FileType.ANSI837_5010_X231, "x12/837X231sample"),
    ANSI837_5010_X223(X12Reader.FileType.ANSI837_5010_X223, "837X223sample"),
    ANSI837_5010_X222(X12Reader.FileType.ANSI837_5010_X222, "837X222sample"),
    ANSI837_5010_X224(X12Reader.FileType.ANSI837_5010_X224, "837X224sample"),
//    ANSI837_4010_X098(X12Reader.FileType.ANSI837_4010_X098, "837X222sample"),
//    ANSI837_4010_X097(X12Reader.FileType.ANSI837_4010_X097, "837X222sample"),
//    ANSI837_4010_X096(X12Reader.FileType.ANSI837_4010_X096, "837X222sample"),
    ANSI835_5010_X221(X12Reader.FileType.ANSI835_5010_X221, "x12/835X221sample");
//    ANSI835_4010_X091(X12Reader.FileType.ANSI835_4010_X091, "835X221sample"),
//    ANSI834_5010_X220(X12Reader.FileType.ANSI835_4010_X091, "834sample"),
//    ANSI820_5010_X218(X12Reader.FileType.ANSI835_4010_X091, "820sample");

    private final X12Reader.FileType fileType;
    private final String resourceName;

    X12FileType(X12Reader.FileType fileType, String resourceName) {
        this.fileType = fileType;
        this.resourceName = resourceName;
    }

    public X12Reader.FileType getFileType() {
        return fileType;
    }

    public String getResourceName() {
        return resourceName;
    }

    public static X12FileType fromString(String fileTypeString) {
        for (X12FileType type : X12FileType.values()) {
            if (type.name().contains(fileTypeString)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown file type: " + fileTypeString);
    }
}

/**
 * Utility class for building JSON schema from X12 transaction files.
 */
public class BuildX12JsonSchema {

    private final TransactionDefinition transactionDef;

    /**
     * Constructs a BuildX12JsonSchema object based on the provided fileType.
     *
     * @param fileType the type of X12 file (e.g., "837_5010_X231").
     * @throws IOException if reading the sample X12 resource fails.
     */
    public BuildX12JsonSchema(String fileType) throws IOException {
        X12FileType x12FileType = X12FileType.fromString(fileType);

        InputStream x12InputStream = loadSampleX12FromResource(x12FileType.getResourceName());
        X12Reader reader = new X12Reader(x12FileType.getFileType(), x12InputStream);
        this.transactionDef = reader.getDefinition();
    }

    /**
     * Loads a sample X12 file from resources.
     *
     * @param resourceName the name of the resource file.
     * @return a ByteArrayInputStream of the resource file.
     * @throws IOException if the resource cannot be read.
     */
    private static ByteArrayInputStream loadSampleX12FromResource(String resourceName) throws IOException {
        try (InputStream inputStream = BuildX12JsonSchema.class.getClassLoader().getResourceAsStream(resourceName)) {
            if (inputStream == null) {
                throw new IOException("Resource not found: " + resourceName);
            }

            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                byteArrayOutputStream.write(buffer, 0, bytesRead);
            }

            return new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        }
    }

    /**
     * Captures the definitions of the transaction and organizes them into a map.
     *
     * @return a map representation of the loop definitions.
     */
    public Map<String, Object> captureDefinitions() {
        return captureLoopDefinitions(transactionDef.getLoop(), true);
    }

    public static String transformString(String input) {
        if (input == null) {
            return null;
        }

        return input.replace(' ', '_')
                .replaceAll("[^a-zA-Z0-9_]", "")
                .toLowerCase();
    }

    /**
     * Captures loop definitions and organizes them into a map.
     *
     * @param loopDef     the loop definition to process.
     * @param isFirstLoop a flag indicating if this is the first loop.
     * @return a map representation of the loop definitions.
     */
    private Map<String, Object> captureLoopDefinitions(LoopDefinition loopDef, boolean isFirstLoop) {
        if (loopDef == null) {
            return null;
        }
        Map<String, Object> loopMap = new HashMap<>();
        loopMap.put("name", loopDef.getXid() + "_" + transformString(loopDef.getName()));

        loopMap.put("nullable", true);
        loopMap.put("metadata", new HashMap<>());

        List<Map<String, Object>> segmentList = new ArrayList<>();
        List<SegmentDefinition> segmentDefs = loopDef.getSegment();
        if (segmentDefs != null) {
            for (SegmentDefinition segmentDef : segmentDefs) {
                if (segmentDef.getUsage().name().equals("NOT_USED")) {
                    continue;
                }

                Map<String, Object> segmentMap = new HashMap<>();
                segmentMap.put("name", segmentDef.getXid() + "_" + transformString(segmentDef.getName()));
                segmentMap.put("nullable", true);
                segmentMap.put("metadata", new HashMap<>());

                List<Map<String, Object>> elementList = new ArrayList<>();
                List<ElementDefinition> elementDefs = segmentDef.getElements();
                if (elementDefs != null) {
                    for (ElementDefinition elementDef : elementDefs) {
                        if (elementDef.getUsage().name().equals("NOT_USED")) {
                            continue;
                        }
                        Map<String, Object> elementMap = new HashMap<>();
                        elementMap.put("name", elementDef.getXid() + "_" + transformString(elementDef.getName()));
                        elementMap.put("type", "string");
                        elementMap.put("nullable", true);
                        elementMap.put("metadata", new HashMap<>());
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
                        elementMap.put("name", comDef.getXid() + "_" + transformString(comDef.getName()));
                        elementMap.put("type", "string");
                        elementMap.put("nullable", true);
                        elementMap.put("metadata", new HashMap<>());
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

    /**
     * Creates error fields to be appended to the schema.
     *
     * @return a list of error field maps.
     */
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
        errorsField.put("metadata", new HashMap<>());

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
        fatalErrorsField.put("metadata", new HashMap<>());

        errorFields.add(fatalErrorsField);

        return errorFields;
    }

    /**
     * Generates and returns the JSON schema of the X12 transaction.
     *
     * @return a JSON string representing the schema.
     */
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
}