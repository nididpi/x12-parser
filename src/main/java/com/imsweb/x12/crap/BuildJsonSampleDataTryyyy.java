package com.imsweb.x12.crap;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.imsweb.x12.Element;
import com.imsweb.x12.Loop;
import com.imsweb.x12.Segment;
import com.imsweb.x12.mapping.*;
import com.imsweb.x12.reader.X12Reader;
import org.json.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class BuildJsonSampleDataTryyyy {

    public static Map<String, Object> captureDefinitions(TransactionDefinition transactionDef, List<Loop> loops) {
        return captureLoopDefinitions(transactionDef.getLoop(), loops, true);
    }

    private static Map<String, Object> captureLoopDefinitions(LoopDefinition loopDef, List<Loop> loops, boolean isFirstLoop) {
        if (loopDef == null) {
            return null;
        }

        Map<String, Object> loopMap = new HashMap<>();

        for (Loop loop : loops) {
            String a = "a";

            if (!loop.getId().equals(loopDef.getXid())) {
                throw new IllegalArgumentException("Error:data loop " + loop.getId() + " doesn't match loop def " + loopDef.getXid());
            }
            JSONObject loopJson = new JSONObject();

            loopMap = new HashMap<>();
            List<Map<String, Object>> segmentList = new ArrayList<>();
            segmentList = processSegments(loopDef, loop);

            List<Map<String, Object>> childLoops = new ArrayList<>();
            List<LoopDefinition> childLoopDefs = loopDef.getLoop();

            if (childLoopDefs != null) {
                for (LoopDefinition childLoopDef : childLoopDefs) {
                    if (childLoopDef.getUsage().name().equals("NOT_USED")) {
                        continue;
                    }
                    childLoops.add(captureLoopDefinitions(childLoopDef, loops, false));
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


        }

        return loopMap;
    }

    private static List<Map<String, Object>> processSegments(LoopDefinition loopDef, Loop loop) {
        List<Map<String, Object>> segmentList = new ArrayList<>();
        List<SegmentDefinition> segmentDefs = loopDef.getSegment();
        List<Segment> segments = loop.getSegments();

        // Set to store Segment IDs for fast lookup.
        Set<String> segmentIds = segments.stream()
                .map(Segment::getId)
                .collect(Collectors.toSet());

        // Collect SegmentDefinitions that are missing corresponding Segments.
        List<SegmentDefinition> missingSegments = new ArrayList<>();
        List<SegmentDefinition> nonMissingSegments = new ArrayList<>();

        // Iterate over SegmentDefinitions to classify them
        for (SegmentDefinition segmentDef : segmentDefs) {
            if (segmentIds.contains(segmentDef.getXid())) {
                // If the SegmentDefinition's Xid is found among Segment IDs
                nonMissingSegments.add(segmentDef);
            } else {
                // If it's not found, it's missing
                missingSegments.add(segmentDef);
            }
        }


        //handling missing
        for (SegmentDefinition segmentDef : missingSegments) {
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


        //handle existing
        for (SegmentDefinition segmentDef : nonMissingSegments) {
            String SegmentId = segmentDef.getXid();
            loop.getSegment(SegmentId);

            Map<String, Object> segmentMap = new HashMap<>();


            Segment segment =  findSegmentById(segments,SegmentId);
            List<Element> elements = segment.getElements();

            for (Element element : elements) {
                String elementId = element.getId();

                String elementDef = findElementByName(segmentDef, elementId);
                String elementName = elementId + "_" + elementDef.replace(' ', '_').replaceAll("[^a-zA-Z0-9_]", "").toLowerCase();
                segmentMap.put(elementName, element.getValue());
            }

            segmentList.add(segmentMap);

        }



        return segmentList;
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

    public static String findElementByName(SegmentDefinition segDef, String elementId) {

        List<ElementDefinition> elementDefs = segDef.getElements();
        List<CompositeDefinition> elementComDefs = segDef.getComposites();

        if (elementDefs != null) {
            for (ElementDefinition obj : elementDefs) {
                if (obj.getXid().equals(elementId)) {
                    return obj.getName();
                }
            }
        }

        if (elementComDefs != null) {
            for (CompositeDefinition obj : elementComDefs) {
                if (obj.getXid().equals(elementId)) {
                    return obj.getName();
                }
            }
        }

        throw new IllegalArgumentException("Error: The element name '" + elementId + "' does not exist in the list.");
    }

    public static Segment findSegmentById(List<Segment> segments, String defId) {
        for (Segment segment : segments) {
            if (segment.getId().equals(defId)) {
                return segment;  // Return the segment if a match is found
            }
        }
        return null;  // Return null if no segment with the given ID is found
    }

    public static void main(String[] args) {
        try {
            X12Reader reader837 = new X12Reader(X12Reader.FileType.ANSI837_5010_X222, new File("837X222sample"));

            TransactionDefinition transactionDef = reader837.getDefinition();
            List<Loop> loops = reader837.getLoops();

            Map<String, Object> structuredData = captureDefinitions(transactionDef, loops);

            String filePath = "837sampledata.json";
            saveToJsonFile(filePath, structuredData);

            System.out.println("JSON saved to " + filePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

