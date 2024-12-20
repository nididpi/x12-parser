package com.imsweb.x12.udf;

import com.imsweb.x12.Element;
import com.imsweb.x12.Loop;
import com.imsweb.x12.Segment;
import com.imsweb.x12.mapping.*;
import com.imsweb.x12.reader.X12Reader;
import com.imsweb.x12.reader.X12Reader.FileType;
import org.apache.spark.sql.api.java.UDF1;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class X12Parser implements UDF1<String, String> {

    private static final long serialVersionUID = 1L;

    private static final Map<String, FileType> FILE_TYPE_MAP = new HashMap<>();

    static {
        FILE_TYPE_MAP.put("837_005010X231", FileType.ANSI837_5010_X231);
        FILE_TYPE_MAP.put("837_005010X223", FileType.ANSI837_5010_X223);
        FILE_TYPE_MAP.put("837_005010X222", FileType.ANSI837_5010_X222);
        FILE_TYPE_MAP.put("837_005010X224", FileType.ANSI837_5010_X224);
//        FILE_TYPE_MAP.put("837_005010X098", FileType.ANSI837_4010_X098);
//        FILE_TYPE_MAP.put("837_005010X097", FileType.ANSI837_4010_X097);
//        FILE_TYPE_MAP.put("837_005010X096", FileType.ANSI837_4010_X096);
        FILE_TYPE_MAP.put("835_005010X221", FileType.ANSI835_5010_X221);
//        FILE_TYPE_MAP.put("835_004010X091", FileType.ANSI835_4010_X091);
        FILE_TYPE_MAP.put("834_005010X220", FileType.ANSI834_5010_X220);
//        FILE_TYPE_MAP.put("820_005010", FileType.ANSI837_5010_X222);
    }

    public String call(String inputText) {

        String fileTypeString = null;
        String fileSpecString = null;

        try {
            // do this to read the file type
            byte[] byteContent = inputText.getBytes(StandardCharsets.UTF_8);
            InputStream inputStream = new ByteArrayInputStream(byteContent);
            String[] lines = new String[0];

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
                StringBuilder contentBuilder = new StringBuilder();
                String line;

                while ((line = reader.readLine()) != null) {
                    contentBuilder.append(line);
                }
                String content = contentBuilder.toString();
                lines = content.split("~");


            } catch (IOException e) {
                e.printStackTrace();
            }
            for (int lineNumber = 0; lineNumber < lines.length; lineNumber++) {
                String line = lines[lineNumber];
                if (lineNumber == 1 && line.contains("*")) {
                    String[] splitArray = line.split("\\*");
                    if (splitArray.length > 8) {
                        fileSpecString = splitArray[8]; // Ensure the index is safe
                    }
                }
                if (lineNumber == 2 && line.contains("*")) {
                    String[] splitArray = line.split("\\*");
                    if (splitArray.length > 1 && fileSpecString != null) {
                        fileTypeString = splitArray[1] + "_" + fileSpecString;
                        break;
                    }
                }
            }


            FileType fileTypeObject = determineFileType(fileTypeString);

            // Create a new InputStream for X12Reader
            InputStream freshInputStream = new ByteArrayInputStream(byteContent);

            X12Reader x12reader = new X12Reader(fileTypeObject, freshInputStream);
            List<Loop> loops = x12reader.getLoops();
            LoopDefinition loopDef = x12reader.getDefinition().getLoop();

            JSONArray jsonArray = new JSONArray();

            for (Loop loop : loops) {
                JSONObject jsonObject = processLoop(loop, loopDef);
                jsonObject.put("FatalErrors", x12reader.getFatalErrors());
                jsonObject.put("Errors", x12reader.getErrors());
                jsonArray.put(jsonObject);
            }

            return jsonArray.toString();
        } catch (Exception e) {
            e.printStackTrace();
            return e.toString();
        }
    }

    private FileType determineFileType(String fileTypeString) {
        return FILE_TYPE_MAP.entrySet().stream()
                .filter(entry -> fileTypeString.contains(entry.getKey()))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown file type: " + fileTypeString));
    }

    private static Map<String, List<SegmentDefinition>> findSegmentByName(LoopDefinition loopDef, Segment segment, String missingId) {
        Map<String, List<SegmentDefinition>> matchMap = new HashMap<>();
        matchMap.put("matched", new ArrayList<>());
        matchMap.put("unmatched", new ArrayList<>());

        List<SegmentDefinition> segmentDefs = loopDef.getSegment();
        String segmentId = segment.getId();

        List<SegmentDefinition> matchSegDef = new ArrayList<>();

        for (SegmentDefinition obj : segmentDefs) {
            if (obj.getXid().equals(segmentId)) {
                matchSegDef.add(obj);
            }
        }

        if (missingId != null) {
            for (SegmentDefinition obj : matchSegDef) {
                if (obj.getXid().equals(segmentId) && !obj.getUsage().name().equals("NOT_USED")) {
                    boolean matchAllElement = true;

                    // Iterate through each ElementDefinition
                    if (obj.getElements() != null) {
                        for (ElementDefinition elementDef : obj.getElements()) {
                            if (elementDef.getUsage().toString().equals("NOT_USED")) {
                                continue;
                            }
                            String elementID = elementDef.getXid();
                            boolean isMatch = false;

                            // Check if valid codes are null, or if they contain the target value
                            if (elementDef.getValidCodes() == null) {
                                isMatch = true;
                            } else {
                                Element element = segment.getElement(elementID);
                                if (element == null || elementDef.getValidCodes().getCodes().contains(element.getValue())) {
                                    isMatch = true;
                                }
                            }

                            // If this element doesn't match, record it and flag as not all matching
                            if (!isMatch) {
                                matchAllElement = false;
                            }
                        }
                    }

                    if (obj.getComposites() != null) {
                        for (CompositeDefinition comDef : obj.getComposites()) {
                            if (comDef.getUsage().equals("N")) {
                                continue;
                            }
                            String elementID = comDef.getXid();

                            List<String> combinedValidCodes = new ArrayList<>();
                            for (ElementDefinition elementDef : comDef.getElements()) {
                                // Get valid codes for this element
                                ValidCodesDefinition validCodes = elementDef.getValidCodes();
                                if (validCodes != null) {
                                    combinedValidCodes.addAll(validCodes.getCodes());
                                }
                            }

                            String code = null;
                            if (segment.getElement(elementID) != null) {
                                String value = segment.getElement(elementID).getValue();
                                code = (value.split(":").length > 0) ? value.split(":")[0] : "";
                            }

                            // Only check for the first subElement
                            if (combinedValidCodes.contains(code) || code == null) {
                                break;
                            } else {
                                matchAllElement = false;
                            }

                        }
                    }

                    if (matchAllElement) {
                        matchMap.get("matched").add(obj);
                    } else {
                        matchMap.get("unmatched").add(obj);
                    }
                }
            }
            return matchMap;
        } else {
            for (SegmentDefinition obj : matchSegDef) {
                if (obj.getXid().equals(segmentId)) {
                    matchMap.get("matched").add(obj);
                    return matchMap;
                }
            }
        }

        throw new IllegalArgumentException("Error: The segment name '" + segmentId + "' does not exist in " + loopDef.getXid());
    }

    private static String findElementByName(SegmentDefinition segDef, String elementId) {

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

        throw new IllegalArgumentException("Error: The element name '" + elementId + "' does not exist in " + segDef.getXid());
    }

    private static LoopDefinition findLoopByName(LoopDefinition loopDef, String loopId) {

        List<LoopDefinition> loopDefs = loopDef.getLoop();

        for (LoopDefinition obj : loopDefs) {
            if (obj.getXid().equals(loopId)) {
                return obj;
            }
        }
        throw new IllegalArgumentException("Error: The loop name '" + loopId + "' does not exist in " + loopDef.getXid());
    }

    private static Map<String, Object> processSegments(Loop loop, LoopDefinition loopDef, List<String> missingSegmentIds) {
        Map<String, List<SegmentDefinition>> matchMap;
        Map<String, Object> segmentResults = new HashMap<>();
        List<SegmentDefinition> missingSegments = new ArrayList<>();

        if (loop != null) {
            for (Segment segment : loop.getSegments()) {
                String segmentId = segment.getId();

                if (missingSegmentIds.contains(segmentId)) {
                    matchMap = findSegmentByName(loopDef, segment, segmentId);
                } else {
                    matchMap = findSegmentByName(loopDef, segment, null);
                }
                SegmentDefinition segmentDef = matchMap.get("matched").get(0);

                if (matchMap != null) {
                    missingSegments.addAll(matchMap.get("unmatched"));
                }

                JSONObject segmentJson = new JSONObject();
                List<Element> elements = segment.getElements();

                for (Element element : elements) {
                    String elementId = element.getId();

                    String elementDef = findElementByName(segmentDef, elementId);
                    String elementName = elementId + "_" + elementDef.replace(' ', '_').replaceAll("[^a-zA-Z0-9_]", "").toLowerCase();
                    segmentJson.put(elementName, element.getValue());
                }

                String segmentName = segmentDef.getName().replace(' ', '_').replaceAll("[^a-zA-Z0-9_]", "").toLowerCase();
                String segmentKeyWithBusinessValue = segmentId + "_" + segmentName;
                String datatype = segmentDef.getMaxUse().equals("1") ? "struct" : "array";

                if ("array".equals(datatype)) {
                    JSONArray segmentArray = (JSONArray) segmentResults.getOrDefault(segmentKeyWithBusinessValue, new JSONArray());
                    segmentArray.put(segmentJson);
                    segmentResults.put(segmentKeyWithBusinessValue, segmentArray);
                } else {
                    segmentResults.put(segmentKeyWithBusinessValue, segmentJson);
                }
            }
        }

        // missing
        Set<String> segmentIds;

        if (loop != null && loop.getSegments() != null) {
            segmentIds = loop.getSegments().stream().map(Segment::getId).collect(Collectors.toSet());
        } else {
            segmentIds = Collections.emptySet();
        }

        // Iterate over SegmentDefinitions to classify them
        if (loopDef.getSegment() != null) {
            for (SegmentDefinition missSegmentDef : loopDef.getSegment()) {
                if (!segmentIds.contains(missSegmentDef.getXid())) {
                    missingSegments.add(missSegmentDef);
                }
            }
        }

        for (SegmentDefinition segmentDef : missingSegments) {
            if (segmentDef.getUsage().name().equals("NOT_USED")) {
                continue;
            }

            JSONObject segmentJson = new JSONObject();
            List<ElementDefinition> elementDefs = segmentDef.getElements();
            List<CompositeDefinition> comDefs = segmentDef.getComposites();

            if (elementDefs != null) {
                for (ElementDefinition elementDef : elementDefs) {
                    if (elementDef.getUsage().name().equals("NOT_USED")) {
                        continue;
                    }
                    segmentJson.put(elementDef.getXid() + "_" + elementDef.getName().replace(' ', '_').replaceAll("[^a-zA-Z0-9_]", "").toLowerCase(), JSONObject.NULL);
                }
            }

            if (comDefs != null) {
                for (CompositeDefinition comDef : comDefs) {
                    if (comDef.getUsage().toString().equals("N")) {
                        continue;
                    }
                    segmentJson.put(comDef.getXid() + "_" + comDef.getName().replace(' ', '_').replaceAll("[^a-zA-Z0-9_]", "").toLowerCase(), JSONObject.NULL);
                }
            }

            String segmentKeyWithBusinessValue = segmentDef.getXid() + "_" + segmentDef.getName().replace(' ', '_').replaceAll("[^a-zA-Z0-9_]", "").toLowerCase();

            if ("1".equals(segmentDef.getMaxUse())) {
                segmentResults.put(segmentKeyWithBusinessValue, segmentJson);
            } else {
                JSONArray segmentArray = (JSONArray) segmentResults.getOrDefault(segmentKeyWithBusinessValue, new JSONArray());
                segmentArray.put(segmentJson);
                segmentResults.put(segmentKeyWithBusinessValue, segmentArray);
            }

        }

        return segmentResults;
    }

    private static JSONObject processLoop(Loop loop, LoopDefinition loopDef) {

        if (loop != null && !loop.getId().equals(loopDef.getXid())) {
            throw new IllegalArgumentException("Error:data loop " + loop.getId() + " doesn't match loop def " + loopDef.getXid());
        }

        JSONObject loopJson = new JSONObject();


        // Handle segment
        Map<String, Integer> idCountInLoopDef = new HashMap<>();
        Map<String, Integer> idCountInLoop = new HashMap<>();
        if (loopDef.getSegment() != null) {
            for (SegmentDefinition segmentDef : loopDef.getSegment()) {
                idCountInLoopDef.merge(segmentDef.getXid(), 1, Integer::sum);
            }
        }

        // Count occurrences of IDs in loopSegments
        if (loop != null) {
            for (Segment segment : loop.getSegments()) {
                idCountInLoop.merge(segment.getId(), 1, Integer::sum);
            }
        }

        List<String> missingSegmentIds = new ArrayList<>();
        idCountInLoopDef.forEach((id, countInLoopDef) -> {
            if (countInLoopDef > 1) {
                int countInLoop = idCountInLoop.getOrDefault(id, 0);
                if (countInLoop != countInLoopDef) {
                    missingSegmentIds.add(id);
                }
            }
        });

        Map<String, Object> segmentResults = processSegments(loop, loopDef, missingSegmentIds);


        // put segment data to current loop json
        for (Map.Entry<String, Object> entry : segmentResults.entrySet()) {
            loopJson.put(entry.getKey(), entry.getValue());
        }

        // Handle child loops
        if (loop != null) {
            for (Loop childLoop : loop.getLoops()) {
                String childLoopId = childLoop.getId();

                LoopDefinition childLoopDef = findLoopByName(loopDef, childLoopId);

                JSONObject childLoopJson = processLoop(childLoop, childLoopDef);

                String datatype = childLoopDef.getRepeat().equals("1") ? "struct" : "array";

                String childDef = childLoopDef.getName().replace(' ', '_').replaceAll("[^a-zA-Z0-9_]", "").toLowerCase();
                String childLoopKeyWithBusinessValue = childLoopId + "_" + childDef;

                if ("array".equals(datatype)) {
                    JSONArray childArray = loopJson.optJSONArray(childLoopKeyWithBusinessValue);
                    if (childArray == null) {
                        childArray = new JSONArray();
                        loopJson.put(childLoopKeyWithBusinessValue, childArray);
                    }
                    childArray.put(childLoopJson);
                } else {
                    loopJson.put(childLoopKeyWithBusinessValue, childLoopJson);
                }
            }
        }

        // Handle missing childloop
        Set<String> looptIds;

        if (loop != null && loop.getSegments() != null) {
            looptIds = loop.getLoops().stream().map(Loop::getId).collect(Collectors.toSet());
        } else {
            looptIds = Collections.emptySet();
        }

        List<LoopDefinition> missingLoops = new ArrayList<>();

        // Iterate over SegmentDefinitions to classify them
        if (loopDef.getLoop() != null) {
            for (LoopDefinition missLoopDef : loopDef.getLoop()) {
                if (!looptIds.contains(missLoopDef.getXid())) {
                    missingLoops.add(missLoopDef);
                }
            }
        }

        for (LoopDefinition childLoopDef : missingLoops) {
            if (childLoopDef.getUsage().toString().equals("NOT_USED")) {
                continue;
            }
            String childLoopId = childLoopDef.getXid();

            JSONObject childLoopJson = processLoop(null, childLoopDef);

            String datatype = childLoopDef.getRepeat().equals("1") ? "struct" : "array";

            String childDef = childLoopDef.getName().replace(' ', '_').replaceAll("[^a-zA-Z0-9_]", "").toLowerCase();
            String childLoopKeyWithBusinessValue = childLoopId + "_" + childDef;

            if ("array".equals(datatype)) {
                JSONArray childArray = loopJson.optJSONArray(childLoopKeyWithBusinessValue);
                if (childArray == null) {
                    childArray = new JSONArray();
                    loopJson.put(childLoopKeyWithBusinessValue, childArray);
                }
                childArray.put(childLoopJson);
            } else {
                loopJson.put(childLoopKeyWithBusinessValue, childLoopJson);
            }
        }


        return loopJson;
    }

}