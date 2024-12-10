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

    public String call(String inputText) {
        String fileTypeString = null;
        String fileSpecString = null;
        FileType fileTypeObject;
        try {
            // do this to read the file type
            byte[] byteContent = inputText.getBytes(StandardCharsets.UTF_8);
            InputStream inputStream = new ByteArrayInputStream(byteContent);

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
                String line;
                int lineNumber = 0;

                while ((line = reader.readLine()) != null) {
                    lineNumber++;
                    if (lineNumber == 2) {
                        String[] splitArray = line.split("\\*");
                        fileSpecString = splitArray[8];
                    }
                    if (lineNumber == 3) {
                        String[] splitArray = line.split("\\*");
                        fileTypeString = splitArray[1] + "_" + fileSpecString;
                        break;
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            if (fileTypeString.contains("837_005010")) {
                fileTypeObject = FileType.ANSI837_5010_X222;
            } else if (fileTypeString.contains("835_005010")) {
                fileTypeObject = FileType.ANSI835_5010_X221;
            } else if (fileTypeString.contains("834_005010")) {
                fileTypeObject = FileType.ANSI834_5010_X220;
            } else if (fileTypeString.contains("820_005010")) {
                fileTypeObject = FileType.ANSI820_5010_X218;
            } else {
                throw new IllegalArgumentException("Unknown file type." + fileTypeString);
            }

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

    public static Map<String, List<SegmentDefinition>> findSegmentByName(LoopDefinition loopDef, Segment segment, String MissingId) {

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

        if (MissingId != null) {
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
                            boolean isMatch = false;

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

                            if (combinedValidCodes.contains(code) || code == null) {
//                                isMatch = true;
                                break;
                            } else {
                                isMatch = false;
                            }

                            // If this element doesn't match, record it and flag as not all matching
                            if (!isMatch) {
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

        throw new IllegalArgumentException("Error: The segment name '" + segmentId + "' does not exist in the list.");
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

    public static LoopDefinition findLoopByName(LoopDefinition loopDef, String loopId) {

        List<LoopDefinition> loopDefs = loopDef.getLoop();

        for (LoopDefinition obj : loopDefs) {
            if (obj.getXid().equals(loopId)) {
                return obj;
            }
        }
        throw new IllegalArgumentException("Error: The loop name '" + loopId + "' does not exist in the list.");
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


        Map<String, Object> segmentResults = new HashMap<>();
        Map<String, List<SegmentDefinition>> matchMap = null;
        List<SegmentDefinition> missingSegments = new ArrayList<>();

        if (loop != null) {
            for (Segment segment : loop.getSegments()) {
                String segmentId = segment.getId();

//                System.out.println(loop.getId() + "_" +  segmentId);
//                if (loop.getId().equals("1000A")) {
//                    String a = "1";
//                }

                if (missingSegmentIds.contains(segmentId)) {
                    matchMap = findSegmentByName(loopDef, segment, segmentId);
                } else {
                    matchMap = findSegmentByName(loopDef, segment, null);
                }
                SegmentDefinition segmentDef = matchMap.get("matched").get(0);

                if (matchMap != null) {
                    missingSegments.addAll(matchMap.get("unmatched"));
                }
//                System.out.println(missingSegments);
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

        // Handle missing segment

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
//        System.out.println("current loop: " + loop.getId());

        // Iterate over SegmentDefinitions to classify them
        if (loopDef.getLoop() != null) {
            for (LoopDefinition missLoopDef : loopDef.getLoop()) {
                if (!looptIds.contains(missLoopDef.getXid())) {
                    missingLoops.add(missLoopDef);
                }
            }
        }

        for (LoopDefinition childLoopDef : missingLoops) {
            if (childLoopDef.getUsage().toString().equals("NOT_USED")) {continue;}
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