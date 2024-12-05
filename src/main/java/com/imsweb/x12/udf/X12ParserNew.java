package com.imsweb.x12.udf;

import com.imsweb.x12.Element;
import com.imsweb.x12.Loop;
import com.imsweb.x12.Segment;
import com.imsweb.x12.X12Test;
import com.imsweb.x12.mapping.ElementDefinition;
import com.imsweb.x12.mapping.LoopDefinition;
import com.imsweb.x12.mapping.SegmentDefinition;
import com.imsweb.x12.mapping.CompositeDefinition;
import com.imsweb.x12.reader.X12Reader;
import com.imsweb.x12.reader.X12Reader.FileType;
import org.apache.spark.sql.api.java.UDF1;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class X12ParserNew implements UDF1<String, String> {

    private static final long serialVersionUID = 1L;

    public String call(String inputText) {
        String fileTypeString = null;
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
                    if (lineNumber == 3) {
//                        System.out.println("Third line: " + line);
                        String[] splitArray = line.split("\\*");
                        fileTypeString = splitArray[1] + "_" + splitArray[3];
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
            }
            else if (fileTypeString.contains("834_005010")) {
                fileTypeObject = FileType.ANSI834_5010_X220;
            }
            else if (fileTypeString.contains("832_005010")) {
                fileTypeObject = FileType.ANSI820_5010_X218;
            }
            else {
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
            return e.toString();
        }
    }

    public static SegmentDefinition findSegmentByName(LoopDefinition loopDef, String segmentId) {

        List<SegmentDefinition> segmentDefs = loopDef.getSegment();

        for (SegmentDefinition obj : segmentDefs) {
            if (obj.getXid().equals(segmentId)) {
                return obj;
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




    private static JSONObject processLoop(Loop loop, LoopDefinition loopDef) throws IOException {

        if (!loop.getId().equals(loopDef.getXid())) {
            throw new IllegalArgumentException("Error:data loop " + loop.getId() + " doesn't match loop def " + loopDef.getXid());
        }

        JSONObject loopJson = new JSONObject();

        Map<String, Object> segmentResults = new HashMap<>();

        for (Segment segment : loop.getSegments()) {
            String segmentId = segment.getId();

            SegmentDefinition segmentDef = findSegmentByName(loopDef, segment.getId());

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

        for (Map.Entry<String, Object> entry : segmentResults.entrySet()) {
            loopJson.put(entry.getKey(), entry.getValue());
        }

        // Handle child loops
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

        return loopJson;
    }


}