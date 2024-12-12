package com.imsweb.x12;

import com.imsweb.x12.writer.X12Writer;
import com.imsweb.x12.reader.X12Reader.FileType;
import com.imsweb.x12.reader.X12Reader;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class X12Test {

    public static void main(String[] args) throws IOException {
        X12Reader reader837 = new X12Reader(FileType.ANSI837_5010_X222, new File("837X222sample"));

        List<Loop> loops837 = reader837.getLoops();
//
//          X12Reader reader835 = new X12Reader(FileType.ANSI835_5010_X221, new File("/Users/weiwu/Documents/git/hl7test/src/main/java/com/example/835sample"));
//
//          List<Loop> loops835 = reader835.getLoops();
//
//          X12Reader reader834 = new X12Reader(FileType.ANSI834_5010_X220, new File("/Users/weiwu/Documents/git/hl7test/src/main/java/com/example/834sample"));
//
//          List<Loop> loops834 = reader834.getLoops();

        X12Reader reader820 = new X12Reader(FileType.ANSI820_5010_X218, new File("820sample"));

        List<Loop> loops820 = reader820.getLoops();


        JSONArray jsonArray = new JSONArray();

        try {
            for (Loop loop : loops837) {
                JSONObject jsonObject = processLoop(loop);
                jsonObject.put("FatalErrors", reader837.getFatalErrors());
                jsonObject.put("Errors", reader837.getErrors());

                jsonArray.put(jsonObject);

//                jsonArray.put(processLoop(loop));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

//        JSONObject jsonObject = new JSONObject();
//        jsonObject.put("FatalErrors", reader837.getFatalErrors());
//        jsonObject.put("Errors", reader837.getErrors());

        // Add the newly created JSONObject to the jsonArray
//        jsonArray.put(jsonObject);


        X12Writer writer = new X12Writer(reader837);

        System.out.println(jsonArray.toString(2));
    }


    private static JSONObject loadDefinitionJsonFromResource(String resourceName) throws IOException {
        try (InputStream inputStream = X12Test.class.getClassLoader().getResourceAsStream(resourceName)) {
            if (inputStream == null) {
                throw new IOException("Resource not found: " + resourceName);
            }

            StringBuilder jsonContent = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    jsonContent.append(line);
                }
            }

            return new JSONObject(jsonContent.toString());
        }
    }

    // Adjusted processLoop to use JSON definitions
    private static JSONObject processLoop(Loop loop) throws IOException {

        JSONObject definitionJson = loadDefinitionJsonFromResource("837definitions.json");

        JSONObject loopJson = new JSONObject();
        String loopId = loop.getId();

        JSONObject loopDefinitions = definitionJson.optJSONObject(loopId);

        Map<String, Object> segmentResults = new HashMap<>();

        for (Segment segment : loop.getSegments()) {
            String segmentId = segment.getId();
            JSONObject segmentDefinitions = loopDefinitions != null ? loopDefinitions.optJSONObject(segmentId) : null;

            JSONObject elementDefinitions = segmentDefinitions != null ? segmentDefinitions.optJSONObject("elements") : null;

            JSONObject segmentJson = new JSONObject();
            List<Element> elements = segment.getElements();
            for (int index = 0; index < elements.size(); index++) {
                String formattedIndex = String.format("%02d", index + 1);
                String elementId = segmentId + formattedIndex;
                String elementDefinition = (elementDefinitions != null ? elementDefinitions.optString(elementId, "") : "");

                String elementKeyWithBusinessValue = elementId + "_" + elementDefinition.replace(' ', '_').replaceAll("[^a-zA-Z0-9_]", "").toLowerCase();
                segmentJson.put(elementKeyWithBusinessValue, elements.get(index));
            }

            String segmentName = (segmentDefinitions != null ? segmentDefinitions.optString("name", "") : "").replace(' ', '_').replaceAll("[^a-zA-Z0-9_]", "").toLowerCase();
            String segmentKeyWithBusinessValue = segmentId + "_" + segmentName;
            String datatype = segmentDefinitions != null ? segmentDefinitions.optString("datatype") : "struct";

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
            JSONObject childLoopJson = processLoop(childLoop);

            JSONObject childloopDefinitions = definitionJson.optJSONObject(childLoopId);
            String datatype = childloopDefinitions != null ? childloopDefinitions.optString("datatype") : "struct";

            String childDef = definitionJson.optJSONObject(childLoopId).optString("name").replace(' ', '_').replaceAll("[^a-zA-Z0-9_]", "").toLowerCase();
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