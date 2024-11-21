package com.imsweb.x12;

import com.imsweb.x12.writer.X12Writer;
import com.imsweb.x12.reader.X12Reader.FileType;
import com.imsweb.x12.reader.X12Reader;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class X12Test {

    public static void main(String[] args) throws IOException {
        X12Reader reader837 = new X12Reader(FileType.ANSI837_5010_X222, new File("837sample"));

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
                jsonArray.put(processLoop(loop));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("FatalErrors", reader837.getFatalErrors());
        jsonObject.put("Errors", reader837.getErrors());

        // Add the newly created JSONObject to the jsonArray
        jsonArray.put(jsonObject);


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
//        loopJson.put("loopname", loopId);

        JSONObject loopDefinitions = definitionJson.optJSONObject(loopId);

        Map<String, JSONArray> segmentMap = new HashMap<>();

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

            if (segmentMap.containsKey(segmentKeyWithBusinessValue)) {
                segmentMap.get(segmentKeyWithBusinessValue).put(segmentJson);
            } else {
                JSONArray segmentArray = new JSONArray();
                segmentArray.put(segmentJson);
                segmentMap.put(segmentKeyWithBusinessValue, segmentArray);
            }
        }

        for (Map.Entry<String, JSONArray> entry : segmentMap.entrySet()) {
            if (entry.getValue().length() == 1) {
                loopJson.put(entry.getKey(), entry.getValue().get(0));
            } else {
                loopJson.put(entry.getKey(), entry.getValue());
            }
        }

        Map<String, JSONArray> childLoopsMap = new HashMap<>();

        for (Loop childLoop : loop.getLoops()) {
            String childLoopId = childLoop.getId();
            JSONObject childLoopJson = processLoop(childLoop);

//            JSONObject childloopDefinitions = definitionJson.optJSONObject(childLoopId);
            String childDef = definitionJson.optJSONObject(childLoopId).optString("name").replace(' ', '_').replaceAll("[^a-zA-Z0-9_]", "").toLowerCase();
            String childLoopKeyWithBusinessValue = childLoopId + "_" + childDef;

            if (childLoopsMap.containsKey(childLoopKeyWithBusinessValue)) {
                childLoopsMap.get(childLoopKeyWithBusinessValue).put(childLoopJson);
            } else {
                JSONArray childArray = new JSONArray();
                childArray.put(childLoopJson);
                childLoopsMap.put(childLoopKeyWithBusinessValue, childArray);
            }
        }

        for (Map.Entry<String, JSONArray> entry : childLoopsMap.entrySet()) {
            loopJson.put(entry.getKey(), entry.getValue());
        }

        return loopJson;
    }
}