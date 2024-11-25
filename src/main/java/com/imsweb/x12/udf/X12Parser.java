package com.imsweb.x12.udf;

import com.imsweb.x12.Element;
import com.imsweb.x12.Loop;
import com.imsweb.x12.Segment;
import com.imsweb.x12.X12Test;
import com.imsweb.x12.reader.X12Reader.FileType;
import com.imsweb.x12.reader.X12Reader;


import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.api.java.UDF1;

public class X12Parser implements UDF1<String, String> {

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
                        System.out.println("Third line: " + line);
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

            JSONArray jsonArray = new JSONArray();

            for (Loop loop : loops) {
                JSONObject jsonObject = processLoop(loop);
                jsonArray.put(jsonObject);
            }

            JSONObject jsonObject = new JSONObject();
            jsonObject.put("FatalErrors", x12reader.getFatalErrors());
            jsonObject.put("Errors", x12reader.getErrors());

            jsonArray.put(jsonObject);

            return jsonArray.toString();
        } catch (Exception e) {
            return e.toString();
        }
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