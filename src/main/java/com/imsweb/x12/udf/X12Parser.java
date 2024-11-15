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

//import java.io.File;
//import java.nio.file.Files;
//import java.nio.file.Paths;

public class X12Parser implements UDF1<String, String> {

    private static final long serialVersionUID = 1L;

//    public static void main(String[] args) {
//        // Example of using a hardcoded file path for testing
//        File file = new File("/Users/weiwu/Documents/git/hl7test/src/main/java/com/example/837sample");
//
//        if (!file.exists()) {
//            System.err.println("The file does not exist: " + file.getAbsolutePath());
//            System.exit(1);
//        }
//
//        try {
//            // Read the content of the file into a String
//            String inputText = new String(Files.readAllBytes(Paths.get(file.toURI())), StandardCharsets.UTF_8);
//            X12Parser udf = new X12Parser();
//            String result = udf.call(inputText);
//            System.out.println("Result: " + result);
//        } catch (Exception e) {
//            System.err.println("Error reading or processing file: " + e.toString());
//        }
//    }


    public String call(String inputText) {
        try {
            InputStream inputStream = new ByteArrayInputStream(inputText.getBytes(StandardCharsets.UTF_8));
            X12Reader x12reader = new X12Reader(FileType.ANSI837_5010_X222, inputStream);
            List<Loop> loops = x12reader.getLoops();

            JSONArray jsonArray = new JSONArray();

            for (Loop loop : loops) {
                JSONObject jsonObject = processLoop(loop);
                jsonArray.put(jsonObject);
            }

            return jsonArray.toString();
        } catch (Exception e)
        {
            return e.toString();
        }
    }


    private static JSONObject processLoop(Loop loop) throws IOException {

        String definitionFilePath = "837definitions.txt";
        Map<String, String> definitionMap = loadDefinitionMapFromResource(definitionFilePath);

        JSONObject loopJson = new JSONObject();
        String loopId = loop.getId();
        loopJson.put("loopname", loopId);

        for (Segment segment : loop.getSegments()) {
            JSONObject segmentJson = new JSONObject();
            List<Element> elements = segment.getElements();
            for (int index = 0; index < elements.size(); index++) {
                String formattedIndex = String.format("%02d", index + 1);
                String segmentId = segment.getId() + formattedIndex;
                String segmentKeyWithBusinessValue = segmentId + "_" + getBusinessValue(definitionMap, segmentId);
                segmentJson.put(segmentKeyWithBusinessValue, elements.get(index));
            }

            String segmentId = segment.getId();
            String segmentKeyWithBusinessValue = segmentId + "_" + getBusinessValue(definitionMap, segmentId);
            loopJson.put(segmentKeyWithBusinessValue, segmentJson);
        }

        Map<String, JSONArray> childLoopsMap = new HashMap<>();

        for (Loop childLoop : loop.getLoops()) {
            String childLoopId = childLoop.getId();
            JSONObject childLoopJson = processLoop(childLoop);

            String childLoopKeyWithBusinessValue = childLoopId + "_" + getBusinessValue(definitionMap, childLoopId);

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

    private static String getBusinessValue(Map<String, String> definitionMap, String id) {
        String businessValue = definitionMap.getOrDefault(id, "");
        businessValue = businessValue.toLowerCase();
        businessValue = businessValue.replace(" ", "_");
        businessValue = businessValue.replaceAll("[^a-z0-9_]", "");

        return businessValue;
    }

    private static Map<String, String> loadDefinitionMapFromResource(String resourceName) throws IOException {
        Map<String, String> definitionMap = new HashMap<>();

        // Open the resource as a stream
        try (InputStream inputStream = X12Parser.class.getClassLoader().getResourceAsStream(resourceName)) {
            if (inputStream == null) {
                throw new IOException("Resource not found: " + resourceName);
            }
            // Read the stream using BufferedReader
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("=", 2);
                    if (parts.length == 2) {
                        definitionMap.put(parts[0], parts[1]);
                    }
                }
            }
        }

        return definitionMap;
    }
}