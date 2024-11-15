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

          for (Loop loop : loops837) {
               jsonArray.put(processLoop(loop));
          }

          X12Writer writer = new X12Writer(reader837);

          System.out.println(jsonArray.toString(2));
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
          try (InputStream inputStream = X12Test.class.getClassLoader().getResourceAsStream(resourceName)) {
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