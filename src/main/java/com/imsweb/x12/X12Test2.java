package com.imsweb.x12;

import com.imsweb.x12.reader.X12Reader;
import com.imsweb.x12.reader.X12Reader.FileType;
import com.imsweb.x12.writer.X12Writer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class X12Test2 {

     public static void main(String[] args) throws IOException {
          // Load the definition map from file
          String definitionFilePath = "837definitions.txt";
          Map<String, String> definitionMap = loadDefinitionMapFromFile(definitionFilePath);

          // Flag to determine if definitions should be used
          boolean useDefinitions = false; // or set this based on your logic

          X12Reader reader837 = new X12Reader(FileType.ANSI837_5010_X222, new File("/Users/weiwu/Documents/git/hl7test/src/main/java/com/example/837sample"));
          List<Loop> loops837 = reader837.getLoops();

          // You can uncomment and use other readers similarly if needed
          // X12Reader reader835 = new X12Reader(FileType.ANSI835_5010_X221, new File("/Users/weiwu/Documents/git/hl7test/src/main/java/com/example/835sample"));
          // List<Loop> loops835 = reader835.getLoops();

          // X12Reader reader834 = new X12Reader(FileType.ANSI834_5010_X220, new File("/Users/weiwu/Documents/git/hl7test/src/main/java/com/example/834sample"));
          // List<Loop> loops834 = reader834.getLoops();

          X12Reader reader820 = new X12Reader(FileType.ANSI820_5010_X218, new File("820sample"));
          List<Loop> loops820 = reader820.getLoops();

          JSONArray jsonArray = new JSONArray();

          for (Loop loop : loops837) {
               jsonArray.put(processLoop(loop, definitionMap, useDefinitions));
          }

          X12Writer writer = new X12Writer(reader837);

          System.out.println(jsonArray.toString(2));
     }

     private static JSONObject processLoop(Loop loop, Map<String, String> definitionMap, boolean useDefinitions) {
          JSONObject loopJson = new JSONObject();
//          String loopName = getMappedName(loop.getId(), definitionMap, useDefinitions);
          loopJson.put("loopname", loop.getId());

          for (Segment segment : loop.getSegments()) {
               JSONObject segmentJson = new JSONObject();
               List<Element> elements = segment.getElements();
               for (int index = 0; index < elements.size(); index++) {
                    Element element = elements.get(index);
                    String elementIndex = String.format("%02d", index + 1);
                    String elementKey = getMappedName(segment.getId() + elementIndex, definitionMap, useDefinitions);
                    segmentJson.put(elementKey, element.getValue());
               }
               String segmentKey = getMappedName(segment.getId(), definitionMap, useDefinitions);
               loopJson.put(segmentKey, segmentJson);
          }

          for (Loop childLoop : loop.getLoops()) {
               String childLoopKey = getMappedName(childLoop.getId(), definitionMap, useDefinitions);
               loopJson.put(childLoopKey, processLoop(childLoop, definitionMap, useDefinitions));
          }

          return loopJson;
     }

     private static String getMappedName(String id, Map<String, String> definitionMap, boolean useDefinitions) {
          return useDefinitions ? definitionMap.getOrDefault(id, id) : id;
     }

     private static Map<String, String> loadDefinitionMapFromFile(String filePath) throws IOException {
          Map<String, String> definitionMap = new HashMap<>();

          try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
               String line;
               while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("=", 2);
                    if (parts.length == 2) {
                         definitionMap.put(parts[0], parts[1]);
                    }
               }
          }

          return definitionMap;
     }
}