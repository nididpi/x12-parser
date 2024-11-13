package com.imsweb.x12;

import com.imsweb.x12.writer.X12Writer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.util.List;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

import com.imsweb.x12.reader.X12Reader.FileType;
import com.imsweb.x12.reader.X12Reader;

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




     private static JSONObject processLoop(Loop loop) {
          JSONObject loopJson = new JSONObject();
          loopJson.put("loopname", loop.getId());

          for (Segment segment : loop.getSegments()) {
               JSONObject segmentJson = new JSONObject();
               List<Element> elements = segment.getElements();
               for (int index = 0; index < elements.size(); index++) {
                    segmentJson.put(segment.getId() + (index + 1), elements.get(index));
               }
               loopJson.put(segment.getId(), segmentJson);
          }

          // Map to hold child loops categorized by their IDs
          Map<String, JSONArray> childLoopsMap = new HashMap<>();

          for (Loop childLoop : loop.getLoops()) {
               String childLoopId = childLoop.getId();
               JSONObject childLoopJson = processLoop(childLoop);

               // If the key already exists, append the loop to the array
               if (childLoopsMap.containsKey(childLoopId)) {
                    childLoopsMap.get(childLoopId).put(childLoopJson);
               } else
               {
                    // Create a new array if the key doesn't exist
                    JSONArray childArray = new JSONArray();
                    childArray.put(childLoopJson);
                    childLoopsMap.put(childLoopId, childArray);
               }
          }

          // Add arrays of loops to the main JSON object
          for (Map.Entry<String, JSONArray> entry : childLoopsMap.entrySet()) {
               loopJson.put(entry.getKey(), entry.getValue());
          }

          return loopJson;
     }
}