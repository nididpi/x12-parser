package com.imsweb.x12.udf;

import com.imsweb.x12.Element;
import com.imsweb.x12.Loop;
import com.imsweb.x12.Segment;
import com.imsweb.x12.reader.X12Reader.FileType;
import com.imsweb.x12.reader.X12Reader;


import org.json.JSONArray;
import org.json.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

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
        }
        catch (Exception e) {
            return e.toString();
        }
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

        for (Loop childLoop : loop.getLoops()) {
            loopJson.put(childLoop.getId(), processLoop(childLoop));
        }

        return loopJson;
    }

}
