package com.imsweb.x12.udf;

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;

class UdfTest {

    @Test
    void testUdf() {
        String resourceName = "837sample";

        try {
            InputStream inputStream = X12Parser.class.getClassLoader().getResourceAsStream(resourceName);
            if (inputStream == null) {
                System.err.println("Resource not found: " + resourceName);
                System.exit(1);
            }

            // Read from the InputStream into a String
            StringBuilder inputTextBuilder = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    inputTextBuilder.append(line);
                    inputTextBuilder.append(System.lineSeparator());
                }
            }

            String inputText = inputTextBuilder.toString();

            X12Parser spark_udf_new = new X12Parser();
            String resultNew = spark_udf_new.call(inputText);
            System.out.println(resultNew);
//
//            X12ParserOld spark_udf = new X12ParserOld();
//            String result = spark_udf.call(inputText);
//            System.out.println("Result: " + result);


//            assertEquals(result, resultNew);
        } catch (Exception e) {
            System.err.println("Error reading or processing file: " + e.toString());
        }


        ;
//        assertEquals("VALUE", element.getValue());
    }

}

