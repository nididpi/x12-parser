package com.imsweb.x12.udf;

import com.imsweb.x12.Element;
import com.imsweb.x12.udf.X12Parser;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;

class UdfTest {

    @Test
    void testUdf() {
        File file = new File("837sample");

        if (!file.exists()) {
            System.err.println("The file does not exist: " + file.getAbsolutePath());
            System.exit(1);
        }
        try {
            // Read the content of the file into a String
            String inputText = new String(Files.readAllBytes(Paths.get(file.toURI())), StandardCharsets.UTF_8);
            X12Parser spark_udf = new X12Parser();
            String result = spark_udf.call(inputText);
            System.out.println("Result: " + result);
        } catch (Exception e) {
            System.err.println("Error reading or processing file: " + e.toString());
        }


//        assertEquals("ID", element.getId());
//        assertEquals("VALUE", element.getValue());
    }

}

