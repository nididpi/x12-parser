package com.imsweb.x12.util;

import com.imsweb.x12.util.BuildJsonSchema;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

class SchemaTest {

    @Test
    void testUdf() {

        try {

            BuildJsonSchema schemaGen = new BuildJsonSchema("837_5010_X222");
            String resultNew = schemaGen.getJsonSchema();
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

