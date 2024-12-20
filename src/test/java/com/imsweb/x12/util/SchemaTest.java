package com.imsweb.x12.util;

import org.junit.jupiter.api.Test;

class SchemaTest {

    @Test
    void testUdf() {

        try {

            BuildX12JsonSchema schemaGen = new BuildX12JsonSchema("837_5010_X224");
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

