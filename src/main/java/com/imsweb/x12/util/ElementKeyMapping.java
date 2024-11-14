package com.imsweb.x12.util;

import com.imsweb.x12.mapping.ElementDefinition;
import com.imsweb.x12.mapping.LoopDefinition;
import com.imsweb.x12.mapping.SegmentDefinition;
import com.imsweb.x12.mapping.TransactionDefinition;
import com.imsweb.x12.reader.X12Reader;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElementKeyMapping {
    private TransactionDefinition transactionDef; // Replace with actual type
    private Map<String, String> definitionMap;

    public ElementKeyMapping(TransactionDefinition transactionDef) {
        this.transactionDef = transactionDef;
        this.definitionMap = new HashMap<>();
    }

    public void flattenDefinitions() {
        flattenDefinitions(transactionDef.getLoop());
    }

    private void flattenDefinitions(LoopDefinition loopDef) {
        if (loopDef == null) {
            return;
        }

        definitionMap.put(loopDef.getXid(), loopDef.getName());

        List<SegmentDefinition> segmentDefs = loopDef.getSegment();
        if (segmentDefs != null) {
            for (SegmentDefinition segmentDef : segmentDefs) {
                definitionMap.put(segmentDef.getXid(), segmentDef.getName());

                List<ElementDefinition> elementDefs = segmentDef.getElements();
                if (elementDefs != null) {
                    for (ElementDefinition elementDef : elementDefs) {
                        definitionMap.put(elementDef.getXid(), elementDef.getName());
                    }
                }
            }
        }

        List<LoopDefinition> childLoopDefs = loopDef.getLoop();
        if (childLoopDefs != null) {
            for (LoopDefinition childLoopDef : childLoopDefs) {
                try {
                    flattenDefinitions(childLoopDef);
                } catch (Exception e) {
                    System.out.println(childLoopDef.getXid());
                    System.out.println(e);
                    e.printStackTrace();
                }
            }
        }
    }

    public void saveDefinitionMapToFile(String filePath) throws IOException {

        System.out.println(definitionMap);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            for (Map.Entry<String, String> entry : definitionMap.entrySet()) {
                writer.write(entry.getKey() + "=" + entry.getValue());
                writer.newLine();
            }
        }
    }

    public Map<String, String> getDefinitionMap() {
        return definitionMap;
    }

    public static void main(String[] args) {
        try {

            X12Reader reader837 = new X12Reader(X12Reader.FileType.ANSI837_5010_X222, new File("/Users/weiwu/Documents/git/hl7test/src/main/java/com/example/837sample"));

            // Assuming you have a valid TransactionDefinition object
            TransactionDefinition transactionDef = reader837.getDefinition();
            ElementKeyMapping flattener = new ElementKeyMapping(transactionDef);

            // Flatten definitions
            flattener.flattenDefinitions();


            // Save to file
            String filePath = "837definitions.txt";
            flattener.saveDefinitionMapToFile(filePath);

            System.out.println("Definitions saved to " + filePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
