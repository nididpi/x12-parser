package com.imsweb.x12.crap;

import com.imsweb.x12.mapping.LoopDefinition;
import com.imsweb.x12.mapping.TransactionDefinition;
import com.imsweb.x12.reader.X12Reader;

import java.io.File;
import java.io.IOException;
import java.util.List;

import java.util.HashMap;
import java.util.Map;

public class CountDistinctXid {

    private TransactionDefinition transactionDef;

    public CountDistinctXid(TransactionDefinition transactionDef) {
        this.transactionDef = transactionDef;
    }

    public void printDistinctXidCounts() {
        Map<String, Integer> xidCountMap = new HashMap<>();
        countLoopXids(transactionDef.getLoop(), xidCountMap);

        // Printing the results
        for (Map.Entry<String, Integer> entry : xidCountMap.entrySet()) {
            System.out.println(entry.getKey() + ", " + entry.getValue());
        }
    }

    private void countLoopXids(LoopDefinition loopDef, Map<String, Integer> xidCountMap) {
        if (loopDef == null) {
            return;
        }

        // Count the current loop's Xid
        String xid = loopDef.getXid();
        xidCountMap.put(xid, xidCountMap.getOrDefault(xid, 0) + 1);

        List<LoopDefinition> childLoopDefs = loopDef.getLoop();
        if (childLoopDefs != null) {
            for (LoopDefinition childLoopDef : childLoopDefs) {
                countLoopXids(childLoopDef, xidCountMap); // Recursively count Xids in child loops
            }
        }
    }

    public static void main(String[] args) {
        try {
            X12Reader reader837 = new X12Reader(X12Reader.FileType.ANSI837_5010_X222, new File("837X222sample"));

            TransactionDefinition transactionDef = reader837.getDefinition();
            CountDistinctXid counter = new CountDistinctXid(transactionDef);

            counter.printDistinctXidCounts();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}