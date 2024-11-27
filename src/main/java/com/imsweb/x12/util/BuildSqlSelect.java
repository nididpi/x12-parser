package com.imsweb.x12.util;

import com.imsweb.x12.mapping.ElementDefinition;
import com.imsweb.x12.mapping.LoopDefinition;
import com.imsweb.x12.mapping.SegmentDefinition;
import com.imsweb.x12.mapping.TransactionDefinition;
import com.imsweb.x12.reader.X12Reader;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class BuildSqlSelect {

    private TransactionDefinition transactionDef;

    public BuildSqlSelect(TransactionDefinition transactionDef) {
        this.transactionDef = transactionDef;
    }

    public String buildSelectStatement() {
        return buildLoopSelect(transactionDef.getLoop(), "parsed_json");
    }

    private String buildLoopSelect(LoopDefinition loopDef, String parentAlias) {
        if (loopDef == null) {
            return "";
        }

        StringBuilder selectBuilder = new StringBuilder();

//        selectBuilder.append("SELECT\n");

        // Directly append segment columns
        List<SegmentDefinition> segmentDefs = loopDef.getSegment();
        if (segmentDefs != null) {
            for (SegmentDefinition segmentDef : segmentDefs) {
                selectBuilder.append(parentAlias + "." + segmentDef.getXid() + "_" + segmentDef.getName().replace(' ', '_').replaceAll("[^a-zA-Z0-9_]", "").toLowerCase() + ",\n");
            }
        }

        List<LoopDefinition> childLoopDefs = loopDef.getLoop();
        if (childLoopDefs != null) {
            for (LoopDefinition childLoopDef : childLoopDefs) {

                String exploded_column_prefix = (parentAlias != null ? parentAlias : "");
                String exploded_column_name = exploded_column_prefix + "." + childLoopDef.getXid() + "_" + childLoopDef.getName().replace(' ', '_').replaceAll("[^a-zA-Z0-9_]", "").toLowerCase();
                String exploded_alias = exploded_column_prefix + "_" + loopDef.getXid().toLowerCase() + "_" + childLoopDef.getXid().toLowerCase();

                selectBuilder.append("EXPLODE_OUTER(")
//                        .append(parentAlias != null ? parentAlias + "." : "")
                        .append(exploded_column_name)
                        .append(") AS exploded_")
                        .append(exploded_alias)
                        .append(",\n");

                String childSelect = buildLoopSelect(childLoopDef, "exploded_" + exploded_alias);
                selectBuilder.append(childSelect); // Append recursively built child SELECT
            }
        }

        selectBuilder.append("\n");

        return selectBuilder.toString();
    }

    public static void main(String[] args) {
        try {
            X12Reader reader837 = new X12Reader(X12Reader.FileType.ANSI837_5010_X222, new File("837sample"));

            TransactionDefinition transactionDef = reader837.getDefinition();
            BuildSqlSelect builder = new BuildSqlSelect(transactionDef);

            String sqlSelect = builder.buildSelectStatement();
            System.out.println(sqlSelect);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}