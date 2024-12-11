package com.imsweb.x12.util;

import com.imsweb.x12.mapping.LoopDefinition;
import com.imsweb.x12.mapping.SegmentDefinition;
import com.imsweb.x12.mapping.TransactionDefinition;
import com.imsweb.x12.reader.X12Reader;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class Build835SqlSelect {

    private TransactionDefinition transactionDef;


    public Build835SqlSelect(TransactionDefinition transactionDef) {
        this.transactionDef = transactionDef;
    }

    public String buildSelectStatement() {
        return buildLoopSelect(transactionDef.getLoop(), "parsed_json", "");
    }

    private String buildLoopSelect(LoopDefinition loopDef, String parentAlias, String aliasTrail) {
        if (loopDef == null) {
            return "";
        }

        String currentAlias = loopDef.getXid().toLowerCase();
        if (!aliasTrail.contains(currentAlias)) {
            aliasTrail += "_" + currentAlias;
        }

        StringBuilder selectBuilder = new StringBuilder();

        // Directly append segment columns
        List<SegmentDefinition> segmentDefs = loopDef.getSegment();
        if (segmentDefs != null) {
            for (SegmentDefinition segmentDef : segmentDefs) {
//                System.out.println(segmentDef.getUsage().toString());
                String segmentBusinessName = segmentDef.getXid() + "_" + segmentDef.getName().replace(' ', '_').replaceAll("[^a-zA-Z0-9_]", "").toLowerCase();
                String columnName = parentAlias + "." + segmentBusinessName;
//                String columnAlias = "segment_" + columnName.replace(".", "_");
                String columnAlias = loopDef.getXid() + "_" + segmentBusinessName;
                selectBuilder.append(columnName + " AS " + columnAlias).append(",\n");
            }
        }

        List<LoopDefinition> childLoopDefs = loopDef.getLoop();
        if (childLoopDefs != null) {
            for (LoopDefinition childLoopDef : childLoopDefs) {
//                System.out.println(childLoopDef.getUsage().toString());
                if (childLoopDef.getUsage().toString().equals("NOT_USED")) {continue;}
                String exploded_alias;


                String exploded_column_name = parentAlias + "." + childLoopDef.getXid() + "_" + childLoopDef.getName().replace(' ', '_').replaceAll("[^a-zA-Z0-9_]", "").toLowerCase();
                exploded_alias = "exploded" + aliasTrail + "_" + childLoopDef.getXid().toLowerCase();

                if (!"1".equals(childLoopDef.getRepeat())) {
                    selectBuilder.append("EXPLODE_OUTER(")
                            .append(exploded_column_name)
                            .append(") AS ")
                            .append(exploded_alias)
                            .append(",\n");

                }
                else {
                    selectBuilder
                            .append(exploded_column_name)
                            .append(" AS ")
                            .append(exploded_alias)
                            .append(",\n");
                }


                String childSelect = buildLoopSelect(childLoopDef, exploded_alias, aliasTrail);
                selectBuilder.append(childSelect);
            }
        }

        return selectBuilder.toString();
    }

    public static void main(String[] args) {
        try {
            X12Reader reader = new X12Reader(X12Reader.FileType.ANSI834_5010_X220, new File("834sample"));

            TransactionDefinition transactionDef = reader.getDefinition();
            Build835SqlSelect builder = new Build835SqlSelect(transactionDef);

            String childSqlSelect = builder.buildSelectStatement();

            childSqlSelect = "select " + childSqlSelect + "parsed_json.Errors,\nparsed_json.FatalErrors from xjson";
//            System.out.println(sqlSelect);

//            String sqlParentQuery = builder.getParentStatement();
//            sqlParentQuery =  sqlParentQuery.endsWith(",\n") ? sqlParentQuery.substring(0, sqlParentQuery.length() - 2) + "\n" : sqlParentQuery;

//            System.out.println(sqlParentQuery);

            String selectSql = childSqlSelect;
            System.out.println(selectSql);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}