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
    private String parentStatement; // Class-level variable to store the parent statement


    public BuildSqlSelect(TransactionDefinition transactionDef) {
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
                System.out.println(segmentDef.getUsage().toString());
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
                System.out.println(childLoopDef.getUsage().toString());
                if (childLoopDef.getUsage().toString().equals("NOT_USED")) {continue;}
                String exploded_alias;

                if ("2300".equals(childLoopDef.getXid()) && "2000B".equals(loopDef.getXid())) {
//                        selectBuilder.append("1 AS 2300,\n");
                    // skip 2300 under 2000B since it will appear from 2000C as well and we will only see one of it
                    continue;

                } else if ("2300".equals(childLoopDef.getXid()) && "2000C".equals(loopDef.getXid())) {

                    exploded_alias = "exploded_prepared_" + childLoopDef.getXid().toLowerCase();

                    selectBuilder.append("case when exploded_isa_loop_gs_loop_st_loop_detail_2000a_2000b.2300_claim_information " +
                            "is not null then exploded_isa_loop_gs_loop_st_loop_detail_2000a_2000b.2300_claim_information " +
                                "else exploded_isa_loop_gs_loop_st_loop_detail_2000a_2000b_2000C.2300_claim_information end AS ")
                        .append(exploded_alias)
                        .append(",\n");

                    buildParentStatement(loopDef, childLoopDef, exploded_alias, aliasTrail);

                    continue;

                } else {
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
                }



                String childSelect = buildLoopSelect(childLoopDef, exploded_alias, aliasTrail);
                selectBuilder.append(childSelect);
            }
        }

        return selectBuilder.toString();
    }

    private void buildParentStatement(LoopDefinition loopDef, LoopDefinition childLoopDef, String parentAlias, String aliasTrail) {
        // Build the SQL for the child loop using buildLoopSelect

        String Select = "select *, explode_outer(exploded_prepared_2300) as exploded_2300, \n";
        Select = Select + buildLoopSelect(childLoopDef, "exploded_2300", "");

        this.parentStatement = Select;
    }

    public String getParentStatement() {
        return this.parentStatement;
    }

    public static void main(String[] args) {
        try {
            X12Reader reader837 = new X12Reader(X12Reader.FileType.ANSI837_5010_X222, new File("837sample"));

            TransactionDefinition transactionDef = reader837.getDefinition();
            BuildSqlSelect builder = new BuildSqlSelect(transactionDef);

            String childSqlSelect = builder.buildSelectStatement();

            childSqlSelect = "(select " + childSqlSelect + "parsed_json.Errors,\nparsed_json.FatalErrors from xjson) su";
//            System.out.println(sqlSelect);

            String sqlParentQuery = builder.getParentStatement();
            sqlParentQuery =  sqlParentQuery.endsWith(",\n") ? sqlParentQuery.substring(0, sqlParentQuery.length() - 2) + "\n" : sqlParentQuery;

//            System.out.println(sqlParentQuery);

            String selectSql = sqlParentQuery + " from \n" + childSqlSelect;
            System.out.println(selectSql);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}