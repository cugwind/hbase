// Copyright, 2014 and onwards
//
// Author: cugwind

package org.apache.hadoop.hbase.tool;

import org.apache.hadoop.hbase.util.Bytes;

public class Constant {

    public static final String TABLE_NAME = "LogTable";
    public static final int MAX_VERSIONS = 1;
    public static final int NUM_REGIONS = 10;
    public static final byte[] FAMILY_INFO = Bytes.toBytes("info");
    public static final byte[] QUALIFIER_BROWSER = Bytes.toBytes("b");

}
