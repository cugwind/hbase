// Copyright, 2014 and onwards
//
// Author: cugwind

package org.apache.hadoop.hbase.tool;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class LogTable {

    private static final Log LOG = LogFactory.getLog(LogTable.class);

    public static void createTable() {
        HBaseAdmin admin = null;
        try {
            admin = new HBaseAdmin(HBaseConfiguration.create());
            if (!admin.tableExists(Constant.TABLE_NAME)) {
                HTableDescriptor tDesc = new HTableDescriptor(Constant.TABLE_NAME);

                HColumnDescriptor cDesc = new HColumnDescriptor(Constant.FAMILY_INFO);
                cDesc.setMaxVersions(Constant.MAX_VERSIONS);
                tDesc.addFamily(cDesc);

                long minIP = 0, maxIP = 4294967295L;
                long p = (maxIP - minIP) / Constant.NUM_REGIONS;
                byte[][] splitKeys = new byte[Constant.NUM_REGIONS - 1][];
                for (int i = 0; i < Constant.NUM_REGIONS - 1; i++) {
                    splitKeys[i] = Bytes.toBytes((i + 1) * p);
                }

                admin.createTable(tDesc, splitKeys);

                LOG.info("表" + Constant.TABLE_NAME + "创建成功!");
            } else {
                LOG.info("表" + Constant.TABLE_NAME + "已存在!");
            }
            admin.close();
        } catch (MasterNotRunningException e) {
            LOG.error(e);
        } catch (ZooKeeperConnectionException e) {
            LOG.error(e);
        } catch (IOException e) {
            LOG.error(e);
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    LOG.error(e);
                }
            }
        }
    }

    public static void main(String[] args) {
        LogTable.createTable();
    }
}
