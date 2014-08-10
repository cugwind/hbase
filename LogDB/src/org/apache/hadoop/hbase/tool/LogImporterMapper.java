// Copyright, 2014 and onwards
//
// Author: cugwind

package org.apache.hadoop.hbase.tool;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Random;

public class LogImporterMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

    private static final Log LOG = LogFactory.getLog(LogImporterMapper.class);

    private static final SimpleDateFormat SDF = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss Z",
            Locale.ENGLISH);

    private Random mRandom; // Random实例

    public Put parserLine(String line) {
        String ipField, dateField, urlField, browserField;
        int left = 0, right = 0;

        right = line.indexOf(" ");
        ipField = line.substring(left, right);

        left = line.indexOf("[", right + 1);
        right = line.indexOf("]", left + 1);
        dateField = line.substring(left + 1, right);

        // 跳过第五项
        left = line.indexOf("\"", right + 1);
        left = line.indexOf("\"", left + 1);

        // 第八项可能是"-"
        left = line.indexOf("\"", left + 1);
        right = line.indexOf("\"", left + 1);
        urlField = line.substring(left + 1, right);
        if (urlField.equals("-")) {
            return null;
        }

        left = line.indexOf("\"", right + 1);
        right = line.indexOf("\"", left + 1);
        browserField = line.substring(left + 1, right);

        LOG.debug("ipField:" + ipField + " dateField:" + dateField + " urlField:" + urlField
                + " browserField:" + browserField);

        byte[] ipBytes = Bytes.toBytes(IPv4.toLong(ipField));
        byte[] urlBytes = Bytes.toBytes(urlField);
        byte[] browserBytes = Bytes.toBytes(browserField);

        long ts = 0;
        byte[] tsBytes = null;
        try {
            ts = SDF.parse(dateField).getTime();
            ts = Long.MAX_VALUE - ts; // 记录顺序按时间由新到旧
            tsBytes = Bytes.toBytes(ts);
        } catch (ParseException e) {
            LOG.error(e);
        }

        if (ipBytes == null || tsBytes == null || urlBytes == null) {
            return null;
        }

        int r = mRandom.nextInt();

        LOG.debug("ip:" + Bytes.toLong(ipBytes) + " ts:" + (Long.MAX_VALUE - ts) + " random:" + r);

        byte[] rowkey = Bytes.add(Bytes.add(ipBytes, tsBytes), Bytes.toBytes(r), urlBytes);
        Put put = new Put(rowkey);
        put.add(Constant.FAMILY_INFO, Constant.QUALIFIER_BROWSER, browserBytes);
        return put;
    }

    @Override
    protected void setup(Context context) {
        mRandom = new Random();
    }

    @Override
    public void map(LongWritable offset, Text value, Context context) throws IOException {

        String line = value.toString();
        if (line == null || line.length() < 1) {
            return;
        }

        Put put = parserLine(line);
        if (put == null) {
            return;
        }

        ImmutableBytesWritable key = new ImmutableBytesWritable(put.getRow());
        try {
            context.write(key, put);

            context.getCounter("LogImporterMapper", "NUM_RECORDS").increment(1);
        } catch (InterruptedException e) {
            LOG.error(e);
        }
    }

    public static void main(String[] args) {
        String FILENAME = "c:/log.txt";
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(FILENAME));
            String line;
            while ((line = reader.readLine()) != null) {
                LogImporterMapper map = new LogImporterMapper();
                map.setup(null);
                map.parserLine(line);
            }
        } catch (IOException e) {
            LOG.error(e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    LOG.error(e);
                }
            }
        }
    }

}
