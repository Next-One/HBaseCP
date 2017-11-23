package com.wang.utils;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author root 用于hbase操作的工具类
 */
public class HBaseUtil {
	private final static Logger LOG = LoggerFactory.getLogger(HBaseUtil.class);

	public static Properties createProps(String path) {
		Properties props = new Properties();
		if(!path.contains(".pro"))
			path += ".properties";
        try {
			props.load(Thread.currentThread().getContextClassLoader()
					.getResourceAsStream(path));
		} catch (IOException e) {
			LOG.error(path+" is miss!",e);
		}
		return props;
	}
	
	/**
	 * 让一个线程睡眠一段时间
	 * @param time
	 */
	public static void sleep(long time) {
		try {
			Thread.sleep(time);
		} catch (InterruptedException e) {
			LOG.error(Thread.currentThread().getName(), e);
		}
	}


	public static Put getPut(String row, String family, String qualifier, byte[] value) {
		Put put = new Put(Bytes.toBytes(row));
		put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), value);
		return put;
	}

	public static Put getPut(String row, String family, String qualifier, long ts, byte[] value) {
		Put put = new Put(Bytes.toBytes(row));
		put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), ts, value);
		return put;
	}

	public static Put getPut(byte[] row, String family, String qualifier, long ts, long value) {
		Put put = new Put(row);
		put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), ts, Bytes.toBytes(value));
		return put;
	}
	public static Put getPut(byte[] row, String family, String qualifier, long ts, int value) {
		Put put = new Put(row);
		put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), ts, Bytes.toBytes(value));
		return put;
	}
	
	public static Put getPut(byte[] row, String family, String qualifier, long ts, double value) {
		Put put = new Put(row);
		put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), ts, Bytes.toBytes(value));
		return put;
	}
	
	public static Put getPut(byte[] row, String family, String qualifier, long ts, byte[] value) {
		Put put = new Put(row);
		put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), ts, value);
		return put;
	}

	public static Put getPut(byte[] row, String family, String qualifier, long ts, String value) {
		return getPut(row, family, qualifier, ts, value.getBytes());
	}

	public static Put getPut(String row, String family, String qualifier, long ts, String value) {
		Put put = new Put(Bytes.toBytes(row));
		put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), ts, Bytes.toBytes(value));
		return put;
	}

	public static Put getPut(String row, String family, String qualifier, long ts, int value) {
		Put put = new Put(Bytes.toBytes(row));
		put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), ts, Bytes.toBytes(value));
		return put;
	}

	public static Put getPut(String row, String family, String qualifier, long value) {
		Put put = new Put(Bytes.toBytes(row));
		put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
		return put;
	}

	public static Put getPut(String row, String family, String qualifier, String value) {
		Put put = new Put(Bytes.toBytes(row));
		put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
		return put;
	}

	public static Put getPut(byte[] row, String family, String qualifier, String value) {
		Put put = new Put(row);
		put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
		return put;
	}
	public static Get getGet(String row) {
		Get get = new Get(Bytes.toBytes(row));
		return get;
	}

	public static Get getGet(String row, String family) {
		Get get = new Get(Bytes.toBytes(row));
		get.addFamily(Bytes.toBytes(family));
		return get;
	}

	public static Get getGet(String row, String family, String qualifier) {
		Get get = new Get(Bytes.toBytes(row));
		get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		return get;
	}

	public static Scan getScan(String family, String qualifier) {
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		return scan;
	}



}
