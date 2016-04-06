package com.sakthiinfotec.backup;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

public class Utils {
	
	private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public static void closeHBaseAdmin(HBaseAdmin admin) {
		if(null != admin)
			try {
				admin.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
	}

	public static void closeHBaseTable(HTable htable) {
		if(null != htable)
			try {
				htable.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
	}

	public static String getISODate(long ts) {
		return DATE_FORMAT.format(new Date(ts * 1000));		
	}
	
}
