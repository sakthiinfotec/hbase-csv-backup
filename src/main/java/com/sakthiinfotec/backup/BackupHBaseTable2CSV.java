package com.sakthiinfotec.backup;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.yaml.snakeyaml.Yaml;

import au.com.bytecode.opencsv.CSVWriter;

/**
 * This class take a backup of HBase table data of a given columns. It depends
 * on OpenCSV, Hadoop & HBase jars.
 */
public class BackupHBaseTable2CSV {

	private static final Logger LOGGER = Logger.getLogger(BackupHBaseTable2CSV.class);
	private static final int WRITE_BATCH_SIZE = 5000;
	private static final String ZK_HOST_KEY = "zk-host";
	private static final String ZK_PORT_KEY = "zk-port";
	private static final String BACKUP_TABLE_NAME_KEY = "backup-table-name";
	private static final String START_TS_KEY = "start-ts";
	private static final String END_TS_KEY = "end-ts";
	private static Configuration config;
	private static final byte[] CF = "DF".getBytes();
	private static final char CSV_FIELD_SEPARATOR = ',';
	private static Map<String, Map<String, String>> yamlMap;
	private static Map<String, String> columnTypeMap;
	private static Map<String, String> connectionSettings;
	private final String tableName;

	/**
	 * Constructor sets configuration options
	 * 
	 * @throws FileNotFoundException
	 */
	public BackupHBaseTable2CSV() throws FileNotFoundException {
		yamlMap = loadYamlConfig();
		columnTypeMap = yamlMap.get("column_type_map");
		connectionSettings = yamlMap.get("connection-settings");
		config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", connectionSettings.get(ZK_HOST_KEY));
		config.set("hbase.zookeeper.property.clientPort", String.valueOf(connectionSettings.get(ZK_PORT_KEY)));
		tableName = connectionSettings.get(BACKUP_TABLE_NAME_KEY);
	}

	private void backup(final String backupFileName) throws IOException {
		LOGGER.info("Creating backup file " + backupFileName);
		CSVWriter writer = new CSVWriter(new FileWriter(backupFileName), CSV_FIELD_SEPARATOR);
		final String columns[] = new String[columnTypeMap.keySet().size()];
		columnTypeMap.keySet().toArray(columns);
		byte[][] bColumns = new byte[columns.length][];
		int i = 0;
		for (String column : columns) {
			bColumns[i++] = Bytes.toBytes(column);
		}
		List<String[]> rowList = new ArrayList<String[]>();

		HConnection connection = HConnectionManager.createConnection(config);
		HTableInterface table = connection.getTable(tableName);
		Scan scan = new Scan();
		long startTs = Long.parseLong(String.valueOf(connectionSettings.get(START_TS_KEY)));
		long endTs = Long.parseLong(String.valueOf(connectionSettings.get(END_TS_KEY)));
		LOGGER.info("Loading rows between start time:" + startTs + ", end time:" + endTs);
		scan.setTimeRange(startTs, endTs);

		try {
			// Write CSV Headers
			Map<String, String> columnDescMap = yamlMap.get("column_desc_map");
			final String csvHeader[] = new String[columnDescMap.keySet().size()];
			columnDescMap.values().toArray(csvHeader);
			writer.writeNext(csvHeader);

			ResultScanner scanner = table.getScanner(scan);
			String[] row;
			int rowCount = 0;
			for (Result result : scanner) {
				i = 0;
				row = new String[columns.length];
				for (byte[] column : bColumns) {
					row[i++] = getColumnValue(result, column);
				}
				rowList.add(row);

				if (++rowCount % WRITE_BATCH_SIZE == 0) {
					LOGGER.info(rowCount + " rows backuped ...");
					writer.writeAll(rowList);
					rowList.clear();
				}
			}

			if (rowList.size() > 0)
				writer.writeAll(rowList);
			LOGGER.info("Totally " + rowCount + " rows backup completed ...");
		} catch (IOException e) {
			throw e;
		} finally {
			writer.close();
		}
	}

	private String getColumnValue(Result result, byte[] qualifier) {
		String column = Bytes.toString(qualifier);
		String type = columnTypeMap.get(column), res = "";
		byte[] value = result.getValue(CF, qualifier);
		switch (type) {
		case "long":
			res = String.valueOf((value == null) ? 0L
					: column.equals("logged_at") ? Utils.getISODate(Bytes.toLong(value)) : Bytes.toLong(value));
			break;
		case "float":
			res = String.valueOf((value == null) ? 0.0 : Bytes.toFloat(value));
			break;
		case "double":
			res = String.valueOf((value == null) ? 0.0 : Bytes.toDouble(value));
			break;
		default:
			res = (value == null) ? "" : Bytes.toString(value);
		}
		return res;
	}

	private Map<String, Map<String, String>> loadYamlConfig() {
		String fileName = "config.yml";
		Yaml yaml = new Yaml();
		InputStream input = null;
		boolean fileExists = true;
		LOGGER.info("Loading default config file " + fileName + " from classpath");
		input = BackupHBaseTable2CSV.class.getClassLoader().getResourceAsStream(fileName);
		if (input == null) {
			LOGGER.error("Unable to locate default config file " + fileName + " in classpath");
			LOGGER.info(
					fileName + " not found in classpath. Falling back to get file using config.file system property");
			fileName = System.getProperty("config.file");
			try {
				input = new FileInputStream(new File(fileName));
			} catch (FileNotFoundException e) {
				fileExists = false;
			}
		}

		if (!fileExists) {
			LOGGER.error("Unable to locate config file");
			System.exit(-1);
		}

		@SuppressWarnings("unchecked")
		Map<String, Map<String, String>> map = (Map<String, Map<String, String>>) yaml.load(input);
		return map;
	}

	private static String getBackupFileName() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmss");
		String date = sdf.format(Calendar.getInstance().getTime());
		return "backup_" + date + ".csv";
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		final String backupFileName = getBackupFileName();
		new BackupHBaseTable2CSV().backup(backupFileName);
		LOGGER.info("Backup completed and data written in " + backupFileName + " ...\n");
	}

}