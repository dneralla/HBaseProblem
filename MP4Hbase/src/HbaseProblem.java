import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class HbaseProblem {

	private static Configuration conf = null;
	static {
		conf = HBaseConfiguration.create();
	}

	public void createTable(String tableName, String[] columnFamilys)
			throws Exception {
		@SuppressWarnings("resource")
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tableName)) {
			System.out.println("table already exists!");
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			for (int i = 0; i < columnFamilys.length; i++) {
				tableDesc.addFamily(new HColumnDescriptor(columnFamilys[i]));
			}
			admin.createTable(tableDesc);
			System.out.println("Table Created successFully" + tableName);
		}
	}

	public static void addRecord(String tableName, String rowKey,
			String family, String qualifier, String value) throws Exception {
		try {
			@SuppressWarnings("resource")
			HTable table = new HTable(conf, tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier),
					Bytes.toBytes(value));
			table.put(put);
			System.out.println("insert recored " + rowKey + " to table "
					+ tableName + " ok.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void addDataFromFile(String dataFileName, String tableName,
			String delimiter, String[] colFamilies) throws Exception {

		BufferedReader br = new BufferedReader(new FileReader(dataFileName));
		String data = null;
		String columns = br.readLine();
		String column[] = null;

		if (columns != null)
			column = columns.split(delimiter);

		int n = column == null ? 0 : column.length;
		Map<String, String> colColFamilyMapping = new HashMap<String, String>();
		for (int i = 0; i < n; i++)
			if (i <= 3)
				colColFamilyMapping.put(column[i], colFamilies[0]);
			else
				colColFamilyMapping.put(column[i], colFamilies[1]);

		int dataRowNumber = 1;

		while ((data = br.readLine()) != null) {
			{

				String rowKey = hash(dataRowNumber);
				String splitData[] = data.split(delimiter);

				for (int i = 0; i < n; i++)
					addRecord(tableName, rowKey,
							colColFamilyMapping.get(column[i]), column[i],
							splitData[i]);

				dataRowNumber++;

			}

		}

		br.close();

	}

	String hash(int dataRowNumber) {
		return "-" + dataRowNumber + "-";
	}

	public static void main(String[] args) {
		try {
			if (args != null) {
				String tableName = "MetaHumans";
				String[] columnFamilys = { "background", "powers" };
				HbaseProblem hProblem = new HbaseProblem();
				hProblem.createTable(tableName, columnFamilys);
				hProblem.addDataFromFile(args[0], tableName, " ", columnFamilys);
			} else
				System.out.println("Pass data file argument atleast");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
