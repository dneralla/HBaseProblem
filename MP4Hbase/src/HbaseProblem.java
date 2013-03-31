import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

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

	public void addDataFromFile(String dataFileName, String tableName)
			throws Exception {
		// ToDo read file name and add data liek below
		// HbaseProblem.addRecord(tableName, "zkb", "background", "id", "5");
	}

	public static void main(String[] args) {
		try {
			if (args != null) {
				String tableName = "MetaHumans";
				String[] columnFamilys = { "background", "powers" };
				HbaseProblem hProblem = new HbaseProblem();
				hProblem.createTable(tableName, columnFamilys);
				hProblem.addDataFromFile(args[0], tableName);
			} else
				System.out.println("Pass data file argument atleast");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
