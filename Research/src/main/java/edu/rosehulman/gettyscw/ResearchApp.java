package edu.rosehulman.gettyscw;

import java.awt.BorderLayout;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.event.TableModelEvent;
import javax.swing.event.TableModelListener;
import javax.swing.table.TableModel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ServiceException;

public class ResearchApp extends JFrame {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6159593264441322136L;
	Configuration config = HBaseConfiguration.create();

	public ResearchApp() throws IOException {
		super("Ngrams Client");
		setup();
		this.setSize(1000, 500);
		this.setDefaultCloseOperation(EXIT_ON_CLOSE);
		this.setVisible(true);
	}

	public static void main(String[] args) {
		ResearchApp app = null;
		try {
			app = new ResearchApp();
		} catch (IOException exception) {
			// TODO Auto-generated catch-block stub.
			exception.printStackTrace();
		}
		try {
			if (app != null && app.connection != null) {
				app.connection.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	JTabbedPane tabs;
	private Admin admin;
	private static String FILE_TABLE_NAME = "ss02";

	ArrayList<String> sheetNames = new ArrayList<>();
	private Table file;
	TableName fileTableName;
	Connection connection;

	private void setup() throws IOException {
		// this.config.set
		this.config.set("hbase.zookeeper.quorum",
				"hadoop-52.csse.rose-hulman.edu,hadoop-12.csse.rose-hulman.edu,hadoop-51.csse.rose-hulman.edu");
		this.config.set("hbase.zookeeper.property.clientPort", "2181");
		this.config.set("zookeeper.znode.parent", "/hbase-unsecure");
		this.config.set("hbase.master", "hadoop-12.csse.rose-hulman.edu");

		// Debugging, make sure configuration is valid
		try {
			HBaseAdmin.checkHBaseAvailable(this.config);
		} catch (ServiceException e) {
			e.printStackTrace();
			System.exit(1);
		}
		this.connection = ConnectionFactory.createConnection(this.config);

		this.admin = this.connection.getAdmin();
		this.fileTableName = TableName.valueOf(FILE_TABLE_NAME);
		this.file = this.connection.getTable(this.fileTableName);
		if (!this.admin.tableExists(this.fileTableName)) {
			System.out.println("Creating table");
			HTableDescriptor newTb = new HTableDescriptor(this.fileTableName);
			HColumnDescriptor newCF = new HColumnDescriptor("Default");
			newCF.setCompactionCompressionType(Algorithm.NONE);
			newCF.setMaxVersions(HConstants.ALL_VERSIONS);
			newTb.addFamily(newCF);
			this.admin.createTable(newTb);
		} else {
			System.out.println("Table already exists");
		}
		if (this.admin.isTableDisabled(this.fileTableName)) {
			this.admin.enableTable(this.fileTableName);
		}
		this.tabs = new JTabbedPane();
		this.setContentPane(this.tabs);

		JPanel main = new MainPanel(this);
		this.tabs.add("Admin", main);

		// Initialize existing sheets

		HColumnDescriptor[] sheets = this.file.getTableDescriptor().getColumnFamilies();
		for (HColumnDescriptor sheet : sheets) {
			String name = sheet.getNameAsString();
			this.sheetNames.add(name);
			JPanel temp = new SpreadsheetPanel( name);
			this.tabs.add(name, temp);
		}

	}

	class MainPanel extends JPanel implements ActionListener {
		/**
		 * 
		 */
		private static final long serialVersionUID = 8336457724116412492L;
		JTextField newTableName;
		JButton makeNewTable;
		JButton export;
		ResearchApp parent;

		MainPanel(ResearchApp parent) {
			super();
			this.parent = parent;
			this.setLayout(new GridLayout(0, 1));

			this.newTableName = new JTextField("New Table Name");
			this.add(this.newTableName);

			this.makeNewTable = new JButton("Make New Table");
			this.add(this.makeNewTable);
			this.makeNewTable.addActionListener(this);

			this.export = new JButton("Export to CSV");
			this.add(this.export);
			this.export.addActionListener(this);
			this.revalidate();

		}

		@Override
		public void actionPerformed(ActionEvent e) {
			JButton jb = (JButton) e.getSource();
			System.out.println("Test");
			if (jb == this.makeNewTable) {
				System.out.println("TestA");
				makeSheet(this.newTableName.getText());
				return;
			}
			if (jb == this.export) {

				System.out.println("TestB");
				export();
				return;

			}
		}

		void makeSheet(String text) {

			try {

				Connection threadConnection = ConnectionFactory.createConnection(ResearchApp.this.config);
				Admin threadAdmin = threadConnection.getAdmin();
				if (!ResearchApp.this.sheetNames.contains(text)) {

					if (text.replaceAll("\\s", "") != text) {
						System.out.println("No whitespace allowed in table names");
						return;
					}
					ResearchApp.this.sheetNames.add(text);

					threadAdmin.disableTable(ResearchApp.this.fileTableName);
					HColumnDescriptor newCF = new HColumnDescriptor(text);
					newCF.setCompactionCompressionType(Algorithm.NONE);
					newCF.setMaxVersions(HConstants.ALL_VERSIONS);
					HTableDescriptor modified = new HTableDescriptor(threadConnection.getTable(ResearchApp.this.fileTableName).getTableDescriptor());
							modified.addFamily(newCF);

					System.out.println("here?");
					threadAdmin.modifyTable(ResearchApp.this.fileTableName, modified);
					threadAdmin.enableTable(ResearchApp.this.fileTableName);

					JPanel tab = new SpreadsheetPanel( text);
					ResearchApp.this.tabs.add(text, tab);
					ResearchApp.this.revalidate();
					threadAdmin.close();

				} else {
					System.out.println("Sheet already exists!");
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return;
		}
	}

	void export() {
		// TODO Auto-generated method stub.

	}

	class SpreadsheetPanel extends JPanel implements TableModelListener {
		/**
		 * 
		 */
		private static final long serialVersionUID = -8226364841373531296L;
		JTable t;
		String sheetName;

		SpreadsheetPanel(String sheetName) throws IOException {
			super();
			this.sheetName = sheetName;

			this.setLayout(new BorderLayout());
			String[] columnNames = { "A", "B", "C", "D", "E", "F" };
			
			Connection threadConnection = ConnectionFactory.createConnection(ResearchApp.this.config);
			Object[][] values = new Object[200][6];
			long[][] timestamps = new long[200][6];
			for (Object[] row : values) {
				Arrays.fill(row, null);
			}

			for (long[] row : timestamps) {
				Arrays.fill(row, 0);
			}
			Scan sc = new Scan();
			sc.addFamily(Bytes.toBytes(sheetName));
			sc.setMaxVersions(1);

			ResultScanner rs =  threadConnection.getTable(ResearchApp.this.fileTableName).getScanner(sc);
		
			Iterator<Result> ir = rs.iterator();
			while (ir.hasNext()) {
				Result r = ir.next();
				while(r.advance()){
				Cell c= r.current();
				String val = Bytes.toString(CellUtil.cloneValue(c));
				int row = Bytes.toInt(CellUtil.cloneRow(c));
				String fam = Bytes.toString(CellUtil.cloneFamily(c));
				int col =Bytes.toString(CellUtil.cloneQualifier(c)).charAt(0)-65;
				long timestamp =c.getTimestamp();
				System.out.println(fam+" ["+row+","+col+"]:"+val+" time:"+timestamp);
				if(timestamps[row][col]<timestamp){
					values[row][col]=val;
				}
				}
			}
			threadConnection.close();
			rs.close();


			this.t = new JTable(values, columnNames);
			JScrollPane scrollPane = new JScrollPane(this.t);
			this.t.setFillsViewportHeight(true);
			this.t.getModel().addTableModelListener(this);

			this.add(scrollPane, BorderLayout.CENTER);

		}

		@Override
		public void tableChanged(TableModelEvent e) {
			System.out.println("Cell changed");
			try {
			Connection threadConnection = ConnectionFactory.createConnection(ResearchApp.this.config);
			Table t = threadConnection.getTable(ResearchApp.this.fileTableName);
			int row = e.getFirstRow();
			int column = e.getColumn();
			TableModel model = (TableModel) e.getSource();
			String columnName = model.getColumnName(column);
			String data = (String) model.getValueAt(row, column);
			System.out.println(data);
			Put putter = new Put(Bytes.toBytes(row));
				t.put(putter.addColumn(Bytes.toBytes(this.sheetName), Bytes.toBytes(columnName),
						Bytes.toBytes(data)));
				threadConnection.close();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}

	}

}
