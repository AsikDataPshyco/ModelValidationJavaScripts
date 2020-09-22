package com.compare.simple;

import java.security.Timestamp;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

public class CompareTwoTableData {

	static Statement srcStatement = null;
	static Statement tgtStatement = null;

	static {
		srcStatement = connectToDB();
		tgtStatement = connectToDB();
	}

	public static void main(String[] args) throws SQLException {

		compareTwoTableColumn();
		getDataFromDB();

	}

	private static boolean getDataFromDB() throws SQLException {

		String srcTableQuery = getSrcQuery();
		String tgtTableQuery = getTgtQuery();

		ResultSet srcResultSet = srcStatement.executeQuery(srcTableQuery);
		ResultSet tgtResultSet = tgtStatement.executeQuery(tgtTableQuery);

		boolean result = true;
		boolean dataExists = false;

		try {

			ResultSetMetaData rsMd1 = srcResultSet.getMetaData();
			ResultSetMetaData rsMd2 = tgtResultSet.getMetaData();

			int numberOfColumns1 = rsMd1.getColumnCount();
			int numberOfColumns2 = rsMd2.getColumnCount();
			if (!(numberOfColumns1 == numberOfColumns2))
				return false;

			int rowCount = 1;

			while (srcResultSet.next() && tgtResultSet.next()) {
				dataExists = true;
				System.out.println("Row " + rowCount + ":  ");

				for (int columnCount = 1; columnCount <= numberOfColumns1; columnCount++) {

					String columnName = rsMd1.getColumnName(columnCount);
					int columnType = rsMd1.getColumnType(columnCount);

					// if (srcResultSet.getString(columnCount).toString() != null ||
					// tgtResultSet.getString(columnCount) != null)
					// {

					if (srcResultSet.getString(columnCount) != null && tgtResultSet.getString(columnCount) != null)

					{

						if (columnType == Types.CHAR || columnType == Types.VARCHAR || columnType == Types.LONGVARCHAR)

						{
							String columnValue1 = srcResultSet.getString(columnCount);
							String columnValue2 = tgtResultSet.getString(columnCount);
							if ((columnValue1.contains(columnValue2))) {
								result = false;
								System.out.println("apass - Source Column Name: " + columnName + " Column Value: " + " "
										+ columnValue1 + " Matched  " + " Column Name: " + columnName
										+ " Column Value: " + columnValue2);
							} 
							else 
							{
								System.out.println("afail - Source Column Name: " + columnName + " Column Value: "
										+ columnValue1 + " " + "Not Matched  " + " Column Name: " + columnName
										+ " Column Value: " + columnValue2);
							}

						}

						if  
							(columnType == Types.INTEGER || columnType == Types.BIGINT || columnType == Types.SMALLINT|| columnType == Types.NUMERIC) 
							{
							Long columnValue1 = srcResultSet.getLong(columnCount);
							Long columnValue2 = tgtResultSet.getLong(columnCount);
							if (!(columnValue1.equals(columnValue2)))
								result = false;
							System.out.println("bpass - Source Column Name: " + columnName + "  Target Column Value: "
									+ columnValue1 + " Matched  " + "Column Name: " + columnName + "  Column Value: "
									+ columnValue2);
							}
							else 
							{
								System.out.println("bfail - Source Column Name: " + columnName + " Column Value: "
										+ srcResultSet.getLong(columnCount)  + " " + "Not Matched  " + " Column Name: " + columnName
										+ " Column Value: " + tgtResultSet.getLong(columnCount));
							}
						
						
						 if (columnType == Types.DECIMAL || columnType == Types.DOUBLE || columnType == Types.FLOAT || columnType == Types.REAL) 
						 
						 {
							Long columnValue1 = srcResultSet.getLong(columnCount);
							Long columnValue2 = tgtResultSet.getLong(columnCount);
							if (!(columnValue1.equals(columnValue2)))
								result = false;

							System.out.println("cpass  - Column Name: " + columnName + "Column Value: " + columnValue1
						     + "Matched  " + "Column Name: " + columnName + "Column Value: " + columnValue2);
						 }
						 else 
						 {
							 System.out.println("cfail - Source Column Name: " + columnName + " Column Value: "
										+ srcResultSet.getLong(columnCount)  + " " + "Not Matched  " + " Column Name: " + columnName
										+ " Column Value: " + tgtResultSet.getLong(columnCount));
						 }

						if (columnType == Types.TIME || columnType == Types.TIMESTAMP || columnType == Types.DATE) {
							java.sql.Timestamp columnValue1 = srcResultSet.getTimestamp(columnCount);
							java.sql.Timestamp columnValue2 = tgtResultSet.getTimestamp(columnCount);
							if (!(columnValue1.equals(columnValue2)))
								result = false;
							System.out.println("dpass - Column Name: " + columnName + "Column Value: " + columnValue1
									+ "Matched  " + "Column Name: " + columnName + "Column Value: " + columnValue2);

						} else {
							System.out.println("dfail - Column Name: " + columnName + " Column Value: "
									+ srcResultSet.getString(columnCount) + "Not matched "
									+ tgtResultSet.getLong(columnCount));
						}
						

					}
					else {
						System.out.println("Null Check Source Value = " + srcResultSet.getString(columnCount) + " Target Value = "+ tgtResultSet.getString(columnCount) );
						
					}
					rowCount++;

				}
			}
		} catch (SQLException sqle) {
			System.out.println(sqle);
		} catch (Exception e) {
			System.out.println(e);

		}
		// System.out.println(result);
		return result; // && dataExists;
	}

	public static String getSrcQuery() {
		return "SELECT * FROM test_null_srce";
	}

	public static String getTgtQuery() {
		return "SELECT * FROM test_null_tgt";
	}

	public static void compareTwoTableColumn() throws SQLException {

		// get the src and tgt count from DB
		int src_cnt1 = getTableDataCount(getSrcQuery());
		int tgt_cnt = getTableDataCount(getTgtQuery());

		if (src_cnt1 == tgt_cnt) {
			System.out.println("Source to Target Count Matched");

		} else {

			List<String> sourceRowValues = placeOrderIdsIntoList(getSrcQuery());
			List<String> targetRowValues = placeOrderIdsIntoList(getTgtQuery());

			System.out.println("No fo Target Rows = " + targetRowValues.size());
			System.out.println("No fo Source Rows = " + sourceRowValues.size());

			if (targetRowValues.size() > sourceRowValues.size()) {
				toPrintOrderIds(sourceRowValues, targetRowValues, false);
			} else {

				toPrintOrderIds(targetRowValues, sourceRowValues, true);

			}

		}

	}

	private static List<String> placeOrderIdsIntoList(String query) throws SQLException {
		List<String> orderIds = new ArrayList<>();
		ResultSet rsOrderSrcId = srcStatement.executeQuery(query);
		while (rsOrderSrcId.next()) {

			orderIds.add(rsOrderSrcId.getString("ORDER_ID"));

		}
		return orderIds;
	}

	private static int getTableDataCount(String rowCountQuery) throws SQLException {
		int tableRowCount = 0;
		String executeRowQuery = "SELECT COUNT(*) AS TABLE_COUNT FROM (" + rowCountQuery + ")";
		ResultSet rsRowCount = srcStatement.executeQuery(executeRowQuery);
		while (rsRowCount.next()) {
			tableRowCount = rsRowCount.getInt("TABLE_COUNT");
		}
		return tableRowCount;
	}

	private static void toPrintOrderIds(List<String> rowValue1, List<String> rowValue2, boolean isTargetPrint) {
		for (int i = 0; i < rowValue2.size(); i++) {
			for (int j = 0; j < rowValue1.size(); j++) {
				if (rowValue1.get(j) != null && rowValue2.get(i) != null) {

					if (rowValue2.get(i).contains(rowValue1.get(j))) {
						System.out.println(rowValue1.get(j).toString() + " is equal to " + rowValue2.get(i).toString());
					} else {
						if (isTargetPrint) {
							System.out.println(rowValue2.get(i).toString()
									+ " This Order is missing in Source but loaded into target");
						} else {
							System.out.println(rowValue2.get(i).toString()
									+ " This Order is in Source but not loaded into target");
						}

					}

				}

			}
		}
	}

	private static Statement connectToDB() {
		Statement statement = null;
		try {

			Class.forName("oracle.jdbc.driver.OracleDriver");
			// create connection object
			Connection connection = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:orcl", "HR",
					"india123");
			// create a statement object
			statement = connection.createStatement();

		} catch (Exception e) {
			System.out.println("Exception thrown while connecting to DB");
			e.printStackTrace();

		}

		return statement;
	}

}
