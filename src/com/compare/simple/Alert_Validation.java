package com.compare.simple;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class Alert_Validation {

	public static void main(String[] args) throws SQLException

	{
		alert_expected_actual();
		find_False_Negatives();
		find_False_Positives();
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

	public static String getRcmExpectedQuery() {
		return "SELECT  execution_id,trade_type,exchange_cd FROM RCM_ALERTS";
	}

	public static String getRcmActualQuery() {
		return "SELECT execution_id FROM  UDM_CDS_ORDERS\r\n" + "WHERE SIDE = 'B' \r\n"
				+ "AND exchange_code <> 'BSE'\r\n" + "AND order_status = 'OPEN'\r\n"
				+ "AND base_curr_print_amt > '1000000'";
	}

	public static void find_False_Positives() throws SQLException 
	{
		List<String> falsePosExeVal = getFalsePosExe(getRcmExpectedQuery());
		
		if (!falsePosExeVal.toString().contains("BSE"))
		{
			System.out.println("False Positive test = Pass: The Exchange BSE should not create an alert, if created it is False Positive");
			
		}
		else 
		{
			System.out.println("False Positive test = Failed : The Exchange BSE Triggered an Alert - Trades/Exchanges Created an alert "+ falsePosExeVal);
		}
	}

	public static void find_False_Negatives() throws SQLException {
		// Trade Type FI is an inclusion if a record in UDM exists SIDE = 'B' AND
		// exchange_code <> 'BSE' AND order_status = 'OPEN' AND base_curr_print_amt >
		// '1000000' AND trade_type = 'FI'
			
		List<String> falsenNegExeVal = getFalsePosExe(getRcmExpectedQuery());
		
		if (falsenNegExeVal.toString().contains("FI"))
		{
			System.out.println("False Negative test = Pass = The trade Type FI - Fixed Income triggred an alert");
			
		}
		else 
		{
			System.out.println("False Negative test = Failed =  The trade Type FI - Fixed Income is not triggred an alert | Only the following Trade Types Created an Alert  "+ falsenNegExeVal);
		}
		
	}
	
	private static List<String> getFalsePosExe(String query) throws SQLException {
		Statement rcmLogicExpected = null;
		rcmLogicExpected = connectToDB();
		List<String> falsPosExe = new ArrayList<>();
		ResultSet rsOrderSrcId = rcmLogicExpected.executeQuery(query);
		while (rsOrderSrcId.next()) {

			falsPosExe.add(rsOrderSrcId.getString("trade_type"));
			falsPosExe.add(rsOrderSrcId.getString("exchange_cd"));

		}
		return falsPosExe;
	}

	public static void verify_Exclusions() throws SQLException {

	}

	public static void verify_inclusions() throws SQLException {

	}

	public static void alert_expected_actual() throws SQLException {

		try {

			int rcmAlertActCnt = alertDataCount(getRcmExpectedQuery());
			int rcmAlertExpCnt = alertDataCount(getRcmActualQuery());

			if (rcmAlertActCnt == rcmAlertExpCnt) {
				System.out.println("Matched = No of Actual Alerts Triggered = " + rcmAlertActCnt
						+ "No of Expected Alerts = " + rcmAlertExpCnt);

			} else {
				List<String> expectedFOExe = placeOrderIdsIntoList(getRcmExpectedQuery());
				System.out.println("The Follwoing Execution should have created the alerts" + expectedFOExe);

				List<String> actualFOExe = placeOrderIdsIntoList(getRcmActualQuery());
				System.out.println("The Follwoing Executions triggered the alerts" + actualFOExe);
			}

		} catch (SQLException e) {

			e.printStackTrace();
		}

	}

	private static List<String> placeOrderIdsIntoList(String query) throws SQLException {

		Statement rcmLogicExpected = null;
		rcmLogicExpected = connectToDB();

		List<String> executionIds = new ArrayList<>();
		ResultSet rsexecutionIds = rcmLogicExpected.executeQuery(query);
		while (rsexecutionIds.next()) {

			executionIds.add(rsexecutionIds.getString("execution_id"));

		}
		return executionIds;
	}

	private static int alertDataCount(String rowCountQuery) throws SQLException {
		Statement rcmLogicExpected = null;
		rcmLogicExpected = connectToDB();

		int tableRowCount = 0;
		String executeRowQuery = "SELECT COUNT(*) AS TABLE_COUNT FROM (" + rowCountQuery + ")";
		ResultSet rsRowCount = rcmLogicExpected.executeQuery(executeRowQuery);
		while (rsRowCount.next()) {
			tableRowCount = rsRowCount.getInt("TABLE_COUNT");
		}
		return tableRowCount;
	}

}
