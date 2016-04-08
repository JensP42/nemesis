package io.quantumdb.nemesis.structure.oracle11;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import io.quantumdb.nemesis.structure.ColumnDefinition;
import io.quantumdb.nemesis.structure.Database;
import io.quantumdb.nemesis.structure.DatabaseCredentials;
import io.quantumdb.nemesis.structure.QueryBuilder;
import io.quantumdb.nemesis.structure.Sequence;
import io.quantumdb.nemesis.structure.Table;
import io.quantumdb.nemesis.structure.TableDefinition;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ToString
@EqualsAndHashCode
public class Oracle11Database implements Database {


	private static final int ORACLE_DDL_LOCK_TIMEOUT_IN_SECONDS = 60;

	private Connection connection;
	private DatabaseCredentials credentials;


	@Override
	public void connect(DatabaseCredentials credentials) throws SQLException {
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			this.connection = DriverManager.getConnection(credentials.getUrl() + ":" + credentials.getDatabase(), credentials.getUsername(), credentials.getPassword());

			this.initialiseDBSession();

			this.credentials = credentials;
		}
		catch (ClassNotFoundException e) {
			throw new SQLException(e);
		}
	}


	private void initialiseDBSession() throws SQLException {
		//By default, a DDL fails if it is not possible to immediately acquire lock on the the requested object (ORA-00054).
		//via DDL_LOCK_TIMEOUT a default waiting period can be specified in seconds (https://docs.oracle.com/cd/B28359_01/server.111/b28320/initparams068.htm)
		this.execute("ALTER SESSION SET DDL_LOCK_TIMEOUT=" + ORACLE_DDL_LOCK_TIMEOUT_IN_SECONDS);
	}

	@Override
	public DatabaseCredentials getCredentials() {
		return this.credentials;
	}

	@Override
	public void close() throws SQLException {
		this.connection.close();
	}

	@Override
	public boolean supports(Feature feature) {
		switch (feature) {
			case MULTIPLE_AUTO_INCREMENT_COLUMNS:
			case RENAME_TABLE_IN_ONE_TX:  //ORACLE does not support DDL transactions, hence each DDL is always one single transaction.
			case MODIFY_DATATYPE:  //ORACLE does not support changing data type from char/varchar type to one of the binary types (blob, clob, text, etc.) ORA-22858
				return false;
			default:
				return true;
		}
	}

	@Override
	public Table createTable(TableDefinition table) throws SQLException {

		List<ColumnDefinition> autoIncrementColumns = new ArrayList<>();

		QueryBuilder queryBuilder = new QueryBuilder();
		queryBuilder.append("CREATE TABLE " + table.getName() + " (");

		boolean columnAdded = false;
		for (ColumnDefinition column : table.getColumns()) {
			if (columnAdded) {
				queryBuilder.append(", ");
			}

			queryBuilder.append(column.getName() + " " + this.getOracleDataType(column.getType()));

			if (column.isIdentity()) {
				queryBuilder.append(" PRIMARY KEY");
			}

			if (!Strings.isNullOrEmpty(column.getDefaultExpression())) {
				queryBuilder.append(" DEFAULT " + column.getDefaultExpression());
			}

			if (!column.isNullable()) {
				queryBuilder.append(" NOT NULL");
			}

			if (column.isAutoIncrement()) {
				//oracle does not support auto_increment. use workaround with trigger and sequence instead...
				autoIncrementColumns.add(column);
			}

			columnAdded = true;
		}

		queryBuilder.append(")");
		execute(queryBuilder.toString());

		this.handleAutoIncrementColumns(table.getName(), autoIncrementColumns);

		return new Oracle11Table(this.connection, this, table.getName());
	}


	/*
	 * Since Oracle does not support auto-increment columns we use appropriate trigger and sequence for that purpose.
	 * */
	public void handleAutoIncrementColumns(String tableName, List<ColumnDefinition> autoIncrementColumns) throws SQLException {

		if (!autoIncrementColumns.isEmpty()) {
			for (ColumnDefinition column : autoIncrementColumns) {
				//create a sequence
				this.createSequenceForAutoIncrementColumn(column, tableName);
				//create trigger
				this.createInsertTrigger(column, tableName);
			}
		}
	}


	private String getOracleDataType(String type) {

		if ("bigint".equals(type)) {
			return "int";
		} else {
			return type;
		}
	}


	private String createSequenceForAutoIncrementColumn(ColumnDefinition column, String tableName) throws SQLException {
		String sequenceName = this.getAutoIncrementSequenceName(column.getName(), tableName);
		this.execute("CREATE SEQUENCE " + sequenceName);
		return sequenceName;
	}

	private void createInsertTrigger(ColumnDefinition column, String tableName) throws SQLException {
		QueryBuilder query = new QueryBuilder();
		query.append("CREATE OR REPLACE TRIGGER " + this.getAutoIncrementTriggerName(column.getName(), tableName))
			.append(" BEFORE INSERT ON " + tableName)
			.append(" FOR EACH ROW")
			.append(" BEGIN")
			.append(" SELECT " + this.getAutoIncrementSequenceName(column.getName(), tableName) + ".NEXTVAL")
			.append(" INTO :new.id")
			.append(" FROM dual;")
			.append(" END;");

		this.execute(query.toString());
	}

	private String getAutoIncrementTriggerName(String columnName, String tableName) {
		return "trg_autoinc_" + tableName + "_" + columnName;
	}

	private String getAutoIncrementSequenceName(String columnName, String tableName) {
		return "seq_autoinc_" + tableName + "_" + columnName;
	}

	void execute(String query) throws SQLException {
		query(query);
		log.debug(query);
	}


	/**
	 * SELECT owner, table_name FROM dba_tables
	 */
	@Override
	public List<Table> listTables() throws SQLException {
		String query = "SELECT table_name FROM dba_tables WHERE UPPER(owner) like '" + this.credentials.getUsername().toUpperCase() + "'";
		List<Table> tables = Lists.newArrayList();
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			log.debug(query);

			ResultSet resultSet = statement.executeQuery();
			while (resultSet.next()) {
				String tableName = resultSet.getString(1);
				tables.add(new Oracle11Table(this.connection, this, tableName));
			}
		}

		return tables;
	}

	/**
	 * RENAME departments_new TO emp_departments;
	 * atomic rename is not supported since oracle oes not support DDL transactions
	 */
	@Override
	public void atomicTableRename(String replacingTableName, String currentTableName, String archivedTableName)
			throws SQLException {

		try {
			String query = "RENAME %s TO %s";
			connection.createStatement().execute(String.format(query, currentTableName, archivedTableName));
			connection.createStatement().execute(String.format(query, replacingTableName, currentTableName));
		} catch (SQLException e) {
			throw new RuntimeException("Atomic rename failed unexpectedly due to SQLException: " + e.getMessage(), e);
		}
	}

	/**
	 * select sequence_owner, sequence_name from dba_sequences;
	 */
	@Override
	public List<Sequence> listSequences() throws SQLException {
		List<Sequence> sequences = Lists.newArrayList();
		try (Statement statement = connection.createStatement()) {
			ResultSet resultSet = statement.executeQuery("select sequence_name from dba_sequences WHERE upper(sequence_owner) like '" + this.credentials.getUsername().toUpperCase() + "'");

			while (resultSet.next()) {
				String name = resultSet.getString(1);
				sequences.add(new Oracle11Sequence(this, name));
			}
		}
		return sequences;
	}

	@Override
	public void dropContents() throws SQLException {
		for (Table table : listTables()) {
			table.drop();
		}
		for (Sequence sequence : listSequences()) {
			sequence.drop();
		}
	}

	@Override
	public Database getSetupDelegate() {
		return this;
	}

	@Override
	public void query(String query) throws SQLException {
		try (Statement statement = connection.createStatement()) {
			statement.execute(query);
		}
		catch (SQLException e) {
			log.error(e.getMessage() + " - " + query, e);
			throw e;
		}
	}

	@Override
	public Connection getConnection() {
		return this.connection;
	}


	public Sequence getAutoIncrementSequenceForColumn(Oracle11Table table, String columnName) throws SQLException, NoSuchElementException {

		Optional<Sequence> sequence = this.listSequences().stream()
			.filter(i -> i.getName().equalsIgnoreCase(this.getAutoIncrementSequenceName(columnName, table.getName())))
			.findFirst();

		return sequence.get();
	}

}
