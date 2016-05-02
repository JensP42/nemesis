package io.quantumdb.nemesis.structure.oracle11;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import io.quantumdb.nemesis.structure.Column;
import io.quantumdb.nemesis.structure.ColumnDefinition;
import io.quantumdb.nemesis.structure.Constraint;
import io.quantumdb.nemesis.structure.ForeignKey;
import io.quantumdb.nemesis.structure.Index;
import io.quantumdb.nemesis.structure.QueryBuilder;
import io.quantumdb.nemesis.structure.Sequence;
import io.quantumdb.nemesis.structure.Table;
import io.quantumdb.nemesis.structure.Trigger;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ToString
@EqualsAndHashCode
public class Oracle11Table implements Table {

	private final Connection connection;
	private final Oracle11Database parent;
	private final String name;


	Oracle11Table(Connection connection, Oracle11Database parent, String name) {
		this.connection = connection;
		this.parent = parent;
		this.name = name;
	}


	@Override
	public String getName() {
		return this.name;
	}

	/**
	 * RENAME departments_new TO emp_departments
	 */
	@Override
	public void rename(String newName) throws SQLException {
		execute(String.format("RENAME %s TO %s", this.name, newName));
	}

	@Override
	public Oracle11Database getParent() {
		return this.parent;
	}


	/**
	 * select cols.column_name, cols.data_type, cols.data_default, cols.nullable, pks.pkcolumn
	 * from all_tab_cols cols left join (
	 *	  SELECT column_name pkcolumn FROM all_cons_columns WHERE constraint_name = (
	 *	    SELECT constraint_name FROM user_constraints
	 *	    WHERE UPPER(table_name) = UPPER('tabname') AND CONSTRAINT_TYPE = 'P')
	 *	) pks on (cols.column_name=pks.pkcolumn)
	 *	where cols.TABLE_NAME='tabname' and cols.owner='owner'
	 */
	@Override
	public List<Column> listColumns() throws SQLException {
		StringBuilder query = new StringBuilder("select cols.column_name, cols.data_type, cols.DATA_LENGTH, cols.data_default, cols.nullable, pks.pkcolumn ");
		query.append("from all_tab_cols cols left join (");
		query.append("  SELECT column_name pkcolumn FROM all_cons_columns WHERE constraint_name = (");
		query.append("    SELECT constraint_name FROM user_constraints ");
		query.append("    WHERE UPPER(table_name) = UPPER('" + this.name + "') AND CONSTRAINT_TYPE = 'P')");
		query.append("  ) pks on (cols.column_name=pks.pkcolumn)");
		query.append("where cols.TABLE_NAME='" + this.name + "' and upper(cols.owner) like '" + this.getParent().getCredentials().getUsername().toUpperCase() + "'");

		List<Column> columns = Lists.newArrayList();
		try (Statement statement = connection.createStatement()) {
			log.debug(query.toString());
			ResultSet resultSet = statement.executeQuery(query.toString());

			while (resultSet.next()) {
				String columnName = resultSet.getString("column_name");
				String expression = resultSet.getString("data_default");
				boolean nullable = "Y".equalsIgnoreCase(resultSet.getString("nullable"));
				String type = this.getOracleDataTypeFromResultSet(resultSet);
				boolean identity = this.name.equalsIgnoreCase(resultSet.getString("pkcolumn"));

				//Oracle does not support auto-increment columns....but we have created a trigger/sequence combination to simulate this feature...
				boolean autoIncrement = this.autoIncrementSequenceExistsForColumn(columnName);

				columns.add(new Oracle11Column(connection, this, columnName, expression, nullable, type, identity, autoIncrement));
			}
		}

		return columns;
	}


	private boolean autoIncrementSequenceExistsForColumn(String columnName) throws SQLException {
		try {
			this.parent.getAutoIncrementSequenceForColumn(this, columnName);
		} catch (NoSuchElementException e) {
			return false;
		}
		return true;
	}


	private String getOracleDataTypeFromResultSet(ResultSet resultSet) throws SQLException {
		String type = resultSet.getString("data_type");
		if ("VARCHAR2".equals(type)) {
			type = String.format("VARCHAR2(%s)", resultSet.getInt("DATA_LENGTH"));
		}

		return type;
	}


	/**
	 * ALTER TABLE tabname ADD colname <colspec>
	 */
	@Override
	public Column addColumn(ColumnDefinition column) throws SQLException {
		QueryBuilder queryBuilder = new QueryBuilder();
		queryBuilder.append("ALTER TABLE " + name);
		queryBuilder.append(" ADD " + column.getName() + " " + this.getMappedOracleDataType(column.getType()));

		if (!Strings.isNullOrEmpty(column.getDefaultExpression())) {
			queryBuilder.append(" DEFAULT " + column.getDefaultExpression());
		}

		if (!column.isNullable()) {
			queryBuilder.append(" NOT NULL");
		}

		execute(queryBuilder.toString());


		if (column.isAutoIncrement()) {
			List<ColumnDefinition> l = new ArrayList<>();
			l.add(column);
			this.parent.handleAutoIncrementColumns(this.getName(), l);
		}

		Oracle11Column created = new Oracle11Column(this.connection, this, column);
		if (column.isIdentity()) {
			created.setIdentity(true);
		}

		return created;
	}

	/**
	 *  SELECT idx.index_name, idx.uniqueness, pks.pkconstraint
	 *	FROM ALL_INDEXES idx left join(
	 *	  SELECT constraint_name pkconstraint FROM all_cons_columns WHERE constraint_name = (
	 *	    SELECT constraint_name FROM user_constraints
	 *	    WHERE UPPER(table_name) = UPPER('tabname') AND CONSTRAINT_TYPE = 'P')
	 *	) pks  on (idx.index_name=pks.pkconstraint)
	 *	where idx.TABLE_NAME='tabname' and idx.owner='owner'
	 */
	@Override
	public List<Index> listIndices() throws SQLException {
		StringBuilder query = new StringBuilder("SELECT idx.index_name, idx.uniqueness, pks.pkconstraint, idx.visibility ");
		query.append("FROM ALL_INDEXES idx left join(");
		query.append("  SELECT constraint_name pkconstraint FROM all_cons_columns WHERE constraint_name = (");
		query.append("    SELECT constraint_name FROM user_constraints ");
		query.append("    WHERE UPPER(table_name) = UPPER('" + this.name + "') AND CONSTRAINT_TYPE = 'P')");
		query.append("  ) pks  on (idx.index_name=pks.pkconstraint)");
		query.append("where idx.TABLE_NAME='" + this.name + "' and upper(idx.owner) like '" + this.getParent().getCredentials().getUsername().toUpperCase() + "'");

		List<Index> indices = Lists.newArrayList();
		try (Statement statement = connection.createStatement()) {

			log.debug(query.toString());
			ResultSet resultSet = statement.executeQuery(query.toString());

			while (resultSet.next()) {
				String indexName = resultSet.getString("index_name");
				boolean isUnique = "unique".equalsIgnoreCase(resultSet.getString("uniqueness"));
				boolean isPrimary = this.name.equalsIgnoreCase(resultSet.getString("pkconstraint"));
				boolean isInvisible = !"VISIBLE".equalsIgnoreCase(resultSet.getString("visibility"));

				indices.add(new Oracle11Index(this, indexName, isUnique, isPrimary, isInvisible));
			}
		}

		return indices;
	}


	private Index createIndex(String name, boolean unique, boolean invisible, boolean online, String... columnNames) throws SQLException {
		String columns = Joiner.on(',').join(columnNames);
		String invisibleOption = invisible ? "INVISIBLE" : "";
		String onlineOption = online ? "ONLINE" : "";
		if (unique) {
			execute(String.format("CREATE UNIQUE INDEX %s ON %s (%s) %s %s", name, this.name, columns, invisibleOption, onlineOption));
		} else {
			execute(String.format("CREATE INDEX %s ON %s (%s) %s %s", name, this.name, columns, invisibleOption, onlineOption));
		}
		return new Oracle11Index(this, name, unique, false);
	}


	/**
	 * CREATE <UNIQUE> INDEX name ON tab(col1,...)
	 */
	@Override
	public Index createIndex(String name, boolean unique, String... columnNames) throws SQLException {
		return this.createIndex(name, unique, false, false, columnNames);
	}


	@Override
	public Index createInvisibleIndex(String name, boolean unique, String... columnNames) throws SQLException {
		return this.createIndex(name, unique, false, false, columnNames);
	}

	@Override
	public Index createOnlineIndex(String name, boolean unique, String... columnNames) throws SQLException {
		return this.createIndex(name, unique, false, true, columnNames);
	}

	/**
	 * select cons.constraint_name, cons.constraint_type, cols.column_name
	 * from all_constraints cons left join all_cons_columns cols on (cons.constraint_name=cols.constraint_name)
	 * where cons.TABLE_NAME='TAEE_EVENTQUEUE' and cons.owner='AIR_DEV_MASTER'
	 */
	@Override
	public List<Constraint> listConstraints() throws SQLException {
		String query = new QueryBuilder()
				.append("select cons.constraint_name, cons.constraint_type, cols.column_name ")
				.append("from all_constraints cons left join all_cons_columns cols on (cons.constraint_name=cols.constraint_name) ")
				.append("where cons.TABLE_NAME=? and upper(cons.owner)=?")
				.toString();

		List<Constraint> constraints = Lists.newArrayList();
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setString(1, this.name);
			statement.setString(2, this.getParent().getCredentials().getUsername().toUpperCase());

			ResultSet resultSet = statement.executeQuery();

			while (resultSet.next()) {
				String constraintName = resultSet.getString("constraint_name");
				String columnName = resultSet.getString("column_name");
				String constraintType = resultSet.getString("constraint_type");

				constraints.add(new Oracle11Constraint(this, constraintName, constraintType, columnName));
			}
		}
		return constraints;
	}

	@Override
	public Constraint createConstraint(String name, String type, String expression) throws SQLException {
		String query = String.format("ALTER TABLE %s ADD CONSTRAINT %s %s %s", this.name, name, type, expression);
		getParent().execute(query);
		return new Oracle11Constraint(this, name, type, expression);
	}

	/**
	 * select * from ALL_CONSTRAINTS where table_name='TAIRATTACHMENTBINARY' and constraint_type = 'R'
	 */
	@Override
	public List<ForeignKey> listForeignKeys() throws SQLException {
		String query = new QueryBuilder()
				.append("select * from ALL_CONSTRAINTS where table_name=? and constraint_type = 'R'")
				.toString();

		List<ForeignKey> foreignKeys = Lists.newArrayList();
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setString(1, this.name);
			ResultSet resultSet = statement.executeQuery();

			while (resultSet.next()) {
				String constraintName = resultSet.getString("constraint_name");
				foreignKeys.add(new Oracle11ForeignKey(this, constraintName));
			}
		}
		return foreignKeys;
	}

	/**
	 * select * from ALL_triggers where table_name='tabname'
	 */
	@Override
	public List<Trigger> listTriggers() throws SQLException {
		String query = "select * from ALL_triggers where table_name=?";

		List<Trigger> triggers = Lists.newArrayList();
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setString(1, name);
			ResultSet resultSet = statement.executeQuery();

			while (resultSet.next()) {
				String triggerName = resultSet.getString("trigger_name");
				triggers.add(new Oracle11Trigger(this, triggerName));
			}
		}

		return triggers;
	}

	/**
	 * alter table cust_table add constraint fk_cust_name FOREIGN KEY (person_name) references person_table (person_name)
	 */
	@Override
	public ForeignKey addForeignKey(String constraint, String[] columns, String referencedTable,
			String[] referencedColumns) throws SQLException {

		execute(String.format("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)", name, constraint,
				Joiner.on(',').join(columns), referencedTable, Joiner.on(',').join(referencedColumns)));

		return new Oracle11ForeignKey(this, constraint);
	}

	@Override
	public void drop() throws SQLException {

		//Since ORACLE does not support auto-increment columns we could have added some sequences for that
		//purpose (see Oracle11Database.createTable). These sequences have to be dropped as well.
		Iterator<Column> it = this.listColumns().stream()
			.filter(c -> c.isAutoIncrement())
			.iterator();

		while (it.hasNext()) {
			Sequence sequence = this.getParent().getAutoIncrementSequenceForColumn(this, it.next().getName());
			sequence.drop();
		}

		execute(String.format("DROP TABLE %s", this.name));
	}

	private void execute(String query) throws SQLException {
		getParent().execute(query);
	}


	/**
	 * Nemesis uses some data types which have to to be mapped to peroper Oracle types...
	 * @param dataType
	 * @return
	 */
	private String getMappedOracleDataType(String dataType) {
		if ("bigint".equals(dataType)) {
			return "INTEGER";
		}

		return dataType;
	}


}
