package io.quantumdb.nemesis.structure.oracle11;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;

import io.quantumdb.nemesis.structure.Column;
import io.quantumdb.nemesis.structure.ColumnDefinition;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ToString
@EqualsAndHashCode
public class Oracle11Column implements Column {


	private final Connection connection;
	private final Oracle11Table parent;

	private String name;
	private String defaultExpression;
	private boolean nullable;
	private String type;
	private boolean identity;
	private boolean autoIncrement;


	Oracle11Column(Connection connection, Oracle11Table parent, ColumnDefinition column) {
		this(connection, parent, column.getName(), column.getDefaultExpression(), column.isNullable(),
				column.getType(), column.isIdentity(), column.isAutoIncrement());
	}

	Oracle11Column(Connection connection, Oracle11Table parent, String name, String defaultExpression,
			boolean nullable, String dataType, boolean identityColumn, boolean autoIncrement) {

		this.connection = connection;
		this.parent = parent;
		this.name = name;
		this.defaultExpression = defaultExpression;
		this.nullable = nullable;
		this.type =  dataType;
		this.identity = identityColumn;
		this.autoIncrement = autoIncrement;
	}


	@Override
	public String getName() {
		return this.name;
	}

	/**
	 * alter table sales rename column order_date to date_of_order;
	 */
	@Override
	public void rename(String newName) throws SQLException {
		execute(String.format("ALTER TABLE %s RENAME COLUMN %s TO %s", parent.getName(), name, newName));
		this.name = newName;
	}

	@Override
	public Oracle11Table getParent() {
		return this.parent;
	}

	@Override
	public String getType() {
		return this.getType();
	}

	/**
	 *  alter table table_name modify (column_name varchar2(30) );
	 */
	@Override
	public void setType(String newType) throws SQLException {
		execute(String.format("ALTER TABLE %s MODIFY (%s %s)", parent.getName(), name, newType));
		this.type = newType;
	}

	@Override
	public boolean isNullable() {
		return this.nullable;
	}

	/**
	 * ALTER TABLE customer MODIFY ( cust_name varchar2(100) not null);
	 */
	@Override
	public void setNullable(boolean isNullable) throws SQLException {
		String action = isNullable ? "NULL" : "NOT NULL";

		//do this only if really required. Oracle don't like modifying columns to values these columns
		//already have... (ORA-01451, ORA-01442)
		if (this.nullableDefinitionHasChanged(isNullable)) {
			execute(String.format("ALTER TABLE %s MODIFY (%s %s %s)", parent.getName(), this.name, this.type, action));
			this.nullable = isNullable;
		} else {
			log.debug("Nullable definition for column " + this.name + " is already set to " + this.nullable + ". Ignoring...");
		}
	}

	private boolean nullableDefinitionHasChanged(boolean isNullable) {
		return !(this.nullable && isNullable) ||
				(!this.nullable && !isNullable);
	}

	@Override
	public String getDefaultExpression() {
		return this.defaultExpression;
	}


	@Override
	public void setDefaultExpression(String newExpression) throws SQLException {

		//we do only change default expression not the NULL constraint so we must not include this expression in the MODIFY statement
		//=>ORA-01442: column to be modified to NOT NULL is already NOT NULL
		if (Strings.isNullOrEmpty(newExpression)) {
			execute(String.format("ALTER TABLE %s MODIFY (%s %s)", parent.getName(), this.name, this.type));
		} else {
			execute(String.format("ALTER TABLE %s MODIFY (%s %s DEFAULT %s)", parent.getName(), this.name, this.type, newExpression));
		}
		this.defaultExpression = newExpression;
	}

	@Override
	public boolean isIdentity() {
		return this.identity;
	}

	@Override
	public void setIdentity(boolean isIdentityColumn) throws SQLException {
		List<String> identityColumns = getParent().listColumns().stream()
				.filter(c -> c.isIdentity())
				.map(c -> c.getName())
				.collect(Collectors.toList());

		if (identity) {
			identityColumns.add(name);
		}

		//at first we drop the pk by its name
		List<String> pkConstraints = this.getParent().listConstraints().stream()
				.filter(c -> c.getType().equalsIgnoreCase("P"))
				.map(c -> c.getName())
				.collect(Collectors.toList());

		if (pkConstraints.size() > 0) {
			execute(String.format("ALTER TABLE %s DROP CONSTRAINT %s", getParent().getName(), pkConstraints.get(0)));
		}

		execute(String.format("ALTER TABLE %s ADD CONSTRAINT pk_%s PRIMARY KEY(%s);", getParent().getName(), getParent().getName(),
				Joiner.on(',').join(identityColumns)));

		this.identity = isIdentityColumn;
	}

	@Override
	public boolean isAutoIncrement() {
		return this.autoIncrement;
	}

	@Override
	public void drop() throws SQLException {
		execute(String.format("ALTER TABLE %s DROP COLUMN %s", parent.getName(), name));
	}


	private void execute(String query) throws SQLException {
		getParent().getParent().execute(query);
	}


}
