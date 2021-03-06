package io.quantumdb.nemesis.structure;

import java.sql.SQLException;
import java.util.List;

public interface Table {

	String getName();

	void rename(String newName) throws SQLException;

	Database getParent();

	List<Column> listColumns() throws SQLException;

	default Column getColumn(String name) throws SQLException {
		return listColumns().stream()
				.filter(i -> i.getName().equalsIgnoreCase(name))
				.findFirst()
				.orElseThrow(() -> new SQLException("Could not find column: " + name));
	}

	default boolean hasColumn(String name) throws SQLException {
		return listColumns().stream()
				.filter(i -> i.getName().equalsIgnoreCase(name))
				.findAny()
				.isPresent();
	}

	Column addColumn(ColumnDefinition column) throws SQLException;

	List<Index> listIndices() throws SQLException;

	default Index getIndex(String name) throws SQLException {
		return listIndices().stream()
				.filter(i -> i.getName().equalsIgnoreCase(name))
				.findFirst()
				.orElseThrow(() -> new SQLException("Could not find index: " + name));
	}

	default boolean hasIndex(String name) throws SQLException {
		return listIndices().stream()
				.filter(i -> i.getName().equalsIgnoreCase(name))
				.findAny()
				.isPresent();
	}

	Index createIndex(String name, boolean unique, String... columnNames) throws SQLException;

	Index createInvisibleIndex(String name, boolean unique, String... columnNames) throws SQLException;

	Index createOnlineIndex(String name, boolean unique, String... columnNames) throws SQLException;

	List<Constraint> listConstraints() throws SQLException;

	default Constraint getConstraint(String name) throws SQLException {
		return listConstraints().stream()
				.filter(i -> i.getName().equalsIgnoreCase(name))
				.findFirst()
				.orElseThrow(() -> new SQLException("Could not find constraint: " + name));
	}

	default boolean hasConstraint(String name) throws SQLException {
		return listConstraints().stream()
				.filter(i -> i.getName().equalsIgnoreCase(name))
				.findAny()
				.isPresent();
	}

	Constraint createConstraint(String name, String type, String expression) throws SQLException;

	List<ForeignKey> listForeignKeys() throws SQLException;

	List<Trigger> listTriggers() throws SQLException;

	default ForeignKey getForeignKey(String name) throws SQLException {
		return listForeignKeys().stream()
				.filter(fk -> fk.getName().equalsIgnoreCase(name))
				.findFirst()
				.orElseThrow(() -> new SQLException("Could not find foreign key: " + name));
	}

	default boolean hasForeignKey(String name) throws SQLException {
		return listForeignKeys().stream()
				.filter(fk -> fk.getName().equalsIgnoreCase(name))
				.findAny()
				.isPresent();
	}


	default ForeignKey addForeignKey(String constraint, String[] columns, String referencedTable, String[] referencedColumns)
			throws SQLException {
		return addForeignKey(constraint, columns, referencedTable, referencedColumns, true);
	}


	ForeignKey addForeignKey(String constraint, String[] columns, String referencedTable, String[] referencedColumns, boolean enabled)
			throws SQLException;

	void drop() throws SQLException;

}
