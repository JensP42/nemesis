package io.quantumdb.nemesis.structure;

import java.sql.SQLException;

public interface Constraint {

	String getName();

	Table getParent();

	String getType();

	void drop() throws SQLException;

	void enable(String option) throws SQLException;

	void validate() throws SQLException;


}
