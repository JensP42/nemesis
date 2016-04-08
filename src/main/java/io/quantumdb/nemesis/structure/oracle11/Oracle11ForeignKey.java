package io.quantumdb.nemesis.structure.oracle11;

import java.sql.SQLException;

import io.quantumdb.nemesis.structure.ForeignKey;

public class Oracle11ForeignKey implements ForeignKey {


	private final Oracle11Table  parent;
	private final String name;

	Oracle11ForeignKey(Oracle11Table parent, String name) {
		this.parent = parent;
		this.name = name;
	}


	@Override
	public String getName() {
		return this.name;
	}

	@Override
	public void drop() throws SQLException {
		execute(String.format("ALTER TABLE %s DROP CONSTRAINT %s", parent.getName(), name));
	}

	private void execute(String query) throws SQLException {
		parent.getParent().execute(query);
	}

}
