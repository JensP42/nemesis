package io.quantumdb.nemesis.structure.oracle11;

import java.sql.SQLException;

import io.quantumdb.nemesis.structure.Constraint;
import io.quantumdb.nemesis.structure.Table;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ToString
@EqualsAndHashCode
public class Oracle11Constraint implements Constraint {


	private final Oracle11Table parent;
	private final String name;
	private final String type;
	private final String expression;

	Oracle11Constraint(Oracle11Table parent, String name, String type, String expression) {
		this.parent = parent;
		this.name = name;
		this.type = type;
		this.expression = expression;
	}


	@Override
	public String getName() {
		return this.name;
	}

	@Override
	public Oracle11Table getParent() {
		return this.parent;
	}

	@Override
	public String getType() {
		return this.type;
	}

	@Override
	public void drop() throws SQLException {
		execute(String.format("ALTER TABLE %s DROP CONSTRAINT %s", parent.getName(), name));
	}

	private void execute(String query) throws SQLException {
		getParent().getParent().execute(query);
	}

}
