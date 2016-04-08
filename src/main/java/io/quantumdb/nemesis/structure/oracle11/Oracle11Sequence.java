package io.quantumdb.nemesis.structure.oracle11;

import java.sql.SQLException;

import io.quantumdb.nemesis.structure.Sequence;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ToString
@EqualsAndHashCode
public class Oracle11Sequence implements Sequence {

	private final Oracle11Database parent;
	private final String name;


	Oracle11Sequence(Oracle11Database parent, String name) {
		this.parent = parent;
		this.name = name;
	}

	@Override
	public String getName() {
		return this.name;
	}

	@Override
	public Oracle11Database getParent() {
		return this.parent;
	}

	@Override
	public void drop() throws SQLException {
		execute(String.format("DROP SEQUENCE %s", name));
	}


	private void execute(String query) throws SQLException {
		getParent().execute(query);
	}

}
