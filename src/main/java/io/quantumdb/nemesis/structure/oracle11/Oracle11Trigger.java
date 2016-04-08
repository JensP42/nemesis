package io.quantumdb.nemesis.structure.oracle11;

import java.sql.SQLException;

import io.quantumdb.nemesis.structure.Table;
import io.quantumdb.nemesis.structure.Trigger;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ToString
@EqualsAndHashCode
public class Oracle11Trigger implements Trigger {


	private final Oracle11Table parent;
	private final String name;

	Oracle11Trigger(Oracle11Table parent, String name) {
		this.parent = parent;
		this.name = name;
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
	public void drop() throws SQLException {
		execute(String.format("DROP TRIGGER %s", name));
	}

	private void execute(String query) throws SQLException {
		getParent().getParent().execute(query);
	}


}
