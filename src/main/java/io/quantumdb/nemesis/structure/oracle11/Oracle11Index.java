package io.quantumdb.nemesis.structure.oracle11;

import java.sql.SQLException;

import io.quantumdb.nemesis.structure.Index;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ToString
@EqualsAndHashCode
public class Oracle11Index implements Index {

	private final Oracle11Table parent;
	private final String name;

	private final boolean unique;
	private final boolean primary;
	private final boolean invisible;


	Oracle11Index(Oracle11Table parent, String name, boolean unique, boolean primary) {
		this(parent, name, unique, primary, false);
	}

	Oracle11Index(Oracle11Table parent, String name, boolean unique, boolean primary, boolean invisible) {
		this.parent = parent;
		this.name = name;
		this.unique = unique;
		this.primary = primary;
		this.invisible = invisible;
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
	public boolean isUnique() {
		return this.unique;
	}

	@Override
	public boolean isPrimary() {
		return this.primary;
	}

	/**
	 * ALTER INDEX index_name RENAME TO new_name;
	 */
	@Override
	public void rename(String name) throws SQLException {
		execute(String.format("ALTER INDEX %s RENAME TO %s", this.name, name));
	}

	@Override
	public void drop() throws SQLException {
		execute(String.format("DROP INDEX %s", name));
	}


	private void execute(String query) throws SQLException {
		getParent().getParent().execute(query);
	}


}
