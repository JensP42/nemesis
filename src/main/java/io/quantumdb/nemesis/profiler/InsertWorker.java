package io.quantumdb.nemesis.profiler;

import java.io.Writer;
import java.sql.SQLException;

import io.quantumdb.nemesis.structure.Database;
import io.quantumdb.nemesis.structure.DatabaseCredentials;


public class InsertWorker extends Worker {

	private static final String QUERY = "INSERT INTO %s (name) VALUES ('%s')";

	private final Database backend;
	private final String tableName;

	public InsertWorker(Database backend, DatabaseCredentials credentials, Writer writer,
			long startingTimestamp, String tableName) {

		super(backend, credentials, writer, startingTimestamp);
		this.backend = backend;
		this.tableName = tableName;
	}
	
	@Override
	void doAction() throws SQLException {
		backend.query(String.format(QUERY, tableName, RandomNameGenerator.generate()));
	}
	
}
