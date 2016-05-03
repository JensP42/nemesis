package io.quantumdb.nemesis.operations;

import java.sql.SQLException;
import java.util.List;

import com.google.common.collect.Lists;
import io.quantumdb.nemesis.structure.ColumnDefinition;
import io.quantumdb.nemesis.structure.Database;
import io.quantumdb.nemesis.structure.TableDefinition;


public class DefaultOperations {

	public List<NamedOperation> all() {
		return Lists.newArrayList(
				createIndexOnColumn(),
				createUniqueConstraintOnColumn(),
				createIndexOnNullableColumn(),
				createInvisibleIndexOnColumn(),
				createOnlineIndexOnColumn(),
				renameIndexOnColumn(),
				dropIndexOnColumn(),
				dropForeignKeyConstraint(),
				addNullableColumn(),
				addNonNullableColumn(),
				addVirtualColumn(),
				dropNullableColumn(),
				dropNonNullableColumn(),
				renameNullableColumn(),
				renameNonNullableColumn(),
				modifyDataTypeOnNullableColumn(),
				modifyDataTypeOnNonNullableColumn(),
				modifyDataTypeFromIntToText(),
				setDefaultExpressionOnNullableColumn(),
				setDefaultExpressionOnNonNullableColumn(),
				makeColumnNullable(),
				makeColumnNonNullable(),
				addNonNullableForeignKey(),
				addNullableForeignKey(),
				renameTable()
		);
	}

	private NamedOperation renameTable() {
		return new NamedOperation("rename-table", new Operation() {
			public void prepare(Database backend) throws SQLException {
				TableDefinition table = new TableDefinition("users_v2")
						.withColumn(new ColumnDefinition("id", "bigint")
								.setNullable(false)
								.setAutoIncrement(true)
								.setIdentity(true))
						.withColumn(new ColumnDefinition("name", "varchar(255)")
								.setNullable(false));

				backend.createTable(table);
			}

			public void perform(Database backend) throws SQLException {
				backend.atomicTableRename("users_v2", "users", "users_v1");
			}

			public void cleanup(Database backend) throws SQLException {
				backend.atomicTableRename("users_v1", "users", "users_v2");
			}

			@Override
			public boolean isSupportedBy(Database backend) {
				return backend.supports(Database.Feature.RENAME_TABLE_IN_ONE_TX);
			}
		});
	}

	public NamedOperation addNullableColumn() {
		return new NamedOperation("add-nullable-column", new Operation() {

			@Override
			public void perform(Database backend) throws SQLException {
				backend.getTable("users").addColumn(new ColumnDefinition("email", "varchar(255)"));
			}

			@Override
			public void cleanup(Database backend) throws SQLException {
				backend.getTable("users").getColumn("email").drop();
			}
		});
	}

	public NamedOperation addVirtualColumn() {
		return new NamedOperation("add-virtual-column", new Operation() {

			@Override
			public void perform(Database backend) throws SQLException {
				backend.getTable("users").addColumn(new ColumnDefinition("initials", "varchar(2 CHAR)")
						.setVirtualColumnExpression("SUBSTR(name, 1, 2)"));
			}

			@Override
			public void cleanup(Database backend) throws SQLException {
				backend.getTable("users").getColumn("initials").drop();
			}

			@Override
			public boolean isSupportedBy(Database backend) {
				return backend.supports(Database.Feature.VIRTUAL_COLUMN);
			}
		});
	}

	public NamedOperation addNonNullableColumn() {
		return new NamedOperation("add-non-nullable-column", new Operation() {

			@Override
			public void perform(Database backend) throws SQLException {
				backend.getTable("users").addColumn(new ColumnDefinition("life_story", "varchar(255)")
						.setDefaultExpression("'Simple story'")
						.setNullable(false));
			}

			@Override
			public void cleanup(Database backend) throws SQLException {
				backend.getTable("users").getColumn("life_story").drop();
			}
		});
	}

	public NamedOperation dropNullableColumn() {
		return new NamedOperation("drop-nullable-column", new Operation() {

			@Override
			public void prepare(Database backend) throws SQLException {
				backend.getTable("users").addColumn(new ColumnDefinition("email", "varchar(255)"));
			}

			@Override
			public void perform(Database backend) throws SQLException {
				backend.getTable("users").getColumn("email").drop();
			}
		});
	}

	public NamedOperation dropNonNullableColumn() {
		return new NamedOperation("drop-non-nullable-column", new Operation() {

			@Override
			public void prepare(Database backend) throws SQLException {
				backend.getTable("users").addColumn(new ColumnDefinition("email", "varchar(255)")
						.setDefaultExpression("\'NOT_SET\'")
						.setNullable(false));
			}

			@Override
			public void perform(Database backend) throws SQLException {
				backend.getTable("users").getColumn("email").drop();
			}
		});
	}

	public NamedOperation renameNullableColumn() {
		return new NamedOperation("rename-nullable-column", new Operation() {

			@Override
			public void prepare(Database backend) throws SQLException {
				backend.getTable("users").addColumn(new ColumnDefinition("email", "varchar(255)"));
			}

			@Override
			public void perform(Database backend) throws SQLException {
				backend.getTable("users").getColumn("email").rename("email2");
			}

			@Override
			public void cleanup(Database backend) throws SQLException {
				backend.getTable("users").getColumn("email2").drop();
			}
		});
	}

	public NamedOperation renameNonNullableColumn() {
		return new NamedOperation("rename-non-nullable-column", new Operation() {

			@Override
			public void prepare(Database backend) throws SQLException {
				backend.getTable("users").addColumn(new ColumnDefinition("email", "varchar(255)")
						.setDefaultExpression("\'NOT_SET\'")
						.setNullable(false));
			}

			@Override
			public void perform(Database backend) throws SQLException {
				backend.getTable("users").getColumn("email").rename("email2");
			}

			@Override
			public void cleanup(Database backend) throws SQLException {
				backend.getTable("users").getColumn("email2").drop();
			}
		});
	}

	public NamedOperation createIndexOnColumn() {
		return new NamedOperation("create-index-on-column", new Operation() {

			@Override
			public void perform(Database backend) throws SQLException {
				backend.getTable("users").createIndex("users_name_idx", false, "name");
			}

			@Override
			public void cleanup(Database backend) throws SQLException {
				backend.getTable("users").getIndex("users_name_idx").drop();
			}
		});
	}

	public NamedOperation createUniqueConstraintOnColumn() {
		return new NamedOperation("create-unique-constraint-on-column", new Operation() {


			@Override
			public void perform(Database backend) throws SQLException {
				backend.getTable("users").createConstraint("name_id_unique_constraint", "UNIQUE", "(id,name)");
			}

			@Override
			public void cleanup(Database backend) throws SQLException {
				backend.getTable("users").getConstraint("name_id_unique_constraint").drop();
			}
		});
	}

	public NamedOperation createInvisibleIndexOnColumn() {
		return new NamedOperation("create-index-invisible-on-column", new Operation() {

			@Override
			public void perform(Database backend) throws SQLException {
				backend.getTable("users").createInvisibleIndex("users_name_idx", false, "name");
			}

			@Override
			public void cleanup(Database backend) throws SQLException {
				backend.getTable("users").getIndex("users_name_idx").drop();
			}

			@Override
			public boolean isSupportedBy(Database backend) {
				return backend.supports(Database.Feature.INVISIBLE_INDEX);
			}

		});
	}

	public NamedOperation createOnlineIndexOnColumn() {
		return new NamedOperation("create-index-online-on-column", new Operation() {

			@Override
			public void perform(Database backend) throws SQLException {
				backend.getTable("users").createOnlineIndex("users_name_idx", false, "name");
			}

			@Override
			public void cleanup(Database backend) throws SQLException {
				backend.getTable("users").getIndex("users_name_idx").drop();
			}

			@Override
			public boolean isSupportedBy(Database backend) {
				return backend.supports(Database.Feature.ONLINE_INDEX);
			}

		});
	}


	public NamedOperation createIndexOnNullableColumn() {
		return new NamedOperation("create-index-on-nullable-column", new Operation() {

			@Override
			public void prepare(Database backend) throws SQLException {
				backend.getTable("users").addColumn(new ColumnDefinition("jobtitle", "varchar(255)"));
			}

			@Override
			public void perform(Database backend) throws SQLException {
				backend.getTable("users").createIndex("users_jobtitle_idx", false, "jobtitle");
			}

			@Override
			public void cleanup(Database backend) throws SQLException {
				backend.getTable("users").getIndex("users_jobtitle_idx").drop();
				backend.getTable("users").getColumn("jobtitle").drop();
			}
		});
	}


	public NamedOperation dropIndexOnColumn() {
		return new NamedOperation("drop-index-on-column", new Operation() {

			@Override
			public void prepare(Database backend) throws SQLException {
				backend.getTable("users").createIndex("users_name_idx", false, "name");
			}

			@Override
			public void perform(Database backend) throws SQLException {
				backend.getTable("users").getIndex("users_name_idx").drop();
			}
		});
	}

	public NamedOperation dropForeignKeyConstraint() {
		return new NamedOperation("drop-foreign-key-constraint", new Operation() {
			@Override
			public void prepare(Database backend) throws SQLException {
				TableDefinition table = new TableDefinition("addresses")
						.withColumn(new ColumnDefinition("id", "bigint")
								.setIdentity(true)
								.setAutoIncrement(true))
						.withColumn(new ColumnDefinition("address", "varchar(255)")
								.setDefaultExpression("''")
								.setNullable(false));

				backend.createTable(table);
				backend.query("INSERT INTO addresses (address) VALUES ('Unknown')");
				backend.getTable("users").addColumn(new ColumnDefinition("address_id", "bigint")
						.setDefaultExpression("'1'")
						.setNullable(false));

				backend.getTable("users").addForeignKey("users_address", new String[] { "address_id" },
						"addresses", new String[] { "id" });
			}

			@Override
			public void perform(Database backend) throws SQLException {
				backend.getTable("users").getForeignKey("users_address").drop();
			}

			@Override
			public void cleanup(Database backend) throws SQLException {
				backend.getTable("users").getColumn("address_id").drop();
				backend.getTable("addresses").drop();
			}

		});
	}

	public NamedOperation renameIndexOnColumn() {
		return new NamedOperation("rename-index", new Operation() {

			@Override
			public void prepare(Database backend) throws SQLException {
				backend.getTable("users").createIndex("users_name_idx", false, "name");
			}

			@Override
			public void perform(Database backend) throws SQLException {
				backend.getTable("users").getIndex("users_name_idx").rename("users_name2_idx");
			}

			@Override
			public void cleanup(Database backend) throws SQLException {
				backend.getTable("users").getIndex("users_name2_idx").drop();
			}

			@Override
			public boolean isSupportedBy(Database backend) {
				return backend.supports(Database.Feature.RENAME_INDEX);
			}
		});
	}

	public NamedOperation modifyDataTypeOnNullableColumn() {
		return new NamedOperation("modify-data-type-on-nullable-column", new Operation() {

			@Override
			public boolean isSupportedBy(Database backend) {
				return backend.supports(Database.Feature.MODIFY_DATATYPE);
			}

			@Override
			public void prepare(Database backend) throws SQLException {
				backend.getTable("users").addColumn(new ColumnDefinition("email", "varchar(255)"));
			}

			@Override
			public void perform(Database backend) throws SQLException {
				backend.getTable("users").getColumn("email").setType("text");
			}

			@Override
			public void cleanup(Database backend) throws SQLException {
				backend.getTable("users").getColumn("email").drop();
			}
		});
	}

	public NamedOperation modifyDataTypeOnNonNullableColumn() {
		return new NamedOperation("modify-data-type-on-non-nullable-column", new Operation() {

			@Override
			public void prepare(Database backend) throws SQLException {
				backend.getTable("users").addColumn(new ColumnDefinition("email", "varchar(255)")
						.setDefaultExpression("\'NOT_SET\'")
						.setNullable(false));
			}

			@Override
			public void perform(Database backend) throws SQLException {
				backend.getTable("users").getColumn("email").setType("text");
			}

			@Override
			public void cleanup(Database backend) throws SQLException {
				backend.getTable("users").getColumn("email").drop();
			}

			@Override
			public boolean isSupportedBy(Database backend) {
				return backend.supports(Database.Feature.MODIFY_DATATYPE) &&
						backend.supports(Database.Feature.DEFAULT_VALUE_FOR_TEXT);
			}
		});
	}

	public NamedOperation modifyDataTypeFromIntToText() {
		return new NamedOperation("modify-data-type-from-int-to-text", new Operation() {

			@Override
			public void prepare(Database backend) throws SQLException {
				backend.getTable("users").addColumn(new ColumnDefinition("cnt", "bigint")
						.setDefaultExpression("8")
						.setNullable(false));
			}

			@Override
			public void perform(Database backend) throws SQLException {
				backend.getTable("users").getColumn("cnt").setType("text");
			}

			@Override
			public void cleanup(Database backend) throws SQLException {
				backend.getTable("users").getColumn("cnt").drop();
			}

			@Override
			public boolean isSupportedBy(Database backend) {
				return backend.supports(Database.Feature.MODIFY_DATATYPE) &&
						backend.supports(Database.Feature.DEFAULT_VALUE_FOR_TEXT);
			}
		});
	}

	public NamedOperation setDefaultExpressionOnNullableColumn() {
		return new NamedOperation("set-default-expression-on-nullable-column", new Operation() {

			@Override
			public void prepare(Database backend) throws SQLException {
				backend.getTable("users").addColumn(new ColumnDefinition("email", "varchar(255)"));
			}

			@Override
			public void perform(Database backend) throws SQLException {
				backend.getTable("users").getColumn("email")
						.setDefaultExpression("\'SOMETHING ELSE\'");
			}

			@Override
			public void cleanup(Database backend) throws SQLException {
				backend.getTable("users").getColumn("email").drop();
			}
		});
	}

	public NamedOperation setDefaultExpressionOnNonNullableColumn() {
		return new NamedOperation("set-default-expression-on-non-nullable-column", new Operation() {

			@Override
			public void prepare(Database backend) throws SQLException {
				backend.getTable("users").addColumn(new ColumnDefinition("email", "varchar(255)")
						.setDefaultExpression("\'NOT_SET\'")
						.setNullable(false));
			}

			@Override
			public void perform(Database backend) throws SQLException {
				backend.getTable("users").getColumn("email").setDefaultExpression("\'SOMETHING ELSE\'");
			}

			@Override
			public void cleanup(Database backend) throws SQLException {
				backend.getTable("users").getColumn("email").drop();
			}
		});
	}

	public NamedOperation makeColumnNullable() {
		return new NamedOperation("make-column-nullable", new Operation() {

			@Override
			public void prepare(Database backend) throws SQLException {
				backend.getTable("users").addColumn(new ColumnDefinition("email", "varchar(255)")
						.setDefaultExpression("\'NOT_SET\'")
						.setNullable(false));
			}

			@Override
			public void perform(Database backend) throws SQLException {
				backend.getTable("users").getColumn("email").setNullable(true);
			}
		});
	}

	public NamedOperation makeColumnNonNullable() {
		return new NamedOperation("make-column-non-nullable", new Operation() {

			@Override
			public void prepare(Database backend) throws SQLException {
				backend.getTable("users").getColumn("email").setNullable(true);
			}

			@Override
			public void perform(Database backend) throws SQLException {
				backend.getTable("users").getColumn("email").setNullable(false);
			}
		});
	}

	public NamedOperation addNonNullableForeignKey() {
		return new NamedOperation("add-non-nullable-foreign-key", new Operation() {

			@Override
			public void prepare(Database backend) throws SQLException {
				TableDefinition table = new TableDefinition("addresses")
						.withColumn(new ColumnDefinition("id", "bigint")
								.setIdentity(true)
								.setAutoIncrement(true))
						.withColumn(new ColumnDefinition("address", "varchar(255)")
								.setDefaultExpression("''")
								.setNullable(false));

				backend.createTable(table);
				backend.query("INSERT INTO addresses (address) VALUES ('Unknown')");
				backend.getTable("users").addColumn(new ColumnDefinition("address_id", "bigint")
						.setDefaultExpression("'1'")
						.setNullable(false));
			}

			@Override
			public void perform(Database backend) throws SQLException {
				backend.getTable("users").addForeignKey("users_address", new String[] { "address_id" },
						"addresses", new String[] { "id" });
			}

			@Override
			public void cleanup(Database backend) throws SQLException {
				backend.getTable("users").getForeignKey("users_address").drop();
				backend.getTable("users").getColumn("address_id").drop();
				backend.getTable("addresses").drop();
			}
		});
	}

	public NamedOperation addNullableForeignKey() {
		return new NamedOperation("add-nullable-foreign-key", new Operation() {

			@Override
			public void prepare(Database backend) throws SQLException {
				TableDefinition table = new TableDefinition("addresses")
						.withColumn(new ColumnDefinition("id", "bigint")
								.setIdentity(true)
								.setAutoIncrement(true))
						.withColumn(new ColumnDefinition("address", "varchar(255)")
								.setDefaultExpression("''")
								.setNullable(false));

				backend.createTable(table);
				backend.query("INSERT INTO addresses (address) VALUES ('Unknown')");
				backend.getTable("users").addColumn(new ColumnDefinition("address_id", "bigint"));
			}

			@Override
			public void perform(Database backend) throws SQLException {
				backend.getTable("users").addForeignKey("users_address", new String[] { "address_id" },
						"addresses", new String[] { "id" });
			}

			@Override
			public void cleanup(Database backend) throws SQLException {
				backend.getTable("users").getForeignKey("users_address").drop();
				backend.getTable("users").getColumn("address_id").drop();
				backend.getTable("addresses").drop();
			}
		});
	}

}
