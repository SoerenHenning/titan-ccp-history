package titan.ccp.aggregation.experimental.kieker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.commons.math3.util.Pair;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.google.common.collect.Streams;

import kieker.common.record.IMonitoringRecord;
import titan.ccp.models.records.PowerConsumptionRecord;

public class CassandraWriter {

	private static final String RECORD_TYPE_NAME = "recordType";
	private static final Class<?> RECORD_TYPE_TYPE = String.class;

	private static final String LOGGING_TIMESTAMP_NAME = "loggingTimestamp";
	private static final Class<?> LOGGING_TIMESTAMP_TYPE = long.class;

	private final Session session;

	private final TableMapper tableMapper = new TableMapper();

	private final Set<String> existingTables = new HashSet<>();

	public CassandraWriter() {
		// TODO Auto-generated constructor stub
		final String host = "";
		final int port = 0;
		final String keyspace = "";

		final Cluster cluster = Cluster.builder().addContactPoint(host).withPort(port).build();
		this.session = cluster.connect(keyspace);
	}

	public void write(final IMonitoringRecord record) {
		final String tableName = this.tableMapper.tableNameMapper.apply(record);

		this.createTableIfNotExists(tableName, record);

		this.store(tableName, record);
	}

	private void createTableIfNotExists(final String tableName, final IMonitoringRecord record) {
		if (!this.existingTables.contains(tableName)) {
			this.createTable(tableName, record);
			this.existingTables.add(tableName);
		}
	}

	private void createTable(final String tableName, final IMonitoringRecord record) {
		final List<String> includedFields = this.getFields(record);
		final List<Class<?>> includedFieldTypes = this.getFieldTypes(record);

		final Set<String> partitionKey = this.tableMapper.primaryKeySelectionStrategy.selectPartitionKeys(tableName, includedFields);
		final Set<String> clusteringColumns = this.tableMapper.primaryKeySelectionStrategy.selectClusteringColumns(tableName, includedFields);

		final Create createStatement = SchemaBuilder.createTable(tableName).ifNotExists();

		Streams.zip(includedFields.stream(), includedFieldTypes.stream(), Pair::create)
				.forEach(field -> {
					if (partitionKey.contains(field.getKey())) {
						createStatement.addPartitionKey(field.getKey(), JavaTypeMapper.map(field.getValue()));
					} else if (clusteringColumns.contains(field.getKey())) {
						createStatement.addClusteringColumn(field.getKey(), JavaTypeMapper.map(field.getValue()));
					} else {
						createStatement.addColumn(field.getKey(), JavaTypeMapper.map(field.getValue()));
					}
				});

		this.session.execute(createStatement);
	}

	private void store(final String table, final IMonitoringRecord record) {
		final String[] valueNames = record.getValueNames();
		final Object[] values = new Object[valueNames.length];
		record.serialize(new ArrayValueSerializer(values));

		final Insert insertStatement = QueryBuilder.insertInto(table);
		if (this.tableMapper.includeRecordType) {
			insertStatement.value(RECORD_TYPE_NAME, record.getClass().getName());
		}
		if (this.tableMapper.includeLoggingTimestamp) {
			insertStatement.value(LOGGING_TIMESTAMP_NAME, record.getLoggingTimestamp());
		}
		insertStatement.values(valueNames, values);

		// TODO execute
		this.session.execute(insertStatement);
	}

	private List<String> getFields(final IMonitoringRecord record) {
		final String[] valueNames = record.getValueNames();

		final List<String> fields = new ArrayList<>(valueNames.length + 2);
		if (this.tableMapper.includeRecordType) {
			fields.add(RECORD_TYPE_NAME);
		}
		if (this.tableMapper.includeLoggingTimestamp) {
			fields.add(LOGGING_TIMESTAMP_NAME);
		}
		Collections.addAll(fields, valueNames);
		return fields;
	}

	private List<Class<?>> getFieldTypes(final IMonitoringRecord record) {
		final Class<?>[] valueTypes = record.getValueTypes();

		final List<Class<?>> fieldTypes = new ArrayList<>(valueTypes.length + 2);
		if (this.tableMapper.includeRecordType) {
			fieldTypes.add(RECORD_TYPE_TYPE);
		}
		if (this.tableMapper.includeLoggingTimestamp) {
			fieldTypes.add(LOGGING_TIMESTAMP_TYPE);
		}
		Collections.addAll(fieldTypes, valueTypes);
		return fieldTypes;
	}

	// Default behavior of PK selector: options:
	// - Use loggingTimestamp
	// - Use all fields

	public static class TableMapper {

		public Function<IMonitoringRecord, String> tableNameMapper = t -> t.getClass().getName();

		public PrimaryKeySelectionStrategy primaryKeySelectionStrategy = null; // TODO

		// public BiFunction<String, IMonitoringRecord, String> partitionKeySelector =
		// null; // TODO

		// public BiFunction<String, IMonitoringRecord, Set<String>>
		// clusteringColumnSelector = null; // TODO

		public boolean includeRecordType = false;

		public boolean includeLoggingTimestamp = true;

		public boolean myIncludeLoggingTimestamp = false;

		public BiFunction<String, IMonitoringRecord, String> myPartitionKeySelector = (tableName, record) -> {
			if (record instanceof PowerConsumptionRecord) {
				return "identifier";
			} else {
				return ""; // TODO
			}
		}; // TODO

		public BiFunction<String, IMonitoringRecord, Set<String>> myClusteringColumnSelector = (tableName, record) -> {
			if (record instanceof PowerConsumptionRecord) {
				return Set.of("timestamp");
			} else {
				return Set.of(); // TODO
			}
		}; // TODO

	}

	public static class JavaTypeMapper {

		public static DataType map(final Class<?> type) {
			if (type == boolean.class) {
				return DataType.cboolean();
			} else if (type == Boolean.class) {
				return DataType.cboolean();
			} else if (type == byte.class) {
				return DataType.tinyint();
			} else if (type == Byte.class) {
				return DataType.tinyint();
			} else if (type == char.class) {
				return DataType.text();
			} else if (type == Character.class) {
				return DataType.text();
			} else if (type == short.class) {
				return DataType.smallint();
			} else if (type == Short.class) {
				return DataType.smallint();
			} else if (type == int.class) {
				return DataType.cint();
			} else if (type == Integer.class) {
				return DataType.cint();
			} else if (type == long.class) {
				return DataType.bigint();
			} else if (type == Long.class) {
				return DataType.bigint();
			} else if (type == float.class) {
				return DataType.cfloat();
			} else if (type == Float.class) {
				return DataType.cfloat();
			} else if (type == double.class) {
				return DataType.cdouble();
			} else if (type == Double.class) {
				return DataType.cdouble();
			} else if (type == Enum.class) {
				return DataType.cint(); // Depend on array serialization strategy
			} else if (type == byte[].class) {
				return DataType.blob();
			} else if (type == Byte[].class) {
				return DataType.blob();
			} else if (type == String.class) {
				return DataType.text();
			} else {
				return null; // TODO throw execption
			}
		}

	}

	public static void main(final String[] args) {
		final PowerConsumptionRecord record = new PowerConsumptionRecord("my-sensor", 12345678, 42);
		new CassandraWriter().write(record);

	}

}
