package titan.ccp.aggregation.experimental.kieker.cassandra;

import com.datastax.driver.core.DataType;

public final class JavaTypeMapper {

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