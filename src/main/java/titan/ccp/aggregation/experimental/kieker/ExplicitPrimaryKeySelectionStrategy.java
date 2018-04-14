package titan.ccp.aggregation.experimental.kieker;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ExplicitPrimaryKeySelectionStrategy implements PrimaryKeySelectionStrategy {

	private final Map<String, String> partionKeys = new HashMap<>();

	private final Map<String, Set<String>> clusteringColumns = new HashMap<>();

	private final PrimaryKeySelectionStrategy fallbackStrategy;

	public ExplicitPrimaryKeySelectionStrategy() {

	}

	@Override
	public String selectPartitionKey(final String tableName, final List<String> possibleColumns) {
		final String partionKey = this.partionKeys.get(tableName);
		if (partionKey == null) {
			return this.fallbackStrategy.selectPartitionKey(tableName, possibleColumns);
		} else {
			return partionKey;
		}
	}

	@Override
	public Set<String> selectClusteringColumn(final String tableName, final List<String> possibleColumns) {
		final Set<String> clusteringColumns = this.clusteringColumns.get(tableName);
		if (clusteringColumns == null) {
			return this.fallbackStrategy.selectClusteringColumn(tableName, possibleColumns);
		} else {
			return clusteringColumns;
		}
	}

	public void registerPartitionKey(final String tableName, final String partitionKey) {
		this.partionKeys.put(tableName, partitionKey);
	}

	public void registerClusteringColumns(final String tableName, final String... clusteringColumns) {
		this.clusteringColumns.put(tableName, Set.of(clusteringColumns));
	}

	public void registerClusteringColumns(final String tableName, final Collection<String> clusteringColumns) {
		// this.clusteringColumns.put(tableName, Set.copyOf(clusteringColumns)); // Java
		// 10
		this.clusteringColumns.put(tableName, new HashSet<>(clusteringColumns));
	}

}
