package titan.ccp.aggregation.experimental.kieker.cassandra;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ExplicitPrimaryKeySelectionStrategy implements PrimaryKeySelectionStrategy {

	private final Map<String, Set<String>> partitionKeys = new HashMap<>();

	private final Map<String, Set<String>> clusteringColumns = new HashMap<>();

	private final PrimaryKeySelectionStrategy fallbackStrategy;

	public ExplicitPrimaryKeySelectionStrategy(final PrimaryKeySelectionStrategy fallbackStrategy) {
		this.fallbackStrategy = fallbackStrategy;
	}

	@Override
	public Set<String> selectPartitionKeys(final String tableName, final List<String> possibleColumns) {
		final Set<String> partionKeys = this.partitionKeys.get(tableName);
		if (partionKeys == null) {
			return this.fallbackStrategy.selectPartitionKeys(tableName, possibleColumns);
		} else {
			return partionKeys;
		}
	}

	@Override
	public Set<String> selectClusteringColumns(final String tableName, final List<String> possibleColumns) {
		final Set<String> clusteringColumns = this.clusteringColumns.get(tableName);
		if (clusteringColumns == null) {
			return this.fallbackStrategy.selectClusteringColumns(tableName, possibleColumns);
		} else {
			return clusteringColumns;
		}
	}

	public void registerPartitionKeys(final String tableName, final String... partitionKeys) {
		this.partitionKeys.put(tableName, Set.of(partitionKeys));
	}

	public void registerPartitionKeys(final String tableName, final Collection<String> partitionKeys) {
		// this.partitionKeys.put(tableName, Set.copyOf(partitionKeys)); // Java
		// 10
		this.partitionKeys.put(tableName, new HashSet<>(partitionKeys));
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
