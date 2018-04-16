package titan.ccp.aggregation.experimental.kieker.cassandra;

import java.util.List;
import java.util.Set;

public interface PrimaryKeySelectionStrategy {

	public Set<String> selectPartitionKeys(String tableName, List<String> possibleColumns);

	public Set<String> selectClusteringColumns(String tableName, List<String> possibleColumns);

}
