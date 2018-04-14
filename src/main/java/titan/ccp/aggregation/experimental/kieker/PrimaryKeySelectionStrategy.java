package titan.ccp.aggregation.experimental.kieker;

import java.util.List;
import java.util.Set;

public interface PrimaryKeySelectionStrategy {

	public String selectPartitionKey(String tableName, List<String> possibleColumns);

	public Set<String> selectClusteringColumn(String tableName, List<String> possibleColumns);

}
