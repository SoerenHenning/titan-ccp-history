package titan.ccp.aggregation.experimental.kieker;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ExplicitPrimaryKeySelectionStrategyTest {

	private static final String EXAMPLE_TABLE_NAME = "table";

	private static final String EXAMPLE_PARTITION_KEY_1 = "first_partition_key";

	private static final String EXAMPLE_PARTITION_KEY_2 = "second_partition_key";

	private static final String EXAMPLE_CLUSTERING_COLUMN_1 = "first_clustering_column";

	private static final String EXAMPLE_CLUSTERING_COLUMN_2 = "second_clustering_column";

	private ExplicitPrimaryKeySelectionStrategy strategy;

	@Before
	public void setUp() throws Exception {
		this.strategy = new ExplicitPrimaryKeySelectionStrategy(null); // TODO null temporary, should not effect test
	}

	@After
	public void tearDown() throws Exception {
		this.strategy = null;
	}

	@Test
	public void testClusteringColumnsWithVarargs() {
		this.strategy.registerClusteringColumns(EXAMPLE_TABLE_NAME, EXAMPLE_CLUSTERING_COLUMN_1, EXAMPLE_CLUSTERING_COLUMN_2);
		final Set<String> returnedClusteringColumns = this.strategy.selectClusteringColumns(EXAMPLE_TABLE_NAME, List.of());
		assertEquals(returnedClusteringColumns, Set.of(EXAMPLE_CLUSTERING_COLUMN_1, EXAMPLE_CLUSTERING_COLUMN_2));
	}

	@Test
	public void testClusteringColumnsWithCollection() {
		this.strategy.registerClusteringColumns(EXAMPLE_TABLE_NAME, List.of(EXAMPLE_PARTITION_KEY_1, EXAMPLE_CLUSTERING_COLUMN_2));
		final Set<String> returnedClusteringColumns = this.strategy.selectClusteringColumns(EXAMPLE_TABLE_NAME, List.of());
		assertEquals(returnedClusteringColumns, Set.of(EXAMPLE_PARTITION_KEY_1, EXAMPLE_CLUSTERING_COLUMN_2));
	}

	@Test
	public void testRegisterPartitionKeysWithVarargs() {
		this.strategy.registerPartitionKeys(EXAMPLE_TABLE_NAME, EXAMPLE_PARTITION_KEY_1, EXAMPLE_PARTITION_KEY_2);
		final Set<String> returnedPartitionKeys = this.strategy.selectPartitionKeys(EXAMPLE_TABLE_NAME, List.of());
		assertEquals(returnedPartitionKeys, Set.of(EXAMPLE_PARTITION_KEY_1, EXAMPLE_PARTITION_KEY_2));
	}

	@Test
	public void testRegisterPartitionKeysWithCollection() {
		this.strategy.registerPartitionKeys(EXAMPLE_TABLE_NAME, List.of(EXAMPLE_PARTITION_KEY_1, EXAMPLE_PARTITION_KEY_2));
		final Set<String> returnedPartitionKeys = this.strategy.selectPartitionKeys(EXAMPLE_TABLE_NAME, List.of());
		assertEquals(returnedPartitionKeys, Set.of(EXAMPLE_PARTITION_KEY_1, EXAMPLE_PARTITION_KEY_2));
	}

}
