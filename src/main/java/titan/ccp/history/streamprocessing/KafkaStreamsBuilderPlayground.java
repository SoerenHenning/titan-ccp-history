package titan.ccp.history.streamprocessing; // NOPMD

import com.datastax.driver.core.Session;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import kieker.common.record.IMonitoringRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import titan.ccp.common.cassandra.CassandraWriter;
import titan.ccp.common.cassandra.ExplicitPrimaryKeySelectionStrategy;
import titan.ccp.common.cassandra.PredefinedTableNameMappers;
import titan.ccp.common.kieker.cassandra.KiekerDataAdapter;
import titan.ccp.common.kieker.kafka.IMonitoringRecordSerde;
import titan.ccp.configuration.events.Event;
import titan.ccp.configuration.events.EventSerde;
import titan.ccp.model.sensorregistry.Sensor;
import titan.ccp.model.sensorregistry.SensorRegistry;
import titan.ccp.models.records.ActivePowerRecord;
import titan.ccp.models.records.ActivePowerRecordFactory;
import titan.ccp.models.records.AggregatedActivePowerRecordFactory;

/**
 * Builder for the Kafka Streams configuration.
 */
@Deprecated
public class KafkaStreamsBuilderPlayground {

  private static final String APPLICATION_ID = "titanccp-aggregation-0.0.11";

  private static final int COMMIT_INTERVAL_MS = 1000;

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamsBuilderPlayground.class);

  private String bootstrapServers; // NOPMD
  private String inputTopic; // NOPMD
  private String outputTopic; // NOPMD
  private String configurationTopic; // NOPMD
  private final String aggregationStoreName = "stream-store"; // NOPMD

  private SensorRegistry sensorRegistry; // NOPMD
  private Session cassandraSession; // NOPMD

  public KafkaStreamsBuilderPlayground sensorRegistry(final SensorRegistry sensorRegistry) {
    this.sensorRegistry = sensorRegistry;
    return this;
  }

  public KafkaStreamsBuilderPlayground cassandraSession(final Session cassandraSession) {
    this.cassandraSession = cassandraSession;
    return this;
  }

  public KafkaStreamsBuilderPlayground inputTopic(final String inputTopic) {
    this.inputTopic = inputTopic;
    return this;
  }

  public KafkaStreamsBuilderPlayground outputTopic(final String outputTopic) {
    this.outputTopic = outputTopic;
    return this;
  }

  public KafkaStreamsBuilderPlayground configurationTopic(final String configurationTopic) {
    this.configurationTopic = configurationTopic;
    return this;
  }

  public KafkaStreamsBuilderPlayground bootstrapServers(final String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
    return this;
  }

  public KafkaStreams build() {
    return new KafkaStreams(this.buildTopology(), this.buildProperties());
  }

  private Topology buildTopology() {
    final StreamsBuilder builder = new StreamsBuilder();

    LOGGER.info("Listening for records at topic '{}'", this.inputTopic);

    final KStream<Event, String> configurationStream =
        builder.stream(this.configurationTopic, Consumed.with(EventSerde.serde(), Serdes.String()))
            .filter((key, value) -> key == Event.SENSOR_REGISTRY_CHANGED);

    configurationStream.foreach((k, v) -> System.out.println(k + ": " + v));

    // final KStream<String, String> childParentsChangelog =
    // configurationStream.mapValues(data -> SensorRegistry.fromJson(data))
    // .flatMap((key, registry) -> {
    // // Registry -> Map/List<Sensor -> All Parents>
    // return registry.getMachineSensors().stream()
    // .map(s -> KeyValue.pair(s.getIdentifier(),
    // s.getParents().stream().map(p -> p.getIdentifier())
    // .collect(Collectors.joining(";"))))
    // .collect(Collectors.toList());
    // });
    //
    // final KTable<String, String> childParentsTable =
    // childParentsChangelog.groupByKey(Grouped.with(Serdes.String(), Serdes.String())).reduce(
    // (aggValue, newValue) -> newValue, Materialized.with(Serdes.String(), Serdes.String()));

    // childParentsTable.toStream().foreach((k, v) -> System.out.println("CPT: " + k + ':' + v));



    // ============

    final KStream<Event, String> childPrentsChangelog2 = configurationStream
        .mapValues(data -> SensorRegistry.fromJson(data))
        .mapValues(registry -> {
          // Registry -> Map/List<Parent -> All children>
          final List<KeyValue<String, String>> childParentsPairs = registry
              .getMachineSensors()
              .stream()
              .map(child -> KeyValue.pair(
                  child.getIdentifier(),
                  child.getParents()
                      .stream()
                      .map(Sensor::getIdentifier)
                      .collect(Collectors.joining(";"))))
              .collect(Collectors.toList());


          return childParentsPairs.stream()
              .map(e -> e.key + ':' + e.value)
              .collect(Collectors.joining("+"));

          // final List<KeyValue<String, String>> parentChildrenPairs = new ArrayList<>();
          // new SensorRegistryTraverser().traverseAggregated(registry, parent -> {
          // final String children = parent.getAllChildren()
          // .stream()
          // .map(Sensor::getIdentifier)
          // .collect(Collectors.joining(";"));
          // parentChildrenPairs.add(KeyValue.pair(parent.getIdentifier(), children));
          // });
          // return parentChildrenPairs.stream()
          // .map(e -> e.key + ':' + e.value)
          // .collect(Collectors.joining("+"));
        });


    final KTable<Event, String> sensorRegistryMetaTable = childPrentsChangelog2
        .groupByKey(Grouped.with(EventSerde.serde(), Serdes.String()))
        .aggregate(() -> "", (key, value, aggr) -> { // reduce should be sufficient
          if (aggr.equals("")) {
            // Nothing has changed
            return value;
          }
          final String[] oldMapEntrys = aggr.split("\\+");
          final Map<String, String> oldMap = new HashMap<>();
          for (final String entry : oldMapEntrys) {
            final String[] keyAndValue = entry.split(":");
            final String child = keyAndValue[0];
            final String parents = keyAndValue[1];
            oldMap.put(child, parents);
          }
          final String[] newMapEntrys = value.split("\\+");
          final Map<String, String> newMap = new HashMap<>();
          for (final String entry : newMapEntrys) {
            final String[] keyAndValue = entry.split(":");
            final String child = keyAndValue[0];
            final String parents = keyAndValue[1];
            newMap.put(child, parents);
          }

          for (final Entry<String, String> entry : oldMap.entrySet()) {
            if (!newMap.containsKey(entry.getKey())) {
              if (entry.getValue().equals("DELETE")) {
                newMap.remove(entry.getKey(), "DELETE");
              } else {
                newMap.put(entry.getKey(), "DELETE"); // DELETE is tombstone
              }
            }
            // else {
            // newMap.compute(entry.getKey(), (k, v) -> {
            // if (v != entry.getValue()) {
            // return "C~" + v; // Set flag
            // } else {
            // return "U~" + v; // Unsetflag
            // }
            // });
            // }
          }

          return newMap.entrySet().stream()
              .map(e -> e.getKey() + ':' + e.getValue())
              .collect(Collectors.joining("+"));

        }, Materialized.with(EventSerde.serde(), Serdes.String()));

    sensorRegistryMetaTable.toStream()
        .foreach((k, v) -> System.out.println("SRMT: " + k + ":" + v));

    final KTable<String, String> childParentsTable2 = sensorRegistryMetaTable
        .toStream()
        .flatMap((key, value) -> {
          if (value.equals("")) {
            return List.of();
          }
          final String[] mapEntrys = value.split("\\+");
          final Map<String, String> map = new HashMap<>();
          for (final String entry : mapEntrys) {
            final String[] keyAndValue = entry.split(":");
            final String child = keyAndValue[0];
            final String parents = keyAndValue[1]; // .equals("null") ? "DELETE" : keyAndValue[1];
            map.put(child, parents);
          }

          return map.entrySet().stream()
              .map(entry -> KeyValue.pair(entry.getKey(), entry.getValue()))
              .collect(Collectors.toList());
        })
        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
        .reduce((old, newValue) -> {
          return newValue.equals("DELETE") ? null : newValue;
        },
            Materialized.with(Serdes.String(), Serdes.String()));


    childParentsTable2.toStream()
        .foreach((k, v) -> {
          System.out.println("CPT: " + k + ":" + (v == null ? "_NULL_" : v));
        });


    // === Transformer

    // create store
    final StoreBuilder<KeyValueStore<String, String>> keyValueStoreBuilder =
        Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("myTransformState"),
            Serdes.String(),
            Serdes.String()).withLoggingEnabled(Map.of());
    // register store
    builder.addStateStore(keyValueStoreBuilder);

    final KTable<String, ActivePowerRecord> inputTable = builder.table(this.inputTopic, Consumed
        .with(Serdes.String(), IMonitoringRecordSerde.serde(new ActivePowerRecordFactory())));


    final KStream<String, ActivePowerRecord> betterXyzStream = inputTable
        .join(childParentsTable2, (record, parents) -> Pair.of(parents, record))
        .toStream()
        .transform(
            new TransformerSupplier<String, Pair<String, ActivePowerRecord>, KeyValue<String, ActivePowerRecord>>() {
              @Override
              public Transformer<String, Pair<String, ActivePowerRecord>, KeyValue<String, ActivePowerRecord>> get() {
                return new Transformer<>() {
                  private ProcessorContext context;
                  private KeyValueStore<String, String> state;

                  @SuppressWarnings("unchecked")
                  public void init(final ProcessorContext context) {
                    this.context = context;
                    this.state =
                        (KeyValueStore<String, String>) context.getStateStore("myTransformState");
                    // punctuate each 1000ms; can access this.state
                    // can emit as many new KeyValue pairs as required via this.context#forward()
                    // context.schedule(1000, PunctuationType.WALL_CLOCK_TIME, new Punctuator(..));
                  }

                  public KeyValue<String, ActivePowerRecord> transform(final String key,
                      final Pair<String, ActivePowerRecord> joinedValue) {

                    final String parentsString = joinedValue.getLeft();
                    final ActivePowerRecord record = joinedValue.getRight();

                    // System.out.println("Transform: " + key + ": " + parentsString);
                    final String oldParentsString = this.state.get(key);
                    // System.out.println("Old Value: " + (oldParentsString == null ? "NULL" :
                    // oldParentsString));

                    // TODO Instead deserialize
                    final Set<String> newParents =
                        parentsString == null ? Set.of() : Set.of(parentsString.split(";"));
                    final Set<String> oldParents =
                        oldParentsString == null ? Set.of() : Set.of(oldParentsString.split(";"));

                    for (final String parent : newParents) {
                      final String key2 = record.getIdentifier() + '#' + parent;
                      // System.out.println("Forward: " + key2 + ":" + record.toString());
                      this.context.forward(key2, record);
                    }

                    if (!newParents.equals(oldParents)) {
                      for (final String oldParent : oldParents) {
                        if (!newParents.contains(oldParent)) {
                          final String key2 = record.getIdentifier() + '#' + oldParent;
                          // System.out.println("Forward: " + key2 + ":" + record.toString());
                          this.context.forward(key2, record);
                        }
                      }
                      this.state.put(key, parentsString); // TODO instead serialize
                    }

                    // can access this.state
                    // can emit as many new KeyValue pairs as required via this.context#forward()
                    // return new KeyValue<>(key, value); // can emit a single value via return --
                    //
                    return null;
                  }

                  public void close() {
                    // can access this.state
                    // can emit as many new KeyValue pairs as required via this.context#forward()
                  }
                };
              }
            }, "myTransformState");
    // betterXyzStream.foreach((k, v) -> System.out.println("TRANS: " + k + ':' + v));

    /*
     * final KTable<String, String> childParentsListTable = childParentsTable2.toStream()
     * .flatMap((k, v) -> { final String[] parents = v.split(";"); return List.of(parents) .stream()
     * .map(p -> KeyValue.pair(k + "#" + p, "xxx")) // No value required
     * .collect(Collectors.toList()); }) .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
     * .reduce((aggValue, newValue) -> newValue, Materialized.with(Serdes.String(),
     * Serdes.String()));
     *
     *
     * childParentsListTable.toStream().foreach((k, v) -> { System.out.println("CPLT: " + k + ":" +
     * v); });
     */


    // =======================


    // final KTable<String, String> deletionsPre = childParentsChangelog
    // .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
    // .aggregate(() -> "#", (key, value, aggr) -> {
    // final String[] split = aggr.split("#");
    // if (aggr.equals("#") || value.equals(split[0])) {
    // // Nothing has changed
    // return value + "#";
    // } else {
    // final List<String> oldChildren = List.of(split[1].split(";"));
    // final Set<String> newChildren = Set.of(value.split(";"));
    // // Sets.difference(old, set2)
    // final String changedChildren = oldChildren.stream()
    // .filter(p -> !newChildren.contains(p))
    // .collect(Collectors.joining(";"));
    // return value + "#" + changedChildren;
    // }
    // }, Materialized.with(Serdes.String(), Serdes.String()));
    // // deletionsPre.toStream().foreach((k, v) -> System.out.println("DTP: " + k + ':' + v));
    // final KTable<String, String> deletions = deletionsPre.mapValues(v -> v.split("#", -1)[1]);


    // deletions.toStream().foreach((k, v) -> System.out.println("DT: " + k + ':' + v));

    // final KStream<String, ActivePowerRecord> deletionsStream = deletions
    // .toStream()
    // .filterNot((k, v) -> {
    // return v.equals("");
    // })
    // .flatMap((k, v) -> {
    // return List.of(v.split(";"))
    // .stream()
    // .map(p -> KeyValue.pair(v + "#" + p, (ActivePowerRecord) null))
    // .collect(Collectors.toList());
    // });

    // deletionsStream.foreach((k, v) -> System.out.println("DS: " + k + ':' + v));


    // ===========



    // final KStream<String, ActivePowerRecord> inputStream = builder.stream(this.inputTopic,
    // Consumed
    // .with(Serdes.String(), IMonitoringRecordSerde.serde(new ActivePowerRecordFactory())));

    // inputStream.foreach((k, v) -> LOGGER.debug("Received record {}.", v));

    // final KTable<String, ActivePowerRecord> inputTable = builder.table(this.inputTopic,
    // Consumed.with(Serdes.String(), IMonitoringRecordSerde.serde(new
    // ActivePowerRecordFactory())));



    final KStream<String, ActivePowerRecord> flatMapped2 = inputTable
        .join(childParentsTable2, (record, parents) -> Pair.of(parents, record))
        .toStream()
        .flatMap((k, v) -> {
          final String[] parents = v.getLeft().split(";");
          final ActivePowerRecord record = v.getRight();
          return List.of(parents).stream()
              .map(parent -> KeyValue.pair(record.getIdentifier() + "#" + parent, record))
              .collect(Collectors.toList());
        });

    // flatMapped2.foreach((k, v) -> System.out.println("FM: " + k + ':' + v));

    //
    // final KStream<String, ActivePowerRecord> flatMapped =
    // inputStream.flatMap((key, value) -> this.flatMap(value));

    // final KStream<String, ActivePowerRecord> deletions = builder.stream("deletions", Consumed
    // .with(Serdes.String(), IMonitoringRecordSerde.serde(new ActivePowerRecordFactory())));

    // final KStream<String, ActivePowerRecord> parentsChangelog = flatMapped.merge(deletions);

    // final KGroupedStream<String, ActivePowerRecord> groupedStream =
    // flatMapped2.groupByKey(Grouped.with(Serdes.String(), IMonitoringRecordSerde.serde(new
    // ActivePowerRecordFactory())));

    final KGroupedStream<String, ActivePowerRecord> groupedStream =
        betterXyzStream.groupByKey(Grouped
            .with(Serdes.String(), IMonitoringRecordSerde.serde(new ActivePowerRecordFactory())));

    // TODO Also deduplicate here
    final KTable<String, ActivePowerRecord> lastValueTable =
        groupedStream.reduce((aggValue, newValue) -> newValue, Materialized.with(Serdes.String(),
            IMonitoringRecordSerde.serde(new ActivePowerRecordFactory())));

    lastValueTable
        .toStream()
        .foreach(
            (k, v) -> System.out.println("LVT: " + k + ':' + this.buildActivePowerRecordString(v)));

    lastValueTable
        .groupBy(
            (k, v) -> KeyValue.pair(k.split("#")[1], v),
            Grouped.with(
                Serdes.String(),
                IMonitoringRecordSerde.serde(new ActivePowerRecordFactory())))
        .aggregate(
            () -> 0.0,
            (k, v, r) -> r + v.getValueInW(), (k, v, r) -> r - v.getValueInW(),
            Materialized.with(Serdes.String(), Serdes.Double()))
        .toStream()
        .foreach((k, v) -> System.out.println(k + ';' + v));


    // ...

    // ==================

    final KTable<String, AggregationHistory> aggregated = groupedStream.aggregate(
        () -> new AggregationHistory(this.sensorRegistry), (aggKey, newValue, aggValue) -> {
          aggValue.update(newValue);
          LOGGER.debug("update history {}", aggValue);
          return aggValue;
        },
        Materialized
            .<String, AggregationHistory, KeyValueStore<Bytes, byte[]>>as(this.aggregationStoreName)
            .withKeySerde(Serdes.String())
            .withValueSerde(AggregationHistorySerde.serde(this.sensorRegistry)));

    aggregated
        .toStream()
        .map((key, value) -> KeyValue.pair(key, value.toRecord(key)))
        .to(this.outputTopic, Produced.with(Serdes.String(),
            IMonitoringRecordSerde.serde(new AggregatedActivePowerRecordFactory())));

    // Cassandra Writer for AggregatedActivePowerRecord
    // final CassandraWriter<IMonitoringRecord> cassandraWriter =
    // this.buildCassandraWriter(AggregatedActivePowerRecord.class);
    builder
        .stream(this.outputTopic,
            Consumed.with(Serdes.String(),
                IMonitoringRecordSerde.serde(new AggregatedActivePowerRecordFactory())))
        .foreach((key, record) -> {
          LOGGER.debug("write to cassandra {}", record); // NOCS
          // cassandraWriter.write(record);
        });

    // Cassandra Writer for ActivePowerRecord
    // final CassandraWriter<IMonitoringRecord> cassandraWriterForNormal =
    // this.buildCassandraWriter(ActivePowerRecord.class);
    // inputStream.foreach((key, record) -> {
    // LOGGER.debug("write to cassandra {}", record); // NOCS
    // // cassandraWriterForNormal.write(record);
    // });

    return builder.build();
  }

  private CassandraWriter<IMonitoringRecord> buildCassandraWriter(
      final Class<? extends IMonitoringRecord> recordClass) {
    final ExplicitPrimaryKeySelectionStrategy primaryKeySelectionStrategy =
        new ExplicitPrimaryKeySelectionStrategy();
    primaryKeySelectionStrategy.registerPartitionKeys(recordClass.getSimpleName(), "identifier");
    primaryKeySelectionStrategy.registerClusteringColumns(recordClass.getSimpleName(), "timestamp");

    final CassandraWriter<IMonitoringRecord> cassandraWriter =
        CassandraWriter.builder(this.cassandraSession, new KiekerDataAdapter())
            .tableNameMapper(PredefinedTableNameMappers.SIMPLE_CLASS_NAME)
            .primaryKeySelectionStrategy(primaryKeySelectionStrategy).build();

    return cassandraWriter;
  }

  private Iterable<KeyValue<String, ActivePowerRecord>> flatMap(final ActivePowerRecord record) {
    LOGGER.debug("Flat map record: {}", record); // TODO Temporary
    final List<KeyValue<String, ActivePowerRecord>> result =
        this.sensorRegistry.getSensorForIdentifier(record.getIdentifier()).stream()
            .flatMap(s -> s.getParents().stream()).map(s -> s.getIdentifier())
            .map(i -> KeyValue.pair(record.getIdentifier() + '#' + i, record))
            .collect(Collectors.toList());
    LOGGER.debug("Flat map result: {}", result); // TODO Temporary
    return result;
  }

  private String buildActivePowerRecordString(final ActivePowerRecord record) {
    return "{" + record.getIdentifier() + ';' + record.getTimestamp() + ';' + record.getValueInW()
        + '}';
  }

  private Properties buildProperties() {
    final Properties properties = new Properties();
    // TODO as parameter
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID); // TODO as parameter
    properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, COMMIT_INTERVAL_MS); // TODO as param.
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
    return properties;
  }

}
