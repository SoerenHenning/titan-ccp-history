package titan.ccp.aggregation.experimental.teetimebased;

import java.io.PrintWriter;
import java.util.function.Consumer;
import java.util.function.Function;

import teetime.framework.AbstractConsumerStage;
import teetime.framework.AbstractProducerStage;
import teetime.stage.basic.AbstractTransformation;
import titan.ccp.models.records.ActivePowerRecord;

public class StageFactory {

	public static <T, O> AbstractProducerStage<O> producer(final Function<Consumer<O>, T> logicFactory,
			final Function<T, Procedure> method) {
		return new AbstractProducerStage<>() {

			T logic = logicFactory.apply(this.getOutputPort()::send);

			@Override
			protected void execute() throws Exception {
				method.apply(this.logic).execute();
			}
		};
	}

	public static <T, I> AbstractConsumerStage<I> consumer(final T logic, final Function<T, Consumer<I>> method) {
		return new AbstractConsumerStage<>() {
			@Override
			protected void execute(final I element) throws Exception {
				method.apply(logic).accept(element);
			}
		};
	}

	public static <T, I, O> AbstractTransformation<I, O> transformation(final Function<Consumer<O>, T> logicFactory,
			final Function<T, Consumer<I>> method) {
		return new AbstractTransformation<>() {

			T logic = logicFactory.apply(this.getOutputPort()::send);

			@Override
			protected void execute(final I element) throws Exception {
				method.apply(this.logic).accept(element);
			}
		};
	}

	@FunctionalInterface
	public static interface Procedure {

		public void execute();

	}

	public static void main(final String[] args) {
		// StageFactory.consumer<PrintWriter,String>(new PrintWriter(System.out), w ->
		// w::println);
		final AbstractConsumerStage<String> consumer = StageFactory.consumer(new PrintWriter(System.out),
				w -> (final String s) -> w.println(s));

		final AbstractTransformation<ActivePowerRecord, AggregationResult> transformation = StageFactory
				.transformation(s -> new Aggregator(null, null, s), (final Aggregator a) -> a::process);

	}

}
