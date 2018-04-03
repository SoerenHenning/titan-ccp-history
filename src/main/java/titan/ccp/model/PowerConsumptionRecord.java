package titan.ccp.model;

import java.nio.BufferOverflowException;

import kieker.common.exception.RecordInstantiationException;
import kieker.common.record.AbstractMonitoringRecord;
import kieker.common.record.IMonitoringRecord;
import kieker.common.record.io.IValueDeserializer;
import kieker.common.record.io.IValueSerializer;
import kieker.common.util.registry.IRegistry;


/**
 * @author Sören Henning
 * API compatibility: Kieker 1.13.0
 * 
 * @since 1.13
 */
public class PowerConsumptionRecord extends AbstractMonitoringRecord implements IMonitoringRecord.Factory, IMonitoringRecord.BinaryFactory {			
	/** Descriptive definition of the serialization size of the record. */
	public static final int SIZE = TYPE_SIZE_STRING // PowerConsumptionRecord.identifier
			 + TYPE_SIZE_LONG // PowerConsumptionRecord.timestamp
			 + TYPE_SIZE_INT; // PowerConsumptionRecord.powerConsumptionInWh
	
	public static final Class<?>[] TYPES = {
		String.class, // PowerConsumptionRecord.identifier
		long.class, // PowerConsumptionRecord.timestamp
		int.class, // PowerConsumptionRecord.powerConsumptionInWh
	};
	
	/** default constants. */
	public static final String IDENTIFIER = "";
	private static final long serialVersionUID = 7891241250783138742L;
	
	/** property name array. */
	private static final String[] PROPERTY_NAMES = {
		"identifier",
		"timestamp",
		"powerConsumptionInWh",
	};
	
	/** property declarations. */
	private final String identifier;
	private final long timestamp;
	private final int powerConsumptionInWh;
	
	/**
	 * Creates a new instance of this class using the given parameters.
	 * 
	 * @param identifier
	 *            identifier
	 * @param timestamp
	 *            timestamp
	 * @param powerConsumptionInWh
	 *            powerConsumptionInWh
	 */
	public PowerConsumptionRecord(final String identifier, final long timestamp, final int powerConsumptionInWh) {
		this.identifier = identifier == null?"":identifier;
		this.timestamp = timestamp;
		this.powerConsumptionInWh = powerConsumptionInWh;
	}

	/**
	 * This constructor converts the given array into a record.
	 * It is recommended to use the array which is the result of a call to {@link #toArray()}.
	 * 
	 * @param values
	 *            The values for the record.
	 *
	 * @deprecated since 1.13. Use {@link #PowerConsumptionRecord(IValueDeserializer)} instead.
	 */
	@Deprecated
	public PowerConsumptionRecord(final Object[] values) { // NOPMD (direct store of values)
		AbstractMonitoringRecord.checkArray(values, TYPES);
		this.identifier = (String) values[0];
		this.timestamp = (Long) values[1];
		this.powerConsumptionInWh = (Integer) values[2];
	}

	/**
	 * This constructor uses the given array to initialize the fields of this record.
	 * 
	 * @param values
	 *            The values for the record.
	 * @param valueTypes
	 *            The types of the elements in the first array.
	 *
	 * @deprecated since 1.13. Use {@link #PowerConsumptionRecord(IValueDeserializer)} instead.
	 */
	@Deprecated
	protected PowerConsumptionRecord(final Object[] values, final Class<?>[] valueTypes) { // NOPMD (values stored directly)
		AbstractMonitoringRecord.checkArray(values, valueTypes);
		this.identifier = (String) values[0];
		this.timestamp = (Long) values[1];
		this.powerConsumptionInWh = (Integer) values[2];
	}

	
	/**
	 * @param deserializer
	 *            The deserializer to use
	 * @throws RecordInstantiationException 
	 *            when the record could not be deserialized
	 */
	public PowerConsumptionRecord(final IValueDeserializer deserializer) throws RecordInstantiationException {
		this.identifier = deserializer.getString();
		this.timestamp = deserializer.getLong();
		this.powerConsumptionInWh = deserializer.getInt();
	}
	
	/**
	 * {@inheritDoc}
	 *
	 * @deprecated since 1.13. Use {@link #serialize(IValueSerializer)} with an array serializer instead.
	 */
	@Override
	@Deprecated
	public Object[] toArray() {
		return new Object[] {
			this.getIdentifier(),
			this.getTimestamp(),
			this.getPowerConsumptionInWh(),
		};
	}
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerStrings(final IRegistry<String> stringRegistry) {	// NOPMD (generated code)
		stringRegistry.get(this.getIdentifier());
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void serialize(final IValueSerializer serializer) throws BufferOverflowException {
		//super.serialize(serializer);
		serializer.putString(this.getIdentifier());
		serializer.putLong(this.getTimestamp());
		serializer.putInt(this.getPowerConsumptionInWh());
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Class<?>[] getValueTypes() {
		return TYPES; // NOPMD
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public String[] getValueNames() {
		return PROPERTY_NAMES; // NOPMD
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getSize() {
		return SIZE;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @deprecated This record uses the {@link kieker.common.record.IMonitoringRecord.Factory} mechanism. Hence, this method is not implemented.
	 */
	@Override
	@Deprecated
	public void initFromArray(final Object[] values) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(final Object obj) {
		if (obj == null) {
			return false;
		}
		if (obj == this) {
			return true;
		}
		if (obj.getClass() != this.getClass()) {
			return false;
		}
		
		final PowerConsumptionRecord castedRecord = (PowerConsumptionRecord) obj;
		if (this.getLoggingTimestamp() != castedRecord.getLoggingTimestamp()) {
			return false;
		}
		if (!this.getIdentifier().equals(castedRecord.getIdentifier())) {
			return false;
		}
		if (this.getTimestamp() != castedRecord.getTimestamp()) {
			return false;
		}
		if (this.getPowerConsumptionInWh() != castedRecord.getPowerConsumptionInWh()) {
			return false;
		}
		
		return true;
	}
	
	public final String getIdentifier() {
		return this.identifier;
	}
	
	
	public final long getTimestamp() {
		return this.timestamp;
	}
	
	
	public final int getPowerConsumptionInWh() {
		return this.powerConsumptionInWh;
	}
	
}
