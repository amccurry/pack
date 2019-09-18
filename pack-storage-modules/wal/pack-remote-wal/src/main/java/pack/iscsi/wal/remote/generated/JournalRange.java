/**
 * Autogenerated by Thrift Compiler (0.12.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package pack.iscsi.wal.remote.generated;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.12.0)", date = "2019-09-18")
public class JournalRange implements org.apache.thrift.TBase<JournalRange, JournalRange._Fields>, java.io.Serializable, Cloneable, Comparable<JournalRange> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("JournalRange");

  private static final org.apache.thrift.protocol.TField VOLUME_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("volumeId", org.apache.thrift.protocol.TType.I64, (short)1);
  private static final org.apache.thrift.protocol.TField BLOCK_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("blockId", org.apache.thrift.protocol.TType.I64, (short)2);
  private static final org.apache.thrift.protocol.TField UUID_FIELD_DESC = new org.apache.thrift.protocol.TField("uuid", org.apache.thrift.protocol.TType.STRING, (short)3);
  private static final org.apache.thrift.protocol.TField MIN_GENERATION_FIELD_DESC = new org.apache.thrift.protocol.TField("minGeneration", org.apache.thrift.protocol.TType.I64, (short)4);
  private static final org.apache.thrift.protocol.TField MAX_GENERATION_FIELD_DESC = new org.apache.thrift.protocol.TField("maxGeneration", org.apache.thrift.protocol.TType.I64, (short)5);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new JournalRangeStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new JournalRangeTupleSchemeFactory();

  public long volumeId; // required
  public long blockId; // required
  public @org.apache.thrift.annotation.Nullable java.lang.String uuid; // required
  public long minGeneration; // required
  public long maxGeneration; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    VOLUME_ID((short)1, "volumeId"),
    BLOCK_ID((short)2, "blockId"),
    UUID((short)3, "uuid"),
    MIN_GENERATION((short)4, "minGeneration"),
    MAX_GENERATION((short)5, "maxGeneration");

    private static final java.util.Map<java.lang.String, _Fields> byName = new java.util.HashMap<java.lang.String, _Fields>();

    static {
      for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // VOLUME_ID
          return VOLUME_ID;
        case 2: // BLOCK_ID
          return BLOCK_ID;
        case 3: // UUID
          return UUID;
        case 4: // MIN_GENERATION
          return MIN_GENERATION;
        case 5: // MAX_GENERATION
          return MAX_GENERATION;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByName(java.lang.String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final java.lang.String _fieldName;

    _Fields(short thriftId, java.lang.String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public java.lang.String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __VOLUMEID_ISSET_ID = 0;
  private static final int __BLOCKID_ISSET_ID = 1;
  private static final int __MINGENERATION_ISSET_ID = 2;
  private static final int __MAXGENERATION_ISSET_ID = 3;
  private byte __isset_bitfield = 0;
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.VOLUME_ID, new org.apache.thrift.meta_data.FieldMetaData("volumeId", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64        , "long")));
    tmpMap.put(_Fields.BLOCK_ID, new org.apache.thrift.meta_data.FieldMetaData("blockId", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64        , "long")));
    tmpMap.put(_Fields.UUID, new org.apache.thrift.meta_data.FieldMetaData("uuid", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.MIN_GENERATION, new org.apache.thrift.meta_data.FieldMetaData("minGeneration", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64        , "long")));
    tmpMap.put(_Fields.MAX_GENERATION, new org.apache.thrift.meta_data.FieldMetaData("maxGeneration", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64        , "long")));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(JournalRange.class, metaDataMap);
  }

  public JournalRange() {
  }

  public JournalRange(
    long volumeId,
    long blockId,
    java.lang.String uuid,
    long minGeneration,
    long maxGeneration)
  {
    this();
    this.volumeId = volumeId;
    setVolumeIdIsSet(true);
    this.blockId = blockId;
    setBlockIdIsSet(true);
    this.uuid = uuid;
    this.minGeneration = minGeneration;
    setMinGenerationIsSet(true);
    this.maxGeneration = maxGeneration;
    setMaxGenerationIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public JournalRange(JournalRange other) {
    __isset_bitfield = other.__isset_bitfield;
    this.volumeId = other.volumeId;
    this.blockId = other.blockId;
    if (other.isSetUuid()) {
      this.uuid = other.uuid;
    }
    this.minGeneration = other.minGeneration;
    this.maxGeneration = other.maxGeneration;
  }

  public JournalRange deepCopy() {
    return new JournalRange(this);
  }

  @Override
  public void clear() {
    setVolumeIdIsSet(false);
    this.volumeId = 0;
    setBlockIdIsSet(false);
    this.blockId = 0;
    this.uuid = null;
    setMinGenerationIsSet(false);
    this.minGeneration = 0;
    setMaxGenerationIsSet(false);
    this.maxGeneration = 0;
  }

  public long getVolumeId() {
    return this.volumeId;
  }

  public JournalRange setVolumeId(long volumeId) {
    this.volumeId = volumeId;
    setVolumeIdIsSet(true);
    return this;
  }

  public void unsetVolumeId() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __VOLUMEID_ISSET_ID);
  }

  /** Returns true if field volumeId is set (has been assigned a value) and false otherwise */
  public boolean isSetVolumeId() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __VOLUMEID_ISSET_ID);
  }

  public void setVolumeIdIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __VOLUMEID_ISSET_ID, value);
  }

  public long getBlockId() {
    return this.blockId;
  }

  public JournalRange setBlockId(long blockId) {
    this.blockId = blockId;
    setBlockIdIsSet(true);
    return this;
  }

  public void unsetBlockId() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __BLOCKID_ISSET_ID);
  }

  /** Returns true if field blockId is set (has been assigned a value) and false otherwise */
  public boolean isSetBlockId() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __BLOCKID_ISSET_ID);
  }

  public void setBlockIdIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __BLOCKID_ISSET_ID, value);
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.String getUuid() {
    return this.uuid;
  }

  public JournalRange setUuid(@org.apache.thrift.annotation.Nullable java.lang.String uuid) {
    this.uuid = uuid;
    return this;
  }

  public void unsetUuid() {
    this.uuid = null;
  }

  /** Returns true if field uuid is set (has been assigned a value) and false otherwise */
  public boolean isSetUuid() {
    return this.uuid != null;
  }

  public void setUuidIsSet(boolean value) {
    if (!value) {
      this.uuid = null;
    }
  }

  public long getMinGeneration() {
    return this.minGeneration;
  }

  public JournalRange setMinGeneration(long minGeneration) {
    this.minGeneration = minGeneration;
    setMinGenerationIsSet(true);
    return this;
  }

  public void unsetMinGeneration() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __MINGENERATION_ISSET_ID);
  }

  /** Returns true if field minGeneration is set (has been assigned a value) and false otherwise */
  public boolean isSetMinGeneration() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __MINGENERATION_ISSET_ID);
  }

  public void setMinGenerationIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __MINGENERATION_ISSET_ID, value);
  }

  public long getMaxGeneration() {
    return this.maxGeneration;
  }

  public JournalRange setMaxGeneration(long maxGeneration) {
    this.maxGeneration = maxGeneration;
    setMaxGenerationIsSet(true);
    return this;
  }

  public void unsetMaxGeneration() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __MAXGENERATION_ISSET_ID);
  }

  /** Returns true if field maxGeneration is set (has been assigned a value) and false otherwise */
  public boolean isSetMaxGeneration() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __MAXGENERATION_ISSET_ID);
  }

  public void setMaxGenerationIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __MAXGENERATION_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case VOLUME_ID:
      if (value == null) {
        unsetVolumeId();
      } else {
        setVolumeId((java.lang.Long)value);
      }
      break;

    case BLOCK_ID:
      if (value == null) {
        unsetBlockId();
      } else {
        setBlockId((java.lang.Long)value);
      }
      break;

    case UUID:
      if (value == null) {
        unsetUuid();
      } else {
        setUuid((java.lang.String)value);
      }
      break;

    case MIN_GENERATION:
      if (value == null) {
        unsetMinGeneration();
      } else {
        setMinGeneration((java.lang.Long)value);
      }
      break;

    case MAX_GENERATION:
      if (value == null) {
        unsetMaxGeneration();
      } else {
        setMaxGeneration((java.lang.Long)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case VOLUME_ID:
      return getVolumeId();

    case BLOCK_ID:
      return getBlockId();

    case UUID:
      return getUuid();

    case MIN_GENERATION:
      return getMinGeneration();

    case MAX_GENERATION:
      return getMaxGeneration();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case VOLUME_ID:
      return isSetVolumeId();
    case BLOCK_ID:
      return isSetBlockId();
    case UUID:
      return isSetUuid();
    case MIN_GENERATION:
      return isSetMinGeneration();
    case MAX_GENERATION:
      return isSetMaxGeneration();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that == null)
      return false;
    if (that instanceof JournalRange)
      return this.equals((JournalRange)that);
    return false;
  }

  public boolean equals(JournalRange that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_volumeId = true;
    boolean that_present_volumeId = true;
    if (this_present_volumeId || that_present_volumeId) {
      if (!(this_present_volumeId && that_present_volumeId))
        return false;
      if (this.volumeId != that.volumeId)
        return false;
    }

    boolean this_present_blockId = true;
    boolean that_present_blockId = true;
    if (this_present_blockId || that_present_blockId) {
      if (!(this_present_blockId && that_present_blockId))
        return false;
      if (this.blockId != that.blockId)
        return false;
    }

    boolean this_present_uuid = true && this.isSetUuid();
    boolean that_present_uuid = true && that.isSetUuid();
    if (this_present_uuid || that_present_uuid) {
      if (!(this_present_uuid && that_present_uuid))
        return false;
      if (!this.uuid.equals(that.uuid))
        return false;
    }

    boolean this_present_minGeneration = true;
    boolean that_present_minGeneration = true;
    if (this_present_minGeneration || that_present_minGeneration) {
      if (!(this_present_minGeneration && that_present_minGeneration))
        return false;
      if (this.minGeneration != that.minGeneration)
        return false;
    }

    boolean this_present_maxGeneration = true;
    boolean that_present_maxGeneration = true;
    if (this_present_maxGeneration || that_present_maxGeneration) {
      if (!(this_present_maxGeneration && that_present_maxGeneration))
        return false;
      if (this.maxGeneration != that.maxGeneration)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + org.apache.thrift.TBaseHelper.hashCode(volumeId);

    hashCode = hashCode * 8191 + org.apache.thrift.TBaseHelper.hashCode(blockId);

    hashCode = hashCode * 8191 + ((isSetUuid()) ? 131071 : 524287);
    if (isSetUuid())
      hashCode = hashCode * 8191 + uuid.hashCode();

    hashCode = hashCode * 8191 + org.apache.thrift.TBaseHelper.hashCode(minGeneration);

    hashCode = hashCode * 8191 + org.apache.thrift.TBaseHelper.hashCode(maxGeneration);

    return hashCode;
  }

  @Override
  public int compareTo(JournalRange other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.valueOf(isSetVolumeId()).compareTo(other.isSetVolumeId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetVolumeId()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.volumeId, other.volumeId);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetBlockId()).compareTo(other.isSetBlockId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetBlockId()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.blockId, other.blockId);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetUuid()).compareTo(other.isSetUuid());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetUuid()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.uuid, other.uuid);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetMinGeneration()).compareTo(other.isSetMinGeneration());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetMinGeneration()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.minGeneration, other.minGeneration);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetMaxGeneration()).compareTo(other.isSetMaxGeneration());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetMaxGeneration()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.maxGeneration, other.maxGeneration);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  @org.apache.thrift.annotation.Nullable
  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    scheme(iprot).read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    scheme(oprot).write(oprot, this);
  }

  @Override
  public java.lang.String toString() {
    java.lang.StringBuilder sb = new java.lang.StringBuilder("JournalRange(");
    boolean first = true;

    sb.append("volumeId:");
    sb.append(this.volumeId);
    first = false;
    if (!first) sb.append(", ");
    sb.append("blockId:");
    sb.append(this.blockId);
    first = false;
    if (!first) sb.append(", ");
    sb.append("uuid:");
    if (this.uuid == null) {
      sb.append("null");
    } else {
      sb.append(this.uuid);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("minGeneration:");
    sb.append(this.minGeneration);
    first = false;
    if (!first) sb.append(", ");
    sb.append("maxGeneration:");
    sb.append(this.maxGeneration);
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, java.lang.ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class JournalRangeStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public JournalRangeStandardScheme getScheme() {
      return new JournalRangeStandardScheme();
    }
  }

  private static class JournalRangeStandardScheme extends org.apache.thrift.scheme.StandardScheme<JournalRange> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, JournalRange struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // VOLUME_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.volumeId = iprot.readI64();
              struct.setVolumeIdIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // BLOCK_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.blockId = iprot.readI64();
              struct.setBlockIdIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // UUID
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.uuid = iprot.readString();
              struct.setUuidIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // MIN_GENERATION
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.minGeneration = iprot.readI64();
              struct.setMinGenerationIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // MAX_GENERATION
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.maxGeneration = iprot.readI64();
              struct.setMaxGenerationIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, JournalRange struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(VOLUME_ID_FIELD_DESC);
      oprot.writeI64(struct.volumeId);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(BLOCK_ID_FIELD_DESC);
      oprot.writeI64(struct.blockId);
      oprot.writeFieldEnd();
      if (struct.uuid != null) {
        oprot.writeFieldBegin(UUID_FIELD_DESC);
        oprot.writeString(struct.uuid);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(MIN_GENERATION_FIELD_DESC);
      oprot.writeI64(struct.minGeneration);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(MAX_GENERATION_FIELD_DESC);
      oprot.writeI64(struct.maxGeneration);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class JournalRangeTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public JournalRangeTupleScheme getScheme() {
      return new JournalRangeTupleScheme();
    }
  }

  private static class JournalRangeTupleScheme extends org.apache.thrift.scheme.TupleScheme<JournalRange> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, JournalRange struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetVolumeId()) {
        optionals.set(0);
      }
      if (struct.isSetBlockId()) {
        optionals.set(1);
      }
      if (struct.isSetUuid()) {
        optionals.set(2);
      }
      if (struct.isSetMinGeneration()) {
        optionals.set(3);
      }
      if (struct.isSetMaxGeneration()) {
        optionals.set(4);
      }
      oprot.writeBitSet(optionals, 5);
      if (struct.isSetVolumeId()) {
        oprot.writeI64(struct.volumeId);
      }
      if (struct.isSetBlockId()) {
        oprot.writeI64(struct.blockId);
      }
      if (struct.isSetUuid()) {
        oprot.writeString(struct.uuid);
      }
      if (struct.isSetMinGeneration()) {
        oprot.writeI64(struct.minGeneration);
      }
      if (struct.isSetMaxGeneration()) {
        oprot.writeI64(struct.maxGeneration);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, JournalRange struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet incoming = iprot.readBitSet(5);
      if (incoming.get(0)) {
        struct.volumeId = iprot.readI64();
        struct.setVolumeIdIsSet(true);
      }
      if (incoming.get(1)) {
        struct.blockId = iprot.readI64();
        struct.setBlockIdIsSet(true);
      }
      if (incoming.get(2)) {
        struct.uuid = iprot.readString();
        struct.setUuidIsSet(true);
      }
      if (incoming.get(3)) {
        struct.minGeneration = iprot.readI64();
        struct.setMinGenerationIsSet(true);
      }
      if (incoming.get(4)) {
        struct.maxGeneration = iprot.readI64();
        struct.setMaxGenerationIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

