/**
 * Autogenerated by Thrift Compiler (0.12.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package pack.iscsi.wal.remote.generated;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.12.0)", date = "2019-09-17")
public class ReleaseRequest implements org.apache.thrift.TBase<ReleaseRequest, ReleaseRequest._Fields>, java.io.Serializable, Cloneable, Comparable<ReleaseRequest> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("ReleaseRequest");

  private static final org.apache.thrift.protocol.TField VOLUME_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("volumeId", org.apache.thrift.protocol.TType.I64, (short)1);
  private static final org.apache.thrift.protocol.TField BLOCK_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("blockId", org.apache.thrift.protocol.TType.I64, (short)2);
  private static final org.apache.thrift.protocol.TField GENERATION_FIELD_DESC = new org.apache.thrift.protocol.TField("generation", org.apache.thrift.protocol.TType.I64, (short)3);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new ReleaseRequestStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new ReleaseRequestTupleSchemeFactory();

  public long volumeId; // required
  public long blockId; // required
  public long generation; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    VOLUME_ID((short)1, "volumeId"),
    BLOCK_ID((short)2, "blockId"),
    GENERATION((short)3, "generation");

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
        case 3: // GENERATION
          return GENERATION;
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
  private static final int __GENERATION_ISSET_ID = 2;
  private byte __isset_bitfield = 0;
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.VOLUME_ID, new org.apache.thrift.meta_data.FieldMetaData("volumeId", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64        , "long")));
    tmpMap.put(_Fields.BLOCK_ID, new org.apache.thrift.meta_data.FieldMetaData("blockId", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64        , "long")));
    tmpMap.put(_Fields.GENERATION, new org.apache.thrift.meta_data.FieldMetaData("generation", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64        , "long")));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(ReleaseRequest.class, metaDataMap);
  }

  public ReleaseRequest() {
  }

  public ReleaseRequest(
    long volumeId,
    long blockId,
    long generation)
  {
    this();
    this.volumeId = volumeId;
    setVolumeIdIsSet(true);
    this.blockId = blockId;
    setBlockIdIsSet(true);
    this.generation = generation;
    setGenerationIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public ReleaseRequest(ReleaseRequest other) {
    __isset_bitfield = other.__isset_bitfield;
    this.volumeId = other.volumeId;
    this.blockId = other.blockId;
    this.generation = other.generation;
  }

  public ReleaseRequest deepCopy() {
    return new ReleaseRequest(this);
  }

  @Override
  public void clear() {
    setVolumeIdIsSet(false);
    this.volumeId = 0;
    setBlockIdIsSet(false);
    this.blockId = 0;
    setGenerationIsSet(false);
    this.generation = 0;
  }

  public long getVolumeId() {
    return this.volumeId;
  }

  public ReleaseRequest setVolumeId(long volumeId) {
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

  public ReleaseRequest setBlockId(long blockId) {
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

  public long getGeneration() {
    return this.generation;
  }

  public ReleaseRequest setGeneration(long generation) {
    this.generation = generation;
    setGenerationIsSet(true);
    return this;
  }

  public void unsetGeneration() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __GENERATION_ISSET_ID);
  }

  /** Returns true if field generation is set (has been assigned a value) and false otherwise */
  public boolean isSetGeneration() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __GENERATION_ISSET_ID);
  }

  public void setGenerationIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __GENERATION_ISSET_ID, value);
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

    case GENERATION:
      if (value == null) {
        unsetGeneration();
      } else {
        setGeneration((java.lang.Long)value);
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

    case GENERATION:
      return getGeneration();

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
    case GENERATION:
      return isSetGeneration();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that == null)
      return false;
    if (that instanceof ReleaseRequest)
      return this.equals((ReleaseRequest)that);
    return false;
  }

  public boolean equals(ReleaseRequest that) {
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

    boolean this_present_generation = true;
    boolean that_present_generation = true;
    if (this_present_generation || that_present_generation) {
      if (!(this_present_generation && that_present_generation))
        return false;
      if (this.generation != that.generation)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + org.apache.thrift.TBaseHelper.hashCode(volumeId);

    hashCode = hashCode * 8191 + org.apache.thrift.TBaseHelper.hashCode(blockId);

    hashCode = hashCode * 8191 + org.apache.thrift.TBaseHelper.hashCode(generation);

    return hashCode;
  }

  @Override
  public int compareTo(ReleaseRequest other) {
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
    lastComparison = java.lang.Boolean.valueOf(isSetGeneration()).compareTo(other.isSetGeneration());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetGeneration()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.generation, other.generation);
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
    java.lang.StringBuilder sb = new java.lang.StringBuilder("ReleaseRequest(");
    boolean first = true;

    sb.append("volumeId:");
    sb.append(this.volumeId);
    first = false;
    if (!first) sb.append(", ");
    sb.append("blockId:");
    sb.append(this.blockId);
    first = false;
    if (!first) sb.append(", ");
    sb.append("generation:");
    sb.append(this.generation);
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

  private static class ReleaseRequestStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public ReleaseRequestStandardScheme getScheme() {
      return new ReleaseRequestStandardScheme();
    }
  }

  private static class ReleaseRequestStandardScheme extends org.apache.thrift.scheme.StandardScheme<ReleaseRequest> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, ReleaseRequest struct) throws org.apache.thrift.TException {
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
          case 3: // GENERATION
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.generation = iprot.readI64();
              struct.setGenerationIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, ReleaseRequest struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(VOLUME_ID_FIELD_DESC);
      oprot.writeI64(struct.volumeId);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(BLOCK_ID_FIELD_DESC);
      oprot.writeI64(struct.blockId);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(GENERATION_FIELD_DESC);
      oprot.writeI64(struct.generation);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class ReleaseRequestTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public ReleaseRequestTupleScheme getScheme() {
      return new ReleaseRequestTupleScheme();
    }
  }

  private static class ReleaseRequestTupleScheme extends org.apache.thrift.scheme.TupleScheme<ReleaseRequest> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, ReleaseRequest struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetVolumeId()) {
        optionals.set(0);
      }
      if (struct.isSetBlockId()) {
        optionals.set(1);
      }
      if (struct.isSetGeneration()) {
        optionals.set(2);
      }
      oprot.writeBitSet(optionals, 3);
      if (struct.isSetVolumeId()) {
        oprot.writeI64(struct.volumeId);
      }
      if (struct.isSetBlockId()) {
        oprot.writeI64(struct.blockId);
      }
      if (struct.isSetGeneration()) {
        oprot.writeI64(struct.generation);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, ReleaseRequest struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet incoming = iprot.readBitSet(3);
      if (incoming.get(0)) {
        struct.volumeId = iprot.readI64();
        struct.setVolumeIdIsSet(true);
      }
      if (incoming.get(1)) {
        struct.blockId = iprot.readI64();
        struct.setBlockIdIsSet(true);
      }
      if (incoming.get(2)) {
        struct.generation = iprot.readI64();
        struct.setGenerationIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

