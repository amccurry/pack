/**
 * Autogenerated by Thrift Compiler (0.12.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package pack.iscsi.brick.remote.generated;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.12.0)", date = "2019-11-18")
public class WriteRequest implements org.apache.thrift.TBase<WriteRequest, WriteRequest._Fields>, java.io.Serializable, Cloneable, Comparable<WriteRequest> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("WriteRequest");

  private static final org.apache.thrift.protocol.TField BRICK_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("brickId", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField POSITION_FIELD_DESC = new org.apache.thrift.protocol.TField("position", org.apache.thrift.protocol.TType.I64, (short)2);
  private static final org.apache.thrift.protocol.TField DATA_FIELD_DESC = new org.apache.thrift.protocol.TField("data", org.apache.thrift.protocol.TType.STRING, (short)3);
  private static final org.apache.thrift.protocol.TField INITIALIZE_FIELD_DESC = new org.apache.thrift.protocol.TField("initialize", org.apache.thrift.protocol.TType.BOOL, (short)4);
  private static final org.apache.thrift.protocol.TField GENERATION_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("generationId", org.apache.thrift.protocol.TType.I64, (short)5);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new WriteRequestStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new WriteRequestTupleSchemeFactory();

  public @org.apache.thrift.annotation.Nullable java.lang.String brickId; // required
  public long position; // required
  public @org.apache.thrift.annotation.Nullable java.nio.ByteBuffer data; // required
  public boolean initialize; // required
  public long generationId; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    BRICK_ID((short)1, "brickId"),
    POSITION((short)2, "position"),
    DATA((short)3, "data"),
    INITIALIZE((short)4, "initialize"),
    GENERATION_ID((short)5, "generationId");

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
        case 1: // BRICK_ID
          return BRICK_ID;
        case 2: // POSITION
          return POSITION;
        case 3: // DATA
          return DATA;
        case 4: // INITIALIZE
          return INITIALIZE;
        case 5: // GENERATION_ID
          return GENERATION_ID;
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
  private static final int __POSITION_ISSET_ID = 0;
  private static final int __INITIALIZE_ISSET_ID = 1;
  private static final int __GENERATIONID_ISSET_ID = 2;
  private byte __isset_bitfield = 0;
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.BRICK_ID, new org.apache.thrift.meta_data.FieldMetaData("brickId", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.POSITION, new org.apache.thrift.meta_data.FieldMetaData("position", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64        , "long")));
    tmpMap.put(_Fields.DATA, new org.apache.thrift.meta_data.FieldMetaData("data", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING        , true)));
    tmpMap.put(_Fields.INITIALIZE, new org.apache.thrift.meta_data.FieldMetaData("initialize", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.BOOL)));
    tmpMap.put(_Fields.GENERATION_ID, new org.apache.thrift.meta_data.FieldMetaData("generationId", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64        , "long")));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(WriteRequest.class, metaDataMap);
  }

  public WriteRequest() {
  }

  public WriteRequest(
    java.lang.String brickId,
    long position,
    java.nio.ByteBuffer data,
    boolean initialize,
    long generationId)
  {
    this();
    this.brickId = brickId;
    this.position = position;
    setPositionIsSet(true);
    this.data = org.apache.thrift.TBaseHelper.copyBinary(data);
    this.initialize = initialize;
    setInitializeIsSet(true);
    this.generationId = generationId;
    setGenerationIdIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public WriteRequest(WriteRequest other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.isSetBrickId()) {
      this.brickId = other.brickId;
    }
    this.position = other.position;
    if (other.isSetData()) {
      this.data = org.apache.thrift.TBaseHelper.copyBinary(other.data);
    }
    this.initialize = other.initialize;
    this.generationId = other.generationId;
  }

  public WriteRequest deepCopy() {
    return new WriteRequest(this);
  }

  @Override
  public void clear() {
    this.brickId = null;
    setPositionIsSet(false);
    this.position = 0;
    this.data = null;
    setInitializeIsSet(false);
    this.initialize = false;
    setGenerationIdIsSet(false);
    this.generationId = 0;
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.String getBrickId() {
    return this.brickId;
  }

  public WriteRequest setBrickId(@org.apache.thrift.annotation.Nullable java.lang.String brickId) {
    this.brickId = brickId;
    return this;
  }

  public void unsetBrickId() {
    this.brickId = null;
  }

  /** Returns true if field brickId is set (has been assigned a value) and false otherwise */
  public boolean isSetBrickId() {
    return this.brickId != null;
  }

  public void setBrickIdIsSet(boolean value) {
    if (!value) {
      this.brickId = null;
    }
  }

  public long getPosition() {
    return this.position;
  }

  public WriteRequest setPosition(long position) {
    this.position = position;
    setPositionIsSet(true);
    return this;
  }

  public void unsetPosition() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __POSITION_ISSET_ID);
  }

  /** Returns true if field position is set (has been assigned a value) and false otherwise */
  public boolean isSetPosition() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __POSITION_ISSET_ID);
  }

  public void setPositionIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __POSITION_ISSET_ID, value);
  }

  public byte[] getData() {
    setData(org.apache.thrift.TBaseHelper.rightSize(data));
    return data == null ? null : data.array();
  }

  public java.nio.ByteBuffer bufferForData() {
    return org.apache.thrift.TBaseHelper.copyBinary(data);
  }

  public WriteRequest setData(byte[] data) {
    this.data = data == null ? (java.nio.ByteBuffer)null   : java.nio.ByteBuffer.wrap(data.clone());
    return this;
  }

  public WriteRequest setData(@org.apache.thrift.annotation.Nullable java.nio.ByteBuffer data) {
    this.data = org.apache.thrift.TBaseHelper.copyBinary(data);
    return this;
  }

  public void unsetData() {
    this.data = null;
  }

  /** Returns true if field data is set (has been assigned a value) and false otherwise */
  public boolean isSetData() {
    return this.data != null;
  }

  public void setDataIsSet(boolean value) {
    if (!value) {
      this.data = null;
    }
  }

  public boolean isInitialize() {
    return this.initialize;
  }

  public WriteRequest setInitialize(boolean initialize) {
    this.initialize = initialize;
    setInitializeIsSet(true);
    return this;
  }

  public void unsetInitialize() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __INITIALIZE_ISSET_ID);
  }

  /** Returns true if field initialize is set (has been assigned a value) and false otherwise */
  public boolean isSetInitialize() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __INITIALIZE_ISSET_ID);
  }

  public void setInitializeIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __INITIALIZE_ISSET_ID, value);
  }

  public long getGenerationId() {
    return this.generationId;
  }

  public WriteRequest setGenerationId(long generationId) {
    this.generationId = generationId;
    setGenerationIdIsSet(true);
    return this;
  }

  public void unsetGenerationId() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __GENERATIONID_ISSET_ID);
  }

  /** Returns true if field generationId is set (has been assigned a value) and false otherwise */
  public boolean isSetGenerationId() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __GENERATIONID_ISSET_ID);
  }

  public void setGenerationIdIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __GENERATIONID_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case BRICK_ID:
      if (value == null) {
        unsetBrickId();
      } else {
        setBrickId((java.lang.String)value);
      }
      break;

    case POSITION:
      if (value == null) {
        unsetPosition();
      } else {
        setPosition((java.lang.Long)value);
      }
      break;

    case DATA:
      if (value == null) {
        unsetData();
      } else {
        if (value instanceof byte[]) {
          setData((byte[])value);
        } else {
          setData((java.nio.ByteBuffer)value);
        }
      }
      break;

    case INITIALIZE:
      if (value == null) {
        unsetInitialize();
      } else {
        setInitialize((java.lang.Boolean)value);
      }
      break;

    case GENERATION_ID:
      if (value == null) {
        unsetGenerationId();
      } else {
        setGenerationId((java.lang.Long)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case BRICK_ID:
      return getBrickId();

    case POSITION:
      return getPosition();

    case DATA:
      return getData();

    case INITIALIZE:
      return isInitialize();

    case GENERATION_ID:
      return getGenerationId();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case BRICK_ID:
      return isSetBrickId();
    case POSITION:
      return isSetPosition();
    case DATA:
      return isSetData();
    case INITIALIZE:
      return isSetInitialize();
    case GENERATION_ID:
      return isSetGenerationId();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that == null)
      return false;
    if (that instanceof WriteRequest)
      return this.equals((WriteRequest)that);
    return false;
  }

  public boolean equals(WriteRequest that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_brickId = true && this.isSetBrickId();
    boolean that_present_brickId = true && that.isSetBrickId();
    if (this_present_brickId || that_present_brickId) {
      if (!(this_present_brickId && that_present_brickId))
        return false;
      if (!this.brickId.equals(that.brickId))
        return false;
    }

    boolean this_present_position = true;
    boolean that_present_position = true;
    if (this_present_position || that_present_position) {
      if (!(this_present_position && that_present_position))
        return false;
      if (this.position != that.position)
        return false;
    }

    boolean this_present_data = true && this.isSetData();
    boolean that_present_data = true && that.isSetData();
    if (this_present_data || that_present_data) {
      if (!(this_present_data && that_present_data))
        return false;
      if (!this.data.equals(that.data))
        return false;
    }

    boolean this_present_initialize = true;
    boolean that_present_initialize = true;
    if (this_present_initialize || that_present_initialize) {
      if (!(this_present_initialize && that_present_initialize))
        return false;
      if (this.initialize != that.initialize)
        return false;
    }

    boolean this_present_generationId = true;
    boolean that_present_generationId = true;
    if (this_present_generationId || that_present_generationId) {
      if (!(this_present_generationId && that_present_generationId))
        return false;
      if (this.generationId != that.generationId)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isSetBrickId()) ? 131071 : 524287);
    if (isSetBrickId())
      hashCode = hashCode * 8191 + brickId.hashCode();

    hashCode = hashCode * 8191 + org.apache.thrift.TBaseHelper.hashCode(position);

    hashCode = hashCode * 8191 + ((isSetData()) ? 131071 : 524287);
    if (isSetData())
      hashCode = hashCode * 8191 + data.hashCode();

    hashCode = hashCode * 8191 + ((initialize) ? 131071 : 524287);

    hashCode = hashCode * 8191 + org.apache.thrift.TBaseHelper.hashCode(generationId);

    return hashCode;
  }

  @Override
  public int compareTo(WriteRequest other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.valueOf(isSetBrickId()).compareTo(other.isSetBrickId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetBrickId()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.brickId, other.brickId);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetPosition()).compareTo(other.isSetPosition());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetPosition()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.position, other.position);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetData()).compareTo(other.isSetData());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetData()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.data, other.data);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetInitialize()).compareTo(other.isSetInitialize());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetInitialize()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.initialize, other.initialize);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetGenerationId()).compareTo(other.isSetGenerationId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetGenerationId()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.generationId, other.generationId);
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
    java.lang.StringBuilder sb = new java.lang.StringBuilder("WriteRequest(");
    boolean first = true;

    sb.append("brickId:");
    if (this.brickId == null) {
      sb.append("null");
    } else {
      sb.append(this.brickId);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("position:");
    sb.append(this.position);
    first = false;
    if (!first) sb.append(", ");
    sb.append("data:");
    if (this.data == null) {
      sb.append("null");
    } else {
      org.apache.thrift.TBaseHelper.toString(this.data, sb);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("initialize:");
    sb.append(this.initialize);
    first = false;
    if (!first) sb.append(", ");
    sb.append("generationId:");
    sb.append(this.generationId);
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

  private static class WriteRequestStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public WriteRequestStandardScheme getScheme() {
      return new WriteRequestStandardScheme();
    }
  }

  private static class WriteRequestStandardScheme extends org.apache.thrift.scheme.StandardScheme<WriteRequest> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, WriteRequest struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // BRICK_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.brickId = iprot.readString();
              struct.setBrickIdIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // POSITION
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.position = iprot.readI64();
              struct.setPositionIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // DATA
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.data = iprot.readBinary();
              struct.setDataIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // INITIALIZE
            if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
              struct.initialize = iprot.readBool();
              struct.setInitializeIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // GENERATION_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.generationId = iprot.readI64();
              struct.setGenerationIdIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, WriteRequest struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.brickId != null) {
        oprot.writeFieldBegin(BRICK_ID_FIELD_DESC);
        oprot.writeString(struct.brickId);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(POSITION_FIELD_DESC);
      oprot.writeI64(struct.position);
      oprot.writeFieldEnd();
      if (struct.data != null) {
        oprot.writeFieldBegin(DATA_FIELD_DESC);
        oprot.writeBinary(struct.data);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(INITIALIZE_FIELD_DESC);
      oprot.writeBool(struct.initialize);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(GENERATION_ID_FIELD_DESC);
      oprot.writeI64(struct.generationId);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class WriteRequestTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public WriteRequestTupleScheme getScheme() {
      return new WriteRequestTupleScheme();
    }
  }

  private static class WriteRequestTupleScheme extends org.apache.thrift.scheme.TupleScheme<WriteRequest> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, WriteRequest struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetBrickId()) {
        optionals.set(0);
      }
      if (struct.isSetPosition()) {
        optionals.set(1);
      }
      if (struct.isSetData()) {
        optionals.set(2);
      }
      if (struct.isSetInitialize()) {
        optionals.set(3);
      }
      if (struct.isSetGenerationId()) {
        optionals.set(4);
      }
      oprot.writeBitSet(optionals, 5);
      if (struct.isSetBrickId()) {
        oprot.writeString(struct.brickId);
      }
      if (struct.isSetPosition()) {
        oprot.writeI64(struct.position);
      }
      if (struct.isSetData()) {
        oprot.writeBinary(struct.data);
      }
      if (struct.isSetInitialize()) {
        oprot.writeBool(struct.initialize);
      }
      if (struct.isSetGenerationId()) {
        oprot.writeI64(struct.generationId);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, WriteRequest struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet incoming = iprot.readBitSet(5);
      if (incoming.get(0)) {
        struct.brickId = iprot.readString();
        struct.setBrickIdIsSet(true);
      }
      if (incoming.get(1)) {
        struct.position = iprot.readI64();
        struct.setPositionIsSet(true);
      }
      if (incoming.get(2)) {
        struct.data = iprot.readBinary();
        struct.setDataIsSet(true);
      }
      if (incoming.get(3)) {
        struct.initialize = iprot.readBool();
        struct.setInitializeIsSet(true);
      }
      if (incoming.get(4)) {
        struct.generationId = iprot.readI64();
        struct.setGenerationIdIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

