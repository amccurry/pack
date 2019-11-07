/**
 * Autogenerated by Thrift Compiler (0.12.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package pack.iscsi.brick.remote.generated;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.12.0)", date = "2019-10-29")
public class ReadRequest implements org.apache.thrift.TBase<ReadRequest, ReadRequest._Fields>, java.io.Serializable, Cloneable, Comparable<ReadRequest> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("ReadRequest");

  private static final org.apache.thrift.protocol.TField BRICK_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("brickId", org.apache.thrift.protocol.TType.I64, (short)1);
  private static final org.apache.thrift.protocol.TField POSITION_FIELD_DESC = new org.apache.thrift.protocol.TField("position", org.apache.thrift.protocol.TType.I64, (short)2);
  private static final org.apache.thrift.protocol.TField LENGTH_FIELD_DESC = new org.apache.thrift.protocol.TField("length", org.apache.thrift.protocol.TType.I32, (short)3);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new ReadRequestStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new ReadRequestTupleSchemeFactory();

  public long brickId; // required
  public long position; // required
  public int length; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    BRICK_ID((short)1, "brickId"),
    POSITION((short)2, "position"),
    LENGTH((short)3, "length");

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
        case 3: // LENGTH
          return LENGTH;
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
  private static final int __BRICKID_ISSET_ID = 0;
  private static final int __POSITION_ISSET_ID = 1;
  private static final int __LENGTH_ISSET_ID = 2;
  private byte __isset_bitfield = 0;
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.BRICK_ID, new org.apache.thrift.meta_data.FieldMetaData("brickId", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64        , "long")));
    tmpMap.put(_Fields.POSITION, new org.apache.thrift.meta_data.FieldMetaData("position", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64        , "long")));
    tmpMap.put(_Fields.LENGTH, new org.apache.thrift.meta_data.FieldMetaData("length", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32        , "int")));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(ReadRequest.class, metaDataMap);
  }

  public ReadRequest() {
  }

  public ReadRequest(
    long brickId,
    long position,
    int length)
  {
    this();
    this.brickId = brickId;
    setBrickIdIsSet(true);
    this.position = position;
    setPositionIsSet(true);
    this.length = length;
    setLengthIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public ReadRequest(ReadRequest other) {
    __isset_bitfield = other.__isset_bitfield;
    this.brickId = other.brickId;
    this.position = other.position;
    this.length = other.length;
  }

  public ReadRequest deepCopy() {
    return new ReadRequest(this);
  }

  @Override
  public void clear() {
    setBrickIdIsSet(false);
    this.brickId = 0;
    setPositionIsSet(false);
    this.position = 0;
    setLengthIsSet(false);
    this.length = 0;
  }

  public long getBrickId() {
    return this.brickId;
  }

  public ReadRequest setBrickId(long brickId) {
    this.brickId = brickId;
    setBrickIdIsSet(true);
    return this;
  }

  public void unsetBrickId() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __BRICKID_ISSET_ID);
  }

  /** Returns true if field brickId is set (has been assigned a value) and false otherwise */
  public boolean isSetBrickId() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __BRICKID_ISSET_ID);
  }

  public void setBrickIdIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __BRICKID_ISSET_ID, value);
  }

  public long getPosition() {
    return this.position;
  }

  public ReadRequest setPosition(long position) {
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

  public int getLength() {
    return this.length;
  }

  public ReadRequest setLength(int length) {
    this.length = length;
    setLengthIsSet(true);
    return this;
  }

  public void unsetLength() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __LENGTH_ISSET_ID);
  }

  /** Returns true if field length is set (has been assigned a value) and false otherwise */
  public boolean isSetLength() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __LENGTH_ISSET_ID);
  }

  public void setLengthIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __LENGTH_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case BRICK_ID:
      if (value == null) {
        unsetBrickId();
      } else {
        setBrickId((java.lang.Long)value);
      }
      break;

    case POSITION:
      if (value == null) {
        unsetPosition();
      } else {
        setPosition((java.lang.Long)value);
      }
      break;

    case LENGTH:
      if (value == null) {
        unsetLength();
      } else {
        setLength((java.lang.Integer)value);
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

    case LENGTH:
      return getLength();

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
    case LENGTH:
      return isSetLength();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that == null)
      return false;
    if (that instanceof ReadRequest)
      return this.equals((ReadRequest)that);
    return false;
  }

  public boolean equals(ReadRequest that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_brickId = true;
    boolean that_present_brickId = true;
    if (this_present_brickId || that_present_brickId) {
      if (!(this_present_brickId && that_present_brickId))
        return false;
      if (this.brickId != that.brickId)
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

    boolean this_present_length = true;
    boolean that_present_length = true;
    if (this_present_length || that_present_length) {
      if (!(this_present_length && that_present_length))
        return false;
      if (this.length != that.length)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + org.apache.thrift.TBaseHelper.hashCode(brickId);

    hashCode = hashCode * 8191 + org.apache.thrift.TBaseHelper.hashCode(position);

    hashCode = hashCode * 8191 + length;

    return hashCode;
  }

  @Override
  public int compareTo(ReadRequest other) {
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
    lastComparison = java.lang.Boolean.valueOf(isSetLength()).compareTo(other.isSetLength());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetLength()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.length, other.length);
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
    java.lang.StringBuilder sb = new java.lang.StringBuilder("ReadRequest(");
    boolean first = true;

    sb.append("brickId:");
    sb.append(this.brickId);
    first = false;
    if (!first) sb.append(", ");
    sb.append("position:");
    sb.append(this.position);
    first = false;
    if (!first) sb.append(", ");
    sb.append("length:");
    sb.append(this.length);
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

  private static class ReadRequestStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public ReadRequestStandardScheme getScheme() {
      return new ReadRequestStandardScheme();
    }
  }

  private static class ReadRequestStandardScheme extends org.apache.thrift.scheme.StandardScheme<ReadRequest> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, ReadRequest struct) throws org.apache.thrift.TException {
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
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.brickId = iprot.readI64();
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
          case 3: // LENGTH
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.length = iprot.readI32();
              struct.setLengthIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, ReadRequest struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(BRICK_ID_FIELD_DESC);
      oprot.writeI64(struct.brickId);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(POSITION_FIELD_DESC);
      oprot.writeI64(struct.position);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(LENGTH_FIELD_DESC);
      oprot.writeI32(struct.length);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class ReadRequestTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public ReadRequestTupleScheme getScheme() {
      return new ReadRequestTupleScheme();
    }
  }

  private static class ReadRequestTupleScheme extends org.apache.thrift.scheme.TupleScheme<ReadRequest> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, ReadRequest struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetBrickId()) {
        optionals.set(0);
      }
      if (struct.isSetPosition()) {
        optionals.set(1);
      }
      if (struct.isSetLength()) {
        optionals.set(2);
      }
      oprot.writeBitSet(optionals, 3);
      if (struct.isSetBrickId()) {
        oprot.writeI64(struct.brickId);
      }
      if (struct.isSetPosition()) {
        oprot.writeI64(struct.position);
      }
      if (struct.isSetLength()) {
        oprot.writeI32(struct.length);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, ReadRequest struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet incoming = iprot.readBitSet(3);
      if (incoming.get(0)) {
        struct.brickId = iprot.readI64();
        struct.setBrickIdIsSet(true);
      }
      if (incoming.get(1)) {
        struct.position = iprot.readI64();
        struct.setPositionIsSet(true);
      }
      if (incoming.get(2)) {
        struct.length = iprot.readI32();
        struct.setLengthIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

