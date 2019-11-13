/**
 * Autogenerated by Thrift Compiler (0.12.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package pack.iscsi.brick.remote.generated;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.12.0)", date = "2019-11-12")
public class RemoveRequest implements org.apache.thrift.TBase<RemoveRequest, RemoveRequest._Fields>, java.io.Serializable, Cloneable, Comparable<RemoveRequest> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("RemoveRequest");

  private static final org.apache.thrift.protocol.TField BRICK_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("brickId", org.apache.thrift.protocol.TType.I64, (short)1);
  private static final org.apache.thrift.protocol.TField BLOCK_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("blockId", org.apache.thrift.protocol.TType.I64, (short)2);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new RemoveRequestStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new RemoveRequestTupleSchemeFactory();

  public long brickId; // required
  public long blockId; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    BRICK_ID((short)1, "brickId"),
    BLOCK_ID((short)2, "blockId");

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
        case 2: // BLOCK_ID
          return BLOCK_ID;
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
  private static final int __BLOCKID_ISSET_ID = 1;
  private byte __isset_bitfield = 0;
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.BRICK_ID, new org.apache.thrift.meta_data.FieldMetaData("brickId", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64        , "long")));
    tmpMap.put(_Fields.BLOCK_ID, new org.apache.thrift.meta_data.FieldMetaData("blockId", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64        , "long")));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(RemoveRequest.class, metaDataMap);
  }

  public RemoveRequest() {
  }

  public RemoveRequest(
    long brickId,
    long blockId)
  {
    this();
    this.brickId = brickId;
    setBrickIdIsSet(true);
    this.blockId = blockId;
    setBlockIdIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public RemoveRequest(RemoveRequest other) {
    __isset_bitfield = other.__isset_bitfield;
    this.brickId = other.brickId;
    this.blockId = other.blockId;
  }

  public RemoveRequest deepCopy() {
    return new RemoveRequest(this);
  }

  @Override
  public void clear() {
    setBrickIdIsSet(false);
    this.brickId = 0;
    setBlockIdIsSet(false);
    this.blockId = 0;
  }

  public long getBrickId() {
    return this.brickId;
  }

  public RemoveRequest setBrickId(long brickId) {
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

  public long getBlockId() {
    return this.blockId;
  }

  public RemoveRequest setBlockId(long blockId) {
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

  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case BRICK_ID:
      if (value == null) {
        unsetBrickId();
      } else {
        setBrickId((java.lang.Long)value);
      }
      break;

    case BLOCK_ID:
      if (value == null) {
        unsetBlockId();
      } else {
        setBlockId((java.lang.Long)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case BRICK_ID:
      return getBrickId();

    case BLOCK_ID:
      return getBlockId();

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
    case BLOCK_ID:
      return isSetBlockId();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that == null)
      return false;
    if (that instanceof RemoveRequest)
      return this.equals((RemoveRequest)that);
    return false;
  }

  public boolean equals(RemoveRequest that) {
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

    boolean this_present_blockId = true;
    boolean that_present_blockId = true;
    if (this_present_blockId || that_present_blockId) {
      if (!(this_present_blockId && that_present_blockId))
        return false;
      if (this.blockId != that.blockId)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + org.apache.thrift.TBaseHelper.hashCode(brickId);

    hashCode = hashCode * 8191 + org.apache.thrift.TBaseHelper.hashCode(blockId);

    return hashCode;
  }

  @Override
  public int compareTo(RemoveRequest other) {
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
    java.lang.StringBuilder sb = new java.lang.StringBuilder("RemoveRequest(");
    boolean first = true;

    sb.append("brickId:");
    sb.append(this.brickId);
    first = false;
    if (!first) sb.append(", ");
    sb.append("blockId:");
    sb.append(this.blockId);
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

  private static class RemoveRequestStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public RemoveRequestStandardScheme getScheme() {
      return new RemoveRequestStandardScheme();
    }
  }

  private static class RemoveRequestStandardScheme extends org.apache.thrift.scheme.StandardScheme<RemoveRequest> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, RemoveRequest struct) throws org.apache.thrift.TException {
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
          case 2: // BLOCK_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.blockId = iprot.readI64();
              struct.setBlockIdIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, RemoveRequest struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(BRICK_ID_FIELD_DESC);
      oprot.writeI64(struct.brickId);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(BLOCK_ID_FIELD_DESC);
      oprot.writeI64(struct.blockId);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class RemoveRequestTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public RemoveRequestTupleScheme getScheme() {
      return new RemoveRequestTupleScheme();
    }
  }

  private static class RemoveRequestTupleScheme extends org.apache.thrift.scheme.TupleScheme<RemoveRequest> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, RemoveRequest struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetBrickId()) {
        optionals.set(0);
      }
      if (struct.isSetBlockId()) {
        optionals.set(1);
      }
      oprot.writeBitSet(optionals, 2);
      if (struct.isSetBrickId()) {
        oprot.writeI64(struct.brickId);
      }
      if (struct.isSetBlockId()) {
        oprot.writeI64(struct.blockId);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, RemoveRequest struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet incoming = iprot.readBitSet(2);
      if (incoming.get(0)) {
        struct.brickId = iprot.readI64();
        struct.setBrickIdIsSet(true);
      }
      if (incoming.get(1)) {
        struct.blockId = iprot.readI64();
        struct.setBlockIdIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

