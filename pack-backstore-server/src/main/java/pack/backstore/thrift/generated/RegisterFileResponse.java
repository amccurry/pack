/**
 * Autogenerated by Thrift Compiler (0.12.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package pack.backstore.thrift.generated;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.12.0)", date = "2019-12-15")
public class RegisterFileResponse implements org.apache.thrift.TBase<RegisterFileResponse, RegisterFileResponse._Fields>, java.io.Serializable, Cloneable, Comparable<RegisterFileResponse> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("RegisterFileResponse");

  private static final org.apache.thrift.protocol.TField LOCK_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("lockId", org.apache.thrift.protocol.TType.STRING, (short)1);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new RegisterFileResponseStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new RegisterFileResponseTupleSchemeFactory();

  public @org.apache.thrift.annotation.Nullable java.lang.String lockId; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    LOCK_ID((short)1, "lockId");

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
        case 1: // LOCK_ID
          return LOCK_ID;
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
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.LOCK_ID, new org.apache.thrift.meta_data.FieldMetaData("lockId", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(RegisterFileResponse.class, metaDataMap);
  }

  public RegisterFileResponse() {
  }

  public RegisterFileResponse(
    java.lang.String lockId)
  {
    this();
    this.lockId = lockId;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public RegisterFileResponse(RegisterFileResponse other) {
    if (other.isSetLockId()) {
      this.lockId = other.lockId;
    }
  }

  public RegisterFileResponse deepCopy() {
    return new RegisterFileResponse(this);
  }

  @Override
  public void clear() {
    this.lockId = null;
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.String getLockId() {
    return this.lockId;
  }

  public RegisterFileResponse setLockId(@org.apache.thrift.annotation.Nullable java.lang.String lockId) {
    this.lockId = lockId;
    return this;
  }

  public void unsetLockId() {
    this.lockId = null;
  }

  /** Returns true if field lockId is set (has been assigned a value) and false otherwise */
  public boolean isSetLockId() {
    return this.lockId != null;
  }

  public void setLockIdIsSet(boolean value) {
    if (!value) {
      this.lockId = null;
    }
  }

  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case LOCK_ID:
      if (value == null) {
        unsetLockId();
      } else {
        setLockId((java.lang.String)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case LOCK_ID:
      return getLockId();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case LOCK_ID:
      return isSetLockId();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that == null)
      return false;
    if (that instanceof RegisterFileResponse)
      return this.equals((RegisterFileResponse)that);
    return false;
  }

  public boolean equals(RegisterFileResponse that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_lockId = true && this.isSetLockId();
    boolean that_present_lockId = true && that.isSetLockId();
    if (this_present_lockId || that_present_lockId) {
      if (!(this_present_lockId && that_present_lockId))
        return false;
      if (!this.lockId.equals(that.lockId))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isSetLockId()) ? 131071 : 524287);
    if (isSetLockId())
      hashCode = hashCode * 8191 + lockId.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(RegisterFileResponse other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.valueOf(isSetLockId()).compareTo(other.isSetLockId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetLockId()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.lockId, other.lockId);
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
    java.lang.StringBuilder sb = new java.lang.StringBuilder("RegisterFileResponse(");
    boolean first = true;

    sb.append("lockId:");
    if (this.lockId == null) {
      sb.append("null");
    } else {
      sb.append(this.lockId);
    }
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
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class RegisterFileResponseStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public RegisterFileResponseStandardScheme getScheme() {
      return new RegisterFileResponseStandardScheme();
    }
  }

  private static class RegisterFileResponseStandardScheme extends org.apache.thrift.scheme.StandardScheme<RegisterFileResponse> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, RegisterFileResponse struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // LOCK_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.lockId = iprot.readString();
              struct.setLockIdIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, RegisterFileResponse struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.lockId != null) {
        oprot.writeFieldBegin(LOCK_ID_FIELD_DESC);
        oprot.writeString(struct.lockId);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class RegisterFileResponseTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public RegisterFileResponseTupleScheme getScheme() {
      return new RegisterFileResponseTupleScheme();
    }
  }

  private static class RegisterFileResponseTupleScheme extends org.apache.thrift.scheme.TupleScheme<RegisterFileResponse> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, RegisterFileResponse struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetLockId()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.isSetLockId()) {
        oprot.writeString(struct.lockId);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, RegisterFileResponse struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        struct.lockId = iprot.readString();
        struct.setLockIdIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

