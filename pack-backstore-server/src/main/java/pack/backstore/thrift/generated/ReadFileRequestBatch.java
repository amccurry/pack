/**
 * Autogenerated by Thrift Compiler (0.12.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package pack.backstore.thrift.generated;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.12.0)", date = "2019-12-15")
public class ReadFileRequestBatch implements org.apache.thrift.TBase<ReadFileRequestBatch, ReadFileRequestBatch._Fields>, java.io.Serializable, Cloneable, Comparable<ReadFileRequestBatch> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("ReadFileRequestBatch");

  private static final org.apache.thrift.protocol.TField FILENAME_FIELD_DESC = new org.apache.thrift.protocol.TField("filename", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField READ_REQUESTS_FIELD_DESC = new org.apache.thrift.protocol.TField("readRequests", org.apache.thrift.protocol.TType.LIST, (short)2);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new ReadFileRequestBatchStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new ReadFileRequestBatchTupleSchemeFactory();

  public @org.apache.thrift.annotation.Nullable java.lang.String filename; // required
  public @org.apache.thrift.annotation.Nullable java.util.List<ReadRequest> readRequests; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    FILENAME((short)1, "filename"),
    READ_REQUESTS((short)2, "readRequests");

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
        case 1: // FILENAME
          return FILENAME;
        case 2: // READ_REQUESTS
          return READ_REQUESTS;
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
    tmpMap.put(_Fields.FILENAME, new org.apache.thrift.meta_data.FieldMetaData("filename", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.READ_REQUESTS, new org.apache.thrift.meta_data.FieldMetaData("readRequests", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, ReadRequest.class))));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(ReadFileRequestBatch.class, metaDataMap);
  }

  public ReadFileRequestBatch() {
  }

  public ReadFileRequestBatch(
    java.lang.String filename,
    java.util.List<ReadRequest> readRequests)
  {
    this();
    this.filename = filename;
    this.readRequests = readRequests;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public ReadFileRequestBatch(ReadFileRequestBatch other) {
    if (other.isSetFilename()) {
      this.filename = other.filename;
    }
    if (other.isSetReadRequests()) {
      java.util.List<ReadRequest> __this__readRequests = new java.util.ArrayList<ReadRequest>(other.readRequests.size());
      for (ReadRequest other_element : other.readRequests) {
        __this__readRequests.add(new ReadRequest(other_element));
      }
      this.readRequests = __this__readRequests;
    }
  }

  public ReadFileRequestBatch deepCopy() {
    return new ReadFileRequestBatch(this);
  }

  @Override
  public void clear() {
    this.filename = null;
    this.readRequests = null;
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.String getFilename() {
    return this.filename;
  }

  public ReadFileRequestBatch setFilename(@org.apache.thrift.annotation.Nullable java.lang.String filename) {
    this.filename = filename;
    return this;
  }

  public void unsetFilename() {
    this.filename = null;
  }

  /** Returns true if field filename is set (has been assigned a value) and false otherwise */
  public boolean isSetFilename() {
    return this.filename != null;
  }

  public void setFilenameIsSet(boolean value) {
    if (!value) {
      this.filename = null;
    }
  }

  public int getReadRequestsSize() {
    return (this.readRequests == null) ? 0 : this.readRequests.size();
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.Iterator<ReadRequest> getReadRequestsIterator() {
    return (this.readRequests == null) ? null : this.readRequests.iterator();
  }

  public void addToReadRequests(ReadRequest elem) {
    if (this.readRequests == null) {
      this.readRequests = new java.util.ArrayList<ReadRequest>();
    }
    this.readRequests.add(elem);
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.List<ReadRequest> getReadRequests() {
    return this.readRequests;
  }

  public ReadFileRequestBatch setReadRequests(@org.apache.thrift.annotation.Nullable java.util.List<ReadRequest> readRequests) {
    this.readRequests = readRequests;
    return this;
  }

  public void unsetReadRequests() {
    this.readRequests = null;
  }

  /** Returns true if field readRequests is set (has been assigned a value) and false otherwise */
  public boolean isSetReadRequests() {
    return this.readRequests != null;
  }

  public void setReadRequestsIsSet(boolean value) {
    if (!value) {
      this.readRequests = null;
    }
  }

  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case FILENAME:
      if (value == null) {
        unsetFilename();
      } else {
        setFilename((java.lang.String)value);
      }
      break;

    case READ_REQUESTS:
      if (value == null) {
        unsetReadRequests();
      } else {
        setReadRequests((java.util.List<ReadRequest>)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case FILENAME:
      return getFilename();

    case READ_REQUESTS:
      return getReadRequests();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case FILENAME:
      return isSetFilename();
    case READ_REQUESTS:
      return isSetReadRequests();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that == null)
      return false;
    if (that instanceof ReadFileRequestBatch)
      return this.equals((ReadFileRequestBatch)that);
    return false;
  }

  public boolean equals(ReadFileRequestBatch that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_filename = true && this.isSetFilename();
    boolean that_present_filename = true && that.isSetFilename();
    if (this_present_filename || that_present_filename) {
      if (!(this_present_filename && that_present_filename))
        return false;
      if (!this.filename.equals(that.filename))
        return false;
    }

    boolean this_present_readRequests = true && this.isSetReadRequests();
    boolean that_present_readRequests = true && that.isSetReadRequests();
    if (this_present_readRequests || that_present_readRequests) {
      if (!(this_present_readRequests && that_present_readRequests))
        return false;
      if (!this.readRequests.equals(that.readRequests))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isSetFilename()) ? 131071 : 524287);
    if (isSetFilename())
      hashCode = hashCode * 8191 + filename.hashCode();

    hashCode = hashCode * 8191 + ((isSetReadRequests()) ? 131071 : 524287);
    if (isSetReadRequests())
      hashCode = hashCode * 8191 + readRequests.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(ReadFileRequestBatch other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.valueOf(isSetFilename()).compareTo(other.isSetFilename());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetFilename()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.filename, other.filename);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetReadRequests()).compareTo(other.isSetReadRequests());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetReadRequests()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.readRequests, other.readRequests);
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
    java.lang.StringBuilder sb = new java.lang.StringBuilder("ReadFileRequestBatch(");
    boolean first = true;

    sb.append("filename:");
    if (this.filename == null) {
      sb.append("null");
    } else {
      sb.append(this.filename);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("readRequests:");
    if (this.readRequests == null) {
      sb.append("null");
    } else {
      sb.append(this.readRequests);
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

  private static class ReadFileRequestBatchStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public ReadFileRequestBatchStandardScheme getScheme() {
      return new ReadFileRequestBatchStandardScheme();
    }
  }

  private static class ReadFileRequestBatchStandardScheme extends org.apache.thrift.scheme.StandardScheme<ReadFileRequestBatch> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, ReadFileRequestBatch struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // FILENAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.filename = iprot.readString();
              struct.setFilenameIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // READ_REQUESTS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list0 = iprot.readListBegin();
                struct.readRequests = new java.util.ArrayList<ReadRequest>(_list0.size);
                @org.apache.thrift.annotation.Nullable ReadRequest _elem1;
                for (int _i2 = 0; _i2 < _list0.size; ++_i2)
                {
                  _elem1 = new ReadRequest();
                  _elem1.read(iprot);
                  struct.readRequests.add(_elem1);
                }
                iprot.readListEnd();
              }
              struct.setReadRequestsIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, ReadFileRequestBatch struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.filename != null) {
        oprot.writeFieldBegin(FILENAME_FIELD_DESC);
        oprot.writeString(struct.filename);
        oprot.writeFieldEnd();
      }
      if (struct.readRequests != null) {
        oprot.writeFieldBegin(READ_REQUESTS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.readRequests.size()));
          for (ReadRequest _iter3 : struct.readRequests)
          {
            _iter3.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class ReadFileRequestBatchTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public ReadFileRequestBatchTupleScheme getScheme() {
      return new ReadFileRequestBatchTupleScheme();
    }
  }

  private static class ReadFileRequestBatchTupleScheme extends org.apache.thrift.scheme.TupleScheme<ReadFileRequestBatch> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, ReadFileRequestBatch struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetFilename()) {
        optionals.set(0);
      }
      if (struct.isSetReadRequests()) {
        optionals.set(1);
      }
      oprot.writeBitSet(optionals, 2);
      if (struct.isSetFilename()) {
        oprot.writeString(struct.filename);
      }
      if (struct.isSetReadRequests()) {
        {
          oprot.writeI32(struct.readRequests.size());
          for (ReadRequest _iter4 : struct.readRequests)
          {
            _iter4.write(oprot);
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, ReadFileRequestBatch struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet incoming = iprot.readBitSet(2);
      if (incoming.get(0)) {
        struct.filename = iprot.readString();
        struct.setFilenameIsSet(true);
      }
      if (incoming.get(1)) {
        {
          org.apache.thrift.protocol.TList _list5 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
          struct.readRequests = new java.util.ArrayList<ReadRequest>(_list5.size);
          @org.apache.thrift.annotation.Nullable ReadRequest _elem6;
          for (int _i7 = 0; _i7 < _list5.size; ++_i7)
          {
            _elem6 = new ReadRequest();
            _elem6.read(iprot);
            struct.readRequests.add(_elem6);
          }
        }
        struct.setReadRequestsIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

