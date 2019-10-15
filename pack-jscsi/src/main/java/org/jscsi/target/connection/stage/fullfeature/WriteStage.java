package org.jscsi.target.connection.stage.fullfeature;

import java.io.IOException;
import java.net.InetAddress;
import java.security.DigestException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jscsi.exception.InternetSCSIException;
import org.jscsi.parser.AbstractMessageParser;
import org.jscsi.parser.BasicHeaderSegment;
import org.jscsi.parser.ProtocolDataUnit;
import org.jscsi.parser.data.DataOutParser;
import org.jscsi.parser.nop.NOPOutParser;
import org.jscsi.parser.scsi.SCSICommandParser;
import org.jscsi.parser.scsi.SCSIResponseParser;
import org.jscsi.parser.scsi.SCSIStatus;
import org.jscsi.target.TargetServer;
import org.jscsi.target.connection.TargetPduFactory;
import org.jscsi.target.connection.phase.TargetFullFeaturePhase;
import org.jscsi.target.scsi.ScsiResponseDataSegment;
import org.jscsi.target.scsi.cdb.ScsiOperationCode;
import org.jscsi.target.scsi.cdb.Write10Cdb;
import org.jscsi.target.scsi.cdb.Write6Cdb;
import org.jscsi.target.scsi.cdb.WriteCdb;
import org.jscsi.target.settings.SettingsException;
import org.jscsi.target.storage.IStorageModule;
import org.jscsi.target.util.Debug;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentracing.Scope;
import pack.util.tracer.TracerUtil;

/**
 * A stage for processing <code>WRITE (6)</code> and <code>WRITE (10)</code>
 * SCSI commands.
 * 
 * @author Andreas Ergenzinger
 */
public final class WriteStage extends ReadOrWriteStage {

  private static final Logger LOGGER = LoggerFactory.getLogger(WriteStage.class);

  private static final boolean CHOPPED_UP = false;

  /**
   * The <code>DataSN</code> value the next Data-Out PDU must carry.
   */
  private int expectedDataSequenceNumber = 0;

  public WriteStage(TargetFullFeaturePhase targetFullFeaturePhase) {
    super(targetFullFeaturePhase);
  }

  /**
   * Is used for checking if the PDUs received in a Data-Out sequence actually
   * are Data-Out PDU and if the PDUs have been received in order.
   * 
   * @param parser
   *          the {@link AbstractMessageParser} subclass instance retrieved from
   *          the {@link ProtocolDataUnit}'s {@link BasicHeaderSegment}
   * @throws InternetSCSIException
   *           if an unexpected PDU has been received
   */
  private void checkDataOutParser(final AbstractMessageParser parser) throws InternetSCSIException {
    if (parser instanceof DataOutParser) {
      final DataOutParser p = (DataOutParser) parser;
      if (p.getDataSequenceNumber() != expectedDataSequenceNumber++) {
        throw new InternetSCSIException(
            "received erroneous PDU in data-out sequence, expected " + (expectedDataSequenceNumber - 1));
      }
    } else if (parser instanceof NOPOutParser || parser instanceof SCSICommandParser) {

    } else {
      if (parser != null) {
        throw new InternetSCSIException("received erroneous PDU in data-out sequence, " + parser.getClass()
                                                                                                .getName());
      } else {
        throw new InternetSCSIException("received erroneous PDU in data-out sequence, parser is null");
      }
    }

  }

  @Override
  public void execute(ProtocolDataUnit pdu)
      throws IOException, DigestException, InterruptedException, InternetSCSIException, SettingsException {
    try (Scope s0 = TracerUtil.trace(getClass(), "execute")) {
      LOGGER.debug("write {}", pdu);

      InetAddress address = session.getTargetServer()
                                   .getTargetAddress();
      int port = session.getTargetServer()
                        .getTargetPort();

      AtomicBoolean needsFlush = new AtomicBoolean(false);

      if (LOGGER.isDebugEnabled())
        LOGGER.debug("Entering WRITE STAGE");

      // get relevant values from settings
      final boolean immediateData = settings.getImmediateData();
      final boolean initialR2T = settings.getInitialR2T();
      final int firstBurstLength = settings.getFirstBurstLength();
      final int maxBurstLength = settings.getMaxBurstLength();

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("immediateData = " + immediateData);
        LOGGER.debug("initialR2T = " + initialR2T);
      }

      // get relevant values from PDU/CDB
      BasicHeaderSegment bhs = pdu.getBasicHeaderSegment();
      SCSICommandParser parser = (SCSICommandParser) bhs.getParser();
      final int initiatorTaskTag = bhs.getInitiatorTaskTag();

      WriteCdb cdb;
      final ScsiOperationCode scsiOpCode = ScsiOperationCode.valueOf(parser.getCDB()
                                                                           .get(0));
      if (scsiOpCode == ScsiOperationCode.WRITE_10)
        cdb = new Write10Cdb(parser.getCDB());
      else if (scsiOpCode == ScsiOperationCode.WRITE_6)
        cdb = new Write6Cdb(parser.getCDB());
      else {
        // anything else wouldn't be good (programmer error)
        // close connection
        throw new InternetSCSIException("wrong SCSI Operation Code " + scsiOpCode + " in WriteStage");
      }
      final int transferLength = cdb.getTransferLength();
      final long logicalBlockAddress = cdb.getLogicalBlockAddress();

      IStorageModule storageModule = session.getStorageModule();
      int blockSize = storageModule.getBlockSize();

      // transform to from block units to byte units
      final int transferLengthInBytes = transferLength * blockSize;
      long storageIndex = logicalBlockAddress * blockSize;

      byte[] buffer = null;
      if (!CHOPPED_UP) {
        try (Scope s1 = TracerUtil.trace(getClass(), "allocate")) {
          buffer = new byte[transferLengthInBytes];
        }
      }

      // check if requested blocks are out of bounds
      // (might add FPSKSD to the CDB's list to be detected in the next step)
      checkOverAndUnderflow(cdb);

      if (cdb.getIllegalFieldPointers() != null) {
        /*
         * CDB is invalid, inform initiator by closing the connection. Sending
         * an error status SCSI Response PDU will not work reliably, since the
         * initiator may not be expecting a response so soon. Also, if the
         * WriteStage is simply left early (without closing the connection), the
         * initiator may send additional unsolicited Data-Out PDUs, which the
         * jSCSI Target is currently unable to ignore or process properly.
         */
        LOGGER.debug("illegal field in Write CDB");
        LOGGER.debug("CDB:\n" + Debug.byteBufferToString(parser.getCDB()));

        // Not necessarily close the connection

        // create and send error PDU and leave stage
        final ProtocolDataUnit responsePdu = createFixedFormatErrorPdu(cdb.getIllegalFieldPointers(), // senseKeySpecificData
            initiatorTaskTag, parser.getExpectedDataTransferLength());
        // this may need work on the flush during error.
        flushIfNeeded(needsFlush, buffer, storageModule, storageIndex);
        try (Scope s1 = TracerUtil.trace(getClass(), "sendPdu")) {
          connection.sendPdu(responsePdu);
        }
        return;
      }

      // *** start receiving data (or process what has already been sent) ***
      int bytesReceived = 0;

      // *** receive immediate data ***
      if (immediateData && bhs.getDataSegmentLength() > 0) {
        final byte[] immediateDataArray = pdu.getDataSegment()
                                             .array();
        int commandSequenceNumber = parser.getCommandSequenceNumber();
        if (CHOPPED_UP) {
          storageModule.write(immediateDataArray, storageIndex, address, port, initiatorTaskTag, commandSequenceNumber,
              null, null);
        } else {
          System.arraycopy(immediateDataArray, 0, buffer, 0, immediateDataArray.length);
        }
        needsFlush.set(true);
        bytesReceived = immediateDataArray.length;

        if (LOGGER.isDebugEnabled())
          LOGGER.debug("wrote " + immediateDataArray.length + "bytes as immediate data");
      }

      // *** receive unsolicited data ***
      if (!initialR2T && !bhs.isFinalFlag()) {

        if (LOGGER.isDebugEnabled())
          LOGGER.debug("receiving unsolicited data");

        boolean firstBurstOver = false;
        while (!firstBurstOver && bytesReceived <= firstBurstLength) {

          // receive and check PDU
          pdu = connection.receivePdu();
          bhs = pdu.getBasicHeaderSegment();

          checkDataOutParser(bhs.getParser());

          final DataOutParser dataOutParser = (DataOutParser) bhs.getParser();
          int dataSequenceNumber = dataOutParser.getDataSequenceNumber();
          int targetTransferTag = dataOutParser.getTargetTransferTag();
          int commandSequenceNumber = dataOutParser.getCommandSequenceNumber();
          byte[] data = pdu.getDataSegment()
                           .array();
          if (CHOPPED_UP) {
            storageModule.write(data, storageIndex + dataOutParser.getBufferOffset(), address, port, initiatorTaskTag,
                commandSequenceNumber, dataSequenceNumber, targetTransferTag);
          } else {
            System.arraycopy(data, 0, buffer, dataOutParser.getBufferOffset(), data.length);
          }

          needsFlush.set(true);
          bytesReceived += bhs.getDataSegmentLength();

          if (bhs.isFinalFlag())
            firstBurstOver = true;
        }
      }

      // *** receive solicited data ***
      if (bytesReceived < transferLengthInBytes) {
        if (LOGGER.isDebugEnabled())
          LOGGER.debug(bytesReceived + "<" + transferLengthInBytes);

        int readyToTransferSequenceNumber = 0;
        int desiredDataTransferLength;

        while (bytesReceived < transferLengthInBytes) {

          desiredDataTransferLength = Math.min(maxBurstLength, transferLengthInBytes - bytesReceived);

          // send R2T
          pdu = TargetPduFactory.createReadyToTransferPdu(0, // logicalUnitNumber
              initiatorTaskTag, TargetServer.getNextTargetTransferTag(), // targetTransferTag
              readyToTransferSequenceNumber++, bytesReceived, // bufferOffset
              desiredDataTransferLength);
          try (Scope s1 = TracerUtil.trace(getClass(), "sendPdu")) {
            connection.sendPdu(pdu);
          }

          // receive DataOut PDUs
          expectedDataSequenceNumber = 0;// reset sequence counter//FIXME
                                         // fix in jSCSI Initiator
          boolean solicitedDataCycleOver = false;
          int bytesReceivedThisCycle = 0;
          while (!solicitedDataCycleOver) {

            // receive and check PDU
            pdu = connection.receivePdu();
            bhs = pdu.getBasicHeaderSegment();
            checkDataOutParser(bhs.getParser());

            if (bhs.getParser() instanceof NOPOutParser) {
              /* send SCSI Response PDU */
              pdu = TargetPduFactory.createSCSIResponsePdu(false, // bidirectionalReadResidualOverflow
                  false, // bidirectionalReadResidualUnderflow
                  false, // residualOverflow
                  false, // residualUnderflow
                  SCSIResponseParser.ServiceResponse.COMMAND_COMPLETED_AT_TARGET, // response
                  SCSIStatus.GOOD, // status
                  initiatorTaskTag, 0, // snackTag
                  0, // (ExpDataSN or) Reserved
                  0, // bidirectionalReadResidualCount
                  0, // residualCount
                  ScsiResponseDataSegment.EMPTY_DATA_SEGMENT);// dataSegment
              flushIfNeeded(needsFlush, buffer, storageModule, storageIndex);
              try (Scope s1 = TracerUtil.trace(getClass(), "sendPdu")) {
                connection.sendPdu(pdu);
              }
              return;
            } else if (bhs.getParser() instanceof DataOutParser) {
              final DataOutParser dataOutParser = (DataOutParser) bhs.getParser();
              int dataSequenceNumber = dataOutParser.getDataSequenceNumber();
              int targetTransferTag = dataOutParser.getTargetTransferTag();
              int commandSequenceNumber = dataOutParser.getCommandSequenceNumber();
              byte[] data = pdu.getDataSegment()
                               .array();
              if (CHOPPED_UP) {
                storageModule.write(data, storageIndex + dataOutParser.getBufferOffset(), address, port,
                    initiatorTaskTag, commandSequenceNumber, dataSequenceNumber, targetTransferTag);
              } else {
                System.arraycopy(data, 0, buffer, dataOutParser.getBufferOffset(), data.length);
              }

              needsFlush.set(true);
              bytesReceivedThisCycle += bhs.getDataSegmentLength();

              /*
               * Checking the final flag should be enough, but is not, when
               * dealing with the jSCSI Initiator. This is also one of the
               * reasons, why the contents of this while loop, though very
               * similar to what is happening during the receiving of the
               * unsolicited data PDU sequence, has not been put into a
               * dedicated method.
               */
              if (bhs.isFinalFlag() || bytesReceivedThisCycle >= desiredDataTransferLength)
                solicitedDataCycleOver = true;
            }
          }
          bytesReceived += bytesReceivedThisCycle;
        }
      }

      /* send SCSI Response PDU */
      pdu = TargetPduFactory.createSCSIResponsePdu(false, // bidirectionalReadResidualOverflow
          false, // bidirectionalReadResidualUnderflow
          false, // residualOverflow
          false, // residualUnderflow
          SCSIResponseParser.ServiceResponse.COMMAND_COMPLETED_AT_TARGET, // response
          SCSIStatus.GOOD, // status
          initiatorTaskTag, 0, // snackTag
          0, // (ExpDataSN or) Reserved
          0, // bidirectionalReadResidualCount
          0, // residualCount
          ScsiResponseDataSegment.EMPTY_DATA_SEGMENT);// dataSegment
      flushIfNeeded(needsFlush, buffer, storageModule, storageIndex);
      try (Scope s1 = TracerUtil.trace(getClass(), "sendPdu")) {
        connection.sendPdu(pdu);
      }
    }
  }

  private void flushIfNeeded(AtomicBoolean needsFlush, byte[] buffer, IStorageModule storageModule, long storageIndex)
      throws IOException {
    try (Scope s0 = TracerUtil.trace(getClass(), "flushIfNeeded")) {
      if (!CHOPPED_UP) {
        storageModule.write(buffer, storageIndex);
      }
      if (needsFlush.get()) {
        storageModule.flushWrites();
        needsFlush.set(false);
      }
    }
  }
}
