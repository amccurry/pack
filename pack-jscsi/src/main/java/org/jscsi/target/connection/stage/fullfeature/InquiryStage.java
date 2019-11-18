package org.jscsi.target.connection.stage.fullfeature;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.DigestException;
import java.util.UUID;

import org.jscsi.exception.InternetSCSIException;
import org.jscsi.parser.BasicHeaderSegment;
import org.jscsi.parser.ProtocolDataUnit;
import org.jscsi.parser.scsi.SCSICommandParser;
import org.jscsi.target.connection.TargetSession;
import org.jscsi.target.connection.phase.TargetFullFeaturePhase;
import org.jscsi.target.scsi.IResponseData;
import org.jscsi.target.scsi.cdb.InquiryCDB;
import org.jscsi.target.scsi.inquiry.PageCode.VitalProductDataPageName;
import org.jscsi.target.scsi.inquiry.StandardInquiryData;
import org.jscsi.target.scsi.inquiry.SupportedVpdPages;
import org.jscsi.target.scsi.inquiry.UnitSerialNumberVpdPage;
import org.jscsi.target.scsi.sense.senseDataDescriptor.senseKeySpecific.FieldPointerSenseKeySpecificData;
import org.jscsi.target.settings.SettingsException;
import org.jscsi.target.storage.IStorageModule;
import org.jscsi.target.util.Debug;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A stage for processing <code>INQUIRY</code> SCSI commands.
 * 
 * @author Andreas Ergenzinger
 */
public class InquiryStage extends TargetFullFeatureStage {

  private static final Logger LOGGER = LoggerFactory.getLogger(InquiryStage.class);

  public InquiryStage(TargetFullFeaturePhase targetFullFeaturePhase) {
    super(targetFullFeaturePhase);
  }

  @Override
  public void execute(ProtocolDataUnit pdu)
      throws IOException, InterruptedException, InternetSCSIException, DigestException, SettingsException {

    final BasicHeaderSegment bhs = pdu.getBasicHeaderSegment();
    final SCSICommandParser parser = (SCSICommandParser) bhs.getParser();

    TargetSession targetSession = connection.getTargetSession();
    IStorageModule storageModule = targetSession.getStorageModule();
    String vendorId = storageModule.getVendorId();
    String productId = storageModule.getProductId();

    ProtocolDataUnit responsePdu = null;// the response PDU

    // get command details in CDB
    if (LOGGER.isDebugEnabled()) {// print CDB bytes
      LOGGER.debug("CDB bytes: \n" + Debug.byteBufferToString(parser.getCDB()));
    }

    final InquiryCDB cdb = new InquiryCDB(parser.getCDB());
    final FieldPointerSenseKeySpecificData[] illegalFieldPointers = cdb.getIllegalFieldPointers();

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("cdb.getAllocationLength() = " + cdb.getAllocationLength());
      LOGGER.debug("cdb.getEnableVitalProductData() = " + cdb.getEnableVitalProductData());
      LOGGER.debug("cdb.isNormalACA() = " + cdb.isNormalACA());
      LOGGER.debug("cdb.getPageCode() = " + cdb.getPageCode());
      LOGGER.debug("cdb.getPageCode().getVitalProductDataPageName() = " + cdb.getPageCode()
                                                                             .getVitalProductDataPageName());
    }

    // LOGGER.info("cdb.getAllocationLength() = " + cdb.getAllocationLength());
    // LOGGER.info("cdb.getEnableVitalProductData() = " +
    // cdb.getEnableVitalProductData());
    // LOGGER.info("cdb.isNormalACA() = " + cdb.isNormalACA());
    // LOGGER.info("cdb.getPageCode() = " + cdb.getPageCode());
    // LOGGER.info("cdb.getPageCode().getVitalProductDataPageName() = " +
    // cdb.getPageCode()
    // .getVitalProductDataPageName());
    // LOGGER.info("parser.getExpectedDataTransferLength() = {}",
    // parser.getExpectedDataTransferLength());

    if (illegalFieldPointers != null) {
      // an illegal request has been made
      LOGGER.error("illegal INQUIRY request");

      for (FieldPointerSenseKeySpecificData o : illegalFieldPointers) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(o.size());
        o.serialize(byteBuffer, 0);
        byteBuffer.flip();
        LOGGER.error("illegal {} {}", o.size(), Debug.byteBufferToString(byteBuffer));
      }

      responsePdu = createFixedFormatErrorPdu(illegalFieldPointers, bhs.getInitiatorTaskTag(),
          parser.getExpectedDataTransferLength());

      // send response
      connection.sendPdu(responsePdu);

    } else {
      // PDU is okay
      // carry out command

      IResponseData responseData = null;

      // "If the EVPD bit is set to zero, ...
      if (!cdb.getEnableVitalProductData()) {
        // ... the device server shall return the standard INQUIRY
        // data."
        responseData = new StandardInquiryData(vendorId, productId, false);
      } else {
        /*
         * SCSI initiator is requesting either "device identification" or
         * "supported VPD pages" or this else block would not have been entered.
         * (see {@link InquiryCDB#checkIntegrity(ByteBuffer dataSegment)})
         */
        final VitalProductDataPageName pageName = cdb.getPageCode()
                                                     .getVitalProductDataPageName();

        switch (pageName) {// is never null
        case SUPPORTED_VPD_PAGES:
          responseData = SupportedVpdPages.getInstance();
          break;
        case DEVICE_IDENTIFICATION:
          responseData = new StandardInquiryData(vendorId, productId, false);
          break;
        case UNIT_SERIAL_NUMBER:
          UUID uuid = session.getStorageModule()
                             .getUnitSerialNumberUUID();
          if (uuid == null) {
            responseData = session.getTargetServer()
                                  .getUnitSerialNumber();
          } else {
            responseData = new UnitSerialNumberVpdPage(uuid);
          }
          break;

        // case BLOCK_LIMITS:
        // responseData = new BlockLimits();
        // break;
        default:
          // The initiator must not request unsupported mode pages.
          throw new InternetSCSIException();
        }
      }

      // send response
      sendResponse(bhs.getInitiatorTaskTag(), responseData.size(), responseData);

    }

  }

}
