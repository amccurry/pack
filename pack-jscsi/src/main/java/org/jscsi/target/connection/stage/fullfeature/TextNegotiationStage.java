package org.jscsi.target.connection.stage.fullfeature;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.DigestException;
import java.util.List;
import java.util.Vector;

import org.jscsi.exception.InternetSCSIException;
import org.jscsi.parser.AbstractMessageParser;
import org.jscsi.parser.BasicHeaderSegment;
import org.jscsi.parser.ProtocolDataUnit;
import org.jscsi.parser.text.TextRequestParser;
import org.jscsi.target.connection.TargetPduFactory;
import org.jscsi.target.connection.phase.TargetFullFeaturePhase;
import org.jscsi.target.settings.SettingsException;
import org.jscsi.target.settings.TextKeyword;
import org.jscsi.target.settings.TextParameter;
import org.jscsi.target.util.ReadWrite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A stage for processing requests by the initiator for a list of all targets
 * available through the iSCSI portal and for negotiating connection parameters
 * in the full feature phase.
 * <p>
 * That latter functionality is currently not implemented.
 * 
 * @author Andreas Ergenzinger
 */
public final class TextNegotiationStage extends TargetFullFeatureStage {

  private static final Logger LOGGER = LoggerFactory.getLogger(TextNegotiationStage.class);

  public TextNegotiationStage(TargetFullFeaturePhase targetFullFeaturePhase) {
    super(targetFullFeaturePhase);
  }

  @Override
  public void execute(ProtocolDataUnit pdu)
      throws IOException, InterruptedException, InternetSCSIException, DigestException, SettingsException {

    final BasicHeaderSegment bhs = pdu.getBasicHeaderSegment();

    final int initiatorTaskTag = bhs.getInitiatorTaskTag();

    final String textRequest = new String(pdu.getDataSegment()
                                             .array());

    LOGGER.debug("text request: " + textRequest);

    ByteBuffer replyDataSegment = null;// for later

    // tokenize key-value pairs
    final List<String> requestKeyValuePairs = TextParameter.tokenizeKeyValuePairs(textRequest);

    final List<String> responseKeyValuePairs = new Vector<String>();

    // process SendTargets command
    if (requestKeyValuePairs != null) {
      // A SendTargets command consists of a single Text request PDU. This
      // PDU contains exactly one text key and value.
      String sendTargetsValue = null;

      if (requestKeyValuePairs.size() == 1)
        sendTargetsValue = TextParameter.getSuffix(requestKeyValuePairs.get(0), // string
            TextKeyword.SEND_TARGETS + TextKeyword.EQUALS);// prefix

      if (sendTargetsValue != null) {
        // initiator wants target information
        /*
         * ALL must be supported in discovery session and must not be supported
         * in operational session. <name-of-this-target> must be supported in
         * discovery session (and it does no harm to support it in the
         * operational session). <nothing> must be supported in the operational
         * session and will only return info on the target the initiator is
         * connected to, this means no info is returned in discovery session.
         * outcome table | discovery |operational|
         * ---------------+-----------+-----------| ALL | TN + TA | fail |
         * ---------------+-----------+-----------| <this target> | TA | TA |
         * ---------------+-----------+-----------| <other target> | fail | fail
         * | ---------------+-----------+-----------| <nothing> | fail | TA | TN
         * stands for TargetName, TA for TargetAddress "fail" means no text
         * response.
         */
        final boolean normal = session.isNormalSession();
        final boolean sendTargetName = // see upper table
            !normal && sendTargetsValue.equals(TextKeyword.ALL);
        final boolean sendTargetAddress = // see upper table
            (!normal && sendTargetsValue.equals(TextKeyword.ALL)) || (session.getTargetServer()
                                                                             .isValidTargetName(sendTargetsValue))
                || (normal && sendTargetsValue.length() == 0);

        /*
         * A target record consists of a TargetName key-value pair followed by
         * one or more TargetAddress key-value pairs for that TargetName. (table
         * above takes precedence to these definitions)
         */

        // add TargetName
        if (sendTargetName) {
          for (String curTargetName : session.getTargetServer()
                                             .getTargetNames()) {
            responseKeyValuePairs.add(TextParameter.toKeyValuePair(TextKeyword.TARGET_NAME, curTargetName));
            // add TargetAddress
            if (sendTargetAddress)
              responseKeyValuePairs.add(TextParameter.toKeyValuePair(TextKeyword.TARGET_ADDRESS,
                  session.getTargetServer()
                         .getConfig()
                         .getTargetAddress()
                      + // domain
                      TextKeyword.COLON + // :
                      session.getTargetServer()
                             .getConfig()
                             .getPort()
                      + // port
                      TextKeyword.COMMA + // ,
                      session.getTargetServer()
                             .getConfig()
                             .getTargetPortalGroupTag())); // groupTag)
          }
        } else {
          // We're here if they sent us a target name and are asking for the
          // address (I think)
          if (sendTargetAddress)
            responseKeyValuePairs.add(TextParameter.toKeyValuePair(TextKeyword.TARGET_ADDRESS, session.getTargetServer()
                                                                                                      .getConfig()
                                                                                                      .getTargetAddress()
                + // domain
                TextKeyword.COLON + // :
                session.getTargetServer()
                       .getConfig()
                       .getPort()
                + // port
                TextKeyword.COMMA + // ,
                session.getTargetServer()
                       .getConfig()
                       .getTargetPortalGroupTag())); // groupTag)
        }

      } else {
        // initiator wants to negotiate or declare parameters
        // TODO
      }
      // concatenate and serialize reply
      final String replyString = TextParameter.concatenateKeyValuePairs(responseKeyValuePairs);
      LOGGER.debug("text negotiation stage reply: " + replyString);
      // definitely fits into one data segment
      // replyDataSegment = ReadWrite.stringToTextDataSegments(replyString,
      // settings.getMaxRecvDataSegmentLength())[0];

      ByteBuffer[] buffers = ReadWrite.stringToTextDataSegments(replyString, settings.getMaxRecvDataSegmentLength());
      if (buffers.length == 1) {
        replyDataSegment = buffers[0];
        // send reply
        final ProtocolDataUnit responsePdu = TargetPduFactory.createTextResponsePdu(true, // finalFlag
            false, // continueFlag
            0, // logicalUnitNumber
            initiatorTaskTag, 0xffffffff, // targetTransferTag
            replyDataSegment);// dataSegment
        connection.sendPdu(responsePdu);
        return;
      } else {
        for (int i = 0; i < buffers.length; i++) {
          replyDataSegment = buffers[i];
          if (i == buffers.length - 1) {
            // last one
            ProtocolDataUnit responsePdu = TargetPduFactory.createTextResponsePdu(true, false, i, initiatorTaskTag, i,
                replyDataSegment);
            LOGGER.info("responsePdu {}", responsePdu);
            connection.sendPdu(responsePdu);
          } else {
            ProtocolDataUnit responsePdu = TargetPduFactory.createTextResponsePdu(false, true, i, initiatorTaskTag, i,
                replyDataSegment);
            LOGGER.info("responsePdu {}", responsePdu);
            connection.sendPdu(responsePdu);
            ProtocolDataUnit receivePdu = connection.receivePdu();
            AbstractMessageParser parser = receivePdu.getBasicHeaderSegment()
                                                     .getParser();
            if (parser instanceof TextRequestParser) {
              TextRequestParser textRequestParser = (TextRequestParser) parser;
              if (i != textRequestParser.getTargetTransferTag()) {
                throw new InternetSCSIException("PDU not recoginized " + receivePdu);
              }
            }
          }
        }
        return;
      }
    }

    // send reply
    final ProtocolDataUnit responsePdu = TargetPduFactory.createTextResponsePdu(true, // finalFlag
        false, // continueFlag
        0, // logicalUnitNumber
        initiatorTaskTag, 0xffffffff, // targetTransferTag
        replyDataSegment);// dataSegment

    connection.sendPdu(responsePdu);
  }

}
