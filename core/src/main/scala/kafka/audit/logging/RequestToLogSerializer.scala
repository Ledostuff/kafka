package kafka.audit.logging

import kafka.audit.logging.AuditEventExtractor._
import kafka.audit.logging.AuditEventExtractorSyntax._
import kafka.utils.Logging
import org.apache.kafka.common.requests._

object RequestToAuditLog extends Logging {
  def logRequest[T <: AbstractRequest](request: T, context: RequestContext): Unit = {
    request match {
      case createAclsRequest: CreateAclsRequest =>
        val createAclEntity = createAclsRequest.extractAuditEventPayload
        debug(CommonAuditRequestToLogSerializer.serializeRequest(context, AuditEventName.CreateAcl, createAclEntity))
      case deleteAclsRequest: DeleteAclsRequest =>
        val deleteAclEntity = deleteAclsRequest.extractAuditEventPayload
        debug(CommonAuditRequestToLogSerializer.serializeRequest(context, AuditEventName.DeleteAcl, deleteAclEntity))
      case describeAclsRequest: DescribeAclsRequest =>
        val describeAclsEntity = describeAclsRequest.extractAuditEventPayload
        debug(CommonAuditRequestToLogSerializer.serializeRequest(context, AuditEventName.DescribeAcl, describeAclsEntity))
      case alterConfigsRequest: AlterConfigsRequest =>
        val alterConfigsEntity = alterConfigsRequest.extractAuditEventPayload
        debug(CommonAuditRequestToLogSerializer.serializeRequest(context, AuditEventName.AlterConfigs, alterConfigsEntity))
      case describeConfigsRequest: DescribeConfigsRequest =>
        val describeConfigsEntity = describeConfigsRequest.extractAuditEventPayload
        debug(CommonAuditRequestToLogSerializer.serializeRequest(context, AuditEventName.DescribeConfigs, describeConfigsEntity))
      case createTopicsRequest: CreateTopicsRequest =>
        val createTopicsEntity = createTopicsRequest.extractAuditEventPayload
        debug(CommonAuditRequestToLogSerializer.serializeRequest(context, AuditEventName.CreateTopic, createTopicsEntity))
      case deleteTopicsRequest: DeleteTopicsRequest =>
        val deleteTopicsEntity = deleteTopicsRequest.extractAuditEventPayload
        debug(CommonAuditRequestToLogSerializer.serializeRequest(context, AuditEventName.DeleteTopic, deleteTopicsEntity))
      case other => warn(s"Unexpected request type to log to audit logger - '${other.getClass.getCanonicalName}'.")
    }
  }
}

trait RequestToLogSerializer[T <: AbstractRequest] {

  def serializeRequest(request: T, context: RequestContext): String

}
