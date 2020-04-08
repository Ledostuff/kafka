package kafka.audit.logging

import kafka.utils.Logging
import org.apache.kafka.common.requests.{AbstractRequest, AlterConfigsRequest, CreateAclsRequest, CreateTopicsRequest, DeleteAclsRequest, DeleteTopicsRequest, DescribeAclsRequest, DescribeConfigsRequest, RequestContext}
import scala.collection.JavaConverters._

object RequestToAuditLog extends Logging {
  def logRequest[T <: AbstractRequest](request: T, context: RequestContext): Unit = {
    request match {
      case createAclsRequest: CreateAclsRequest =>
        val createAclEntity = createAclsRequest.aclCreations().asScala.mkString("[", ", ", "]")
        debug(CommonAuditRequestToLogSerializer.serializeRequest(context, AuditEventName.CreateAcl, createAclEntity))
      case deleteAclsRequest: DeleteAclsRequest =>
        val deleteAclEntity = deleteAclsRequest.filters().asScala.mkString("[", ", ", "]")
        debug(CommonAuditRequestToLogSerializer.serializeRequest(context, AuditEventName.DeleteAcl, deleteAclEntity))
      case describeAclsRequest: DescribeAclsRequest =>
        val describeAclsEntity = describeAclsRequest.filter().toString
        debug(CommonAuditRequestToLogSerializer.serializeRequest(context, AuditEventName.DescribeAcl, describeAclsEntity))
      case alterConfigsRequest: AlterConfigsRequest =>
        val alterConfigsEntity = alterConfigsRequest.configs().asScala.mapValues { conf =>
          s"[${conf.entries().asScala.map(e => s"${e.name()} -> ${e.value()}").mkString("[", ", ", "]")}]"
        }.mkString("[", ", ", "]")
        debug(CommonAuditRequestToLogSerializer.serializeRequest(context, AuditEventName.AlterConfigs, alterConfigsEntity))
      case describeConfigsRequest: DescribeConfigsRequest =>
        val describeConfigsEntity = describeConfigsRequest.resources().asScala.mkString("[", ", ", "]")
        debug(CommonAuditRequestToLogSerializer.serializeRequest(context, AuditEventName.DescribeConfigs, describeConfigsEntity))
      case createTopicsRequest: CreateTopicsRequest =>
        val createTopicsEntity = createTopicsRequest.data().topics().valuesList().asScala.mkString("[", ", ", "]")
        debug(CommonAuditRequestToLogSerializer.serializeRequest(context, AuditEventName.CreateTopic, createTopicsEntity))
      case deleteTopicsRequest: DeleteTopicsRequest =>
        val deleteTopicsEntity = deleteTopicsRequest.data().topicNames().asScala.mkString("[", ", ", "]")
        debug(CommonAuditRequestToLogSerializer.serializeRequest(context, AuditEventName.DeleteTopic, deleteTopicsEntity))
      case other => warn(s"Unexpected request type to log to audit logger - '${other.getClass.getCanonicalName}'.")
    }
  }
}

trait RequestToLogSerializer[T <: AbstractRequest] {

  def serializeRequest(request: T, context: RequestContext): String

}
