package kafka.audit.logging

import kafka.utils.Logging
import org.apache.kafka.common.requests.{AbstractRequest, CreateAclsRequest, CreateTopicsRequest, RequestContext}

object RequestToAuditLog extends Logging {
  def logRequest[T <: AbstractRequest](request: T, context: RequestContext): Unit = {
    request match {
      case createAclsRequest: CreateAclsRequest => debug(CreateAclsRequestToLogSerializer.serializeRequest(createAclsRequest, context))
      case createTopicsRequest: CreateTopicsRequest => debug(CreateTopicsRequestToLogSerializer.serializeRequest(createTopicsRequest, context))
      case other => warn(s"Unexpected request type to log to audit logger - '${other.getClass.getCanonicalName}'.")
    }
  }
}

trait RequestToLogSerializer[T <: AbstractRequest] {

  def serializeRequest(request: T, context: RequestContext): String

}
