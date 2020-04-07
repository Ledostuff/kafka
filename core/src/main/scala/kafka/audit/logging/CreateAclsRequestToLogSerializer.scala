package kafka.audit.logging

import org.apache.kafka.common.requests.{CreateAclsRequest, RequestContext}

import scala.collection.JavaConverters._

object CreateAclsRequestToLogSerializer extends RequestToLogSerializer[CreateAclsRequest] {
  override def serializeRequest(request: CreateAclsRequest, context: RequestContext): String = {
    s"""${AuditEventName.CommonAuditEventPrefix}={
      |${AuditEventName.CreateAcl}={
      |${AuditToLogFieldNames.EntityNameField}={
      |${request.aclCreations().asScala.mkString("[", ", ", "]")}
      |}
      |${AuditToLogFieldNames.ClientHostField}={
      |${context.clientAddress.getCanonicalHostName}
      |}
      |${AuditToLogFieldNames.ClientPrincipalField}={
      |${context.principal.getName}
      |}
      |}
      |}""".stripMargin
  }
}
