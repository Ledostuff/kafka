package kafka.audit.logging

import org.apache.kafka.common.requests.{CreateTopicsRequest, RequestContext}

import scala.collection.JavaConverters._

object CreateTopicsRequestToLogSerializer extends RequestToLogSerializer[CreateTopicsRequest] {
  override def serializeRequest(request: CreateTopicsRequest, context: RequestContext): String = {
    s"""${AuditEventName.CommonAuditEventPrefix}={
      |${AuditEventName.CreateTopic}={
      |${AuditToLogFieldNames.EntityNameField}={
      |${request.data().topics().valuesList().asScala.mkString("[", ", ", "]")}
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
