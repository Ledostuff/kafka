package kafka.audit.logging

import org.apache.kafka.common.requests.RequestContext

object CommonAuditRequestToLogSerializer {

  def serializeRequest(context: RequestContext, eventType: String, entityName: String): String = {
    s"""${AuditEventName.CommonAuditEventPrefix}={
       |$eventType={
       |${AuditToLogFieldNames.EntityNameField}={
       |$entityName
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
