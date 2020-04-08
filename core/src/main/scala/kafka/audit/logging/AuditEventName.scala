package kafka.audit.logging

object AuditEventName {
  val CommonAuditEventPrefix = "AUDIT_EVENT"

  val CreateTopic = "CREATE_TOPIC"

  val DeleteTopic = "DELETE_TOPIC"

  val CreateAcl = "CREATE_ACL"

  val DeleteAcl = "DELETE_ACL"

  val DescribeAcl = "DESCRIBE_ACL"

  val AlterConfigs = "ALTER_CONFIGS"

  val DescribeConfigs = "DESCRIBE_CONFIGS"
}

object AuditToLogFieldNames {

  val ClientHostField = "CLIENT_HOST_FIELD"
  val ClientPrincipalField = "CLIENT_PRINCIPAL_FIELD"
  val EntityNameField = "ENTITY_NAME_FIELD"
}