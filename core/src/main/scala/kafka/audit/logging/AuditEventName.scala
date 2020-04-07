package kafka.audit.logging

object AuditEventName {
  val CommonAuditEventPrefix = "AUDIT_EVENT"

  val CreateTopic = "CREATE_TOPIC"
  val CreateTopicStart = "CREATE_TOPIC_START"
  val CreateTopicEnd = "CREATE_TOPIC_END"

  val DeleteTopic = "DELETE_TOPIC"
  val DeleteTopicStart = "DELETE_TOPIC_START"
  val DeleteTopicEnd = "DELETE_TOPIC_END"

  val CreateAcl = "CREATE_ACL"
  val CreateAclStart = "CREATE_ACL_START"
  val CreateAclEnd = "CREATE_ACL_END"

  val DeleteAcl = "DELETE_ACL"
  val DeleteAclStart = "DELETE_ACL_START"
  val DeleteAclEnd = "DELETE_ACL_END"

  val DescribeAcl = "DESCRIBE_ACL"
  val DescribeAclStart = "DESCRIBE_ACL_START"
  val DescribeAclEnd = "DESCRIBE_ACL_END"

  val AlterConfigsStart = "ALTER_CONFIGS_START"
  val AlterConfigsEnd = "ALTER_CONFIGS_END"

  val DescribeConfigsStart = "DESCRIBE_CONFIGS_START"
  val DescribeConfigsEnd = "DESCRIBE_CONFIGS_END"
}

object AuditToLogFieldNames {

  val ClientHostField = "CLIENT_HOST_FIELD"
  val ClientPrincipalField = "CLIENT_PRINCIPAL_FIELD"
  val EntityNameField = "ENTITY_NAME_FIELD"
}