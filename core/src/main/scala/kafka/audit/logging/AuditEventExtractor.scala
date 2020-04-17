package kafka.audit.logging

import kafka.audit.logging.AuditEventExtractor.AuditEventExtractor
import org.apache.kafka.common.acl.AclBindingFilter
import org.apache.kafka.common.protocol.MessageUtil
import org.apache.kafka.common.requests.{AlterConfigsRequest, CreateAclsRequest, CreateTopicsRequest, DeleteAclsRequest, DeleteTopicsRequest, DescribeAclsRequest, DescribeConfigsRequest}
import org.apache.kafka.common.resource.{ResourcePattern, ResourcePatternFilter}

import scala.collection.JavaConverters._

object AuditEventExtractor {

  sealed trait AuditEventExtractor[IN] {
    def extractStringRepr(in: IN): String
  }

  implicit object CreateAclsRequestAuditEventExtractor extends AuditEventExtractor[CreateAclsRequest] {
    def extractStringRepr(request: CreateAclsRequest): String = {
      request.aclCreations().asScala.map { creation =>
        val entry = creation.acl().entry()
        val pattern = creation.acl().pattern()
        val patternString = AclPatternExtractor.extractStringRepr(pattern)
        s"""(acl=(entry=${entry.toString}, pattern=${patternString}))"""
      }.mkString("[", ", ", "]")
    }
  }

  implicit object DeleteAclsRequestAuditEventExtractor extends AuditEventExtractor[DeleteAclsRequest] {
    def extractStringRepr(request: DeleteAclsRequest): String = {
      request.filters().asScala.map { filter =>
        AclBindingFilterExtractor.extractStringRepr(filter)
      }.mkString("[", ", ", "]")
    }
  }

  implicit object DescribeAclsRequestAuditEventExtractor extends AuditEventExtractor[DescribeAclsRequest] {
    def extractStringRepr(request: DescribeAclsRequest): String = {
      AclBindingFilterExtractor.extractStringRepr(request.filter())
    }
  }

  implicit object AlterConfigsRequestAuditEventExtractor extends AuditEventExtractor[AlterConfigsRequest] {
    def extractStringRepr(request: AlterConfigsRequest): String = {
      request.configs().asScala.mapValues { conf =>
        s"[${conf.entries().asScala.map(e => s"${e.name()} -> ${e.value()}").mkString("[", ", ", "]")}]"
      }.mkString("[", ", ", "]")
    }
  }

  implicit object DescribeConfigsRequestAuditEventExtractor extends AuditEventExtractor[DescribeConfigsRequest] {
    def extractStringRepr(request: DescribeConfigsRequest): String = {
      request.resources().asScala.map { resource =>
        s"type='${resource.`type`()}', name='${resource.name()}')"
      }.mkString("[", ", ", "]")
    }
  }

  implicit object CreateTopicsRequestAuditEventExtractor extends AuditEventExtractor[CreateTopicsRequest] {
    def extractStringRepr(request: CreateTopicsRequest): String = {
      request.data().topics().valuesList().asScala.map { creatableTopic =>
        "name=" + (if (creatableTopic.name() == null) "null"
        else "'" + creatableTopic.name() + "'") +
          ", numPartitions=" + creatableTopic.numPartitions() +
          ", replicationFactor=" + creatableTopic.replicationFactor() +
          ", assignments=" + MessageUtil.deepToString(creatableTopic.assignments().iterator) +
          ", configs=" + MessageUtil.deepToString(creatableTopic.configs().iterator) +
          ")"
      }.mkString("[", ", ", "]")
    }
  }

  implicit object DeleteTopicsRequestAuditEventExtractor extends AuditEventExtractor[DeleteTopicsRequest] {
    def extractStringRepr(request: DeleteTopicsRequest): String = {
      request.data().topicNames().asScala.mkString("[", ", ", "]")
    }
  }

  private object AclBindingFilterExtractor extends AuditEventExtractor[AclBindingFilter] {
    def extractStringRepr(filter: AclBindingFilter): String = {
      val entry = filter.entryFilter()
      val pattern = filter.patternFilter()
      val patternString = AclPatternFilterExtractor.extractStringRepr(pattern)
      s"""(entry=${entry.toString}, pattern=${patternString})"""
    }
  }

  private object AclPatternExtractor extends AuditEventExtractor[ResourcePattern] {
    def extractStringRepr(pattern: ResourcePattern): String = {
      s"""(${pattern.resourceType}, name=${(if (pattern.name == null) "<any>" else pattern.name)}, patternType=${pattern.patternType})"""
    }
  }

  private object AclPatternFilterExtractor extends AuditEventExtractor[ResourcePatternFilter] {
    def extractStringRepr(pattern: ResourcePatternFilter): String = {
      s"""(${pattern.resourceType}, name=${(if (pattern.name == null) "<any>" else pattern.name)}, patternType=${pattern.patternType})"""
    }
  }
}

object AuditEventExtractorSyntax {

  sealed trait ExtractorSyntax[T] {
    def extractAuditEventPayload(implicit extractor: AuditEventExtractor[T]): String
  }

  implicit class CreateAclsRequestAuditEventExtractorSyntax(val request: CreateAclsRequest) extends ExtractorSyntax[CreateAclsRequest] {
    def extractAuditEventPayload(implicit extractor: AuditEventExtractor[CreateAclsRequest]): String = {
      extractor.extractStringRepr(request)
    }
  }

  implicit class DeleteAclsRequestAuditEventExtractorSyntax(val request: DeleteAclsRequest) extends ExtractorSyntax[DeleteAclsRequest] {
    def extractAuditEventPayload(implicit extractor: AuditEventExtractor[DeleteAclsRequest]): String = {
      extractor.extractStringRepr(request)
    }
  }

  implicit class DescribeAclsRequestAuditEventExtractorSyntax(val request: DescribeAclsRequest) extends ExtractorSyntax[DescribeAclsRequest] {
    def extractAuditEventPayload(implicit extractor: AuditEventExtractor[DescribeAclsRequest]): String = {
      extractor.extractStringRepr(request)
    }
  }

  implicit class AlterConfigsRequestAuditEventExtractorSyntax(val request: AlterConfigsRequest) extends ExtractorSyntax[AlterConfigsRequest] {
    def extractAuditEventPayload(implicit extractor: AuditEventExtractor[AlterConfigsRequest]): String = {
      extractor.extractStringRepr(request)
    }
  }

  implicit class DescribeConfigsRequestAuditEventExtractorSyntax(val request: DescribeConfigsRequest) extends ExtractorSyntax[DescribeConfigsRequest] {
    def extractAuditEventPayload(implicit extractor: AuditEventExtractor[DescribeConfigsRequest]): String = {
      extractor.extractStringRepr(request)
    }
  }

  implicit class CreateTopicsRequestAuditEventExtractorSyntax(val request: CreateTopicsRequest) extends ExtractorSyntax[CreateTopicsRequest] {
    def extractAuditEventPayload(implicit extractor: AuditEventExtractor[CreateTopicsRequest]): String = {
      extractor.extractStringRepr(request)
    }
  }

  implicit class DeleteTopicsRequestAuditEventExtractorSyntax(val request: DeleteTopicsRequest) extends ExtractorSyntax[DeleteTopicsRequest] {
    def extractAuditEventPayload(implicit extractor: AuditEventExtractor[DeleteTopicsRequest]): String = {
      extractor.extractStringRepr(request)
    }
  }

}
