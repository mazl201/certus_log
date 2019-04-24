package kafkaTest.utils

import kafkaTest.entity.LogLineEntity
import org.apache.commons.lang3.StringUtils

object LogEntity {

  def splitStrToLogEntity(logLine : String) : LogLineEntity = {
    if(StringUtils.isNotBlank(logLine) && logLine.length > 3){
      val lineCodes = logLine.split("\\s+")
      var entity = new LogLineEntity()
      entity.contentNow = logLine

      entity.date = lineCodes(0)
      entity.isError = Boolean.box(true)
      entity.time = lineCodes(1)
      entity
    }else{
      null
    }
  }
}
