package changestream

import org.slf4j.LoggerFactory

object IntegrityVerifier {
  protected val log = LoggerFactory.getLogger(getClass)
  protected val sqlTableNameRegex = raw"((?i)insert into|update|delete from) (`?\w+`?\.)?`?(\w+)`?".r.unanchored

  def matchesTableAndSql(tableName: String, sql: String, position: String, source: String = ""): Boolean = {
    val sqlTable = for (m <- sqlTableNameRegex.findFirstMatchIn(sql)) yield m.group(3)

    if(sqlTable.nonEmpty && tableName != sqlTable.get) {
      log.error("[{}] Anomaly found with table name: `{}`, sql table name: `{}` on binlog position `{}`.", source, tableName, sqlTable.get, position)
      false
    } else {
      true
    }
  }
}
