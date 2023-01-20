package changestream

import changestream.helpers.{Base}

class IntegrityVerifierSpec extends Base {
  "when provided table matches sql query" should {
    "it returns true" in {
      val tableName = "users"
      val sql = "INSERT INTO `changestream_test`.`users` (first_name, last_name, company) VALUES (one, two, three)"
      val position = "binary_log.0001:123"

      IntegrityVerifier.matchesTableAndSql(tableName, sql, position) should be(true)
    }

    "when provided sql query is empty" in {
      val tableName = "users"
      val sql = ""
      val position = "binary_log.0001:123"

      IntegrityVerifier.matchesTableAndSql(tableName, sql, position) should be(true)
    }
  }

  "with table mismatch sql query" should {
    "with insert into query" in {
      val tableName = "accounts"
      val sql = "INSERT INTO `users` (first_name, last_name, company) VALUES (one, two, three)"
      val position = "binary_log.0001:123"

      IntegrityVerifier.matchesTableAndSql(tableName, sql, position) should be(false)
    }

    "with update query" in {
      val tableName = "accounts"
      val sql = "UPDATE `users` where x=1"
      val position = "binary_log.0001:123"

      IntegrityVerifier.matchesTableAndSql(tableName, sql, position) should be(false)
    }

    "with delete query" in {
      val tableName = "accounts"
      val sql = "DELETE FROM `changestream_test`.`users` WHERE x=1"
      val position = "binary_log.0001:123"

      IntegrityVerifier.matchesTableAndSql(tableName, sql, position) should be(false)
    }
  }
}
