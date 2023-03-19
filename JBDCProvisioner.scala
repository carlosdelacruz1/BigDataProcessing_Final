package io.keepcoding.spark.exercise.provisioner

import java.sql.{Connection, DriverManager}

object JBDCProvisioner {

  def main(args: Array[String]) {
    val IpServer = "IpnecesariaDelPosgreDelGCP"

    // connect to the database
    val driver = "org.postgresql.Driver"
    val url = s"jdbc:postgresql://$IpServer:5432/postgres"
    val username = "postgres"
    val password = "keepcoding"

    // there's probably a better way to do this
    var connection: Connection = null

    try {
      // make the connection
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)

      // create the statement, and run the select query
      val statement = connection.createStatement()
      println("Conexión establecida correctamente!")

      // Metadata table
      println("Creando la tabla user_metadata (id TEXT, model TEXT, version TEXT, location TEXT)")
      statement.execute("CREATE TABLE IF NOT EXISTS user_metadata (id TEXT, name TEXT, email TEXT, quota BIGINT)")

      // Stream aggregation tables
      println("Creando la tabla bytes_agg_antenna (antenna_id TEXT, date TIMESTAMP, sum_antenna_bytes BIGINT)")
      statement.execute("CREATE TABLE IF NOT EXISTS bytes_agg_antenna (antenna_id TEXT, date TIMESTAMP, sum_antenna_bytes BIGINT)")
      println("Creando la tabla bytes_agg_user (id TEXT, date TIMESTAMP, sum_user_bytes BIGINT)")
      statement.execute("CREATE TABLE IF NOT EXISTS bytes_agg_user (id TEXT, date TIMESTAMP, sum_user_bytes BIGINT)")
      println("Creando la tabla bytes_agg_app (app TEXT, date TIMESTAMP, sum_app_bytes BIGINT)")
      statement.execute("CREATE TABLE IF NOT EXISTS bytes_agg_app (app TEXT, date TIMESTAMP, sum_app_bytes BIGINT)")

      // Batch aggregation tables
      println("Creando la tabla bytes_agg_antenna_1h (antenna_id TEXT, date TIMESTAMP, sum_antenna_bytes BIGINT)")
      statement.execute("CREATE TABLE IF NOT EXISTS bytes_agg_antenna_1h (antenna_id TEXT, date TIMESTAMP, sum_antenna_bytes BIGINT)")
      println("Creando la tabla bytes_agg_user_1h (email TEXT, date TIMESTAMP, sum_user_bytes BIGINT)")
      statement.execute("CREATE TABLE IF NOT EXISTS bytes_agg_user_1h (email TEXT, date TIMESTAMP, sum_user_bytes BIGINT)")
      println("Creando la tabla bytes_agg_app_1h (app TEXT, date TIMESTAMP, sum_app_bytes BIGINT)")
      statement.execute("CREATE TABLE IF NOT EXISTS bytes_agg_app_1h (app TEXT, date TIMESTAMP, sum_app_bytes BIGINT)")
      println("Creando la tabla users_over_quota_1h (email TEXT, date TIMESTAMP, quota BIGINT, sum_user_bytes BIGINT)")
      statement.execute("CREATE TABLE IF NOT EXISTS users_over_quota_1h (email TEXT, date TIMESTAMP, quota BIGINT, sum_user_bytes BIGINT)")


      println("Dando de alta la información de usuarios")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000001', 'andres', 'andres@gmail.com', 200000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000002', 'paco', 'paco@gmail.com', 300000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000003', 'juan', 'juan@gmail.com', 100000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000004', 'fede', 'fede@gmail.com', 5000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000005', 'gorka', 'gorka@gmail.com', 200000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000006', 'luis', 'luis@gmail.com', 200000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000007', 'eric', 'eric@gmail.com', 300000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000008', 'carlos', 'carlos@gmail.com', 100000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000009', 'david', 'david@gmail.com', 300000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000010', 'juanchu', 'juanchu@gmail.com', 300000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000011', 'charo', 'charo@gmail.com', 300000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000012', 'delicidas', 'delicidas@gmail.com', 1000000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000013', 'milagros', 'milagros@gmail.com', 200000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000014', 'antonio', 'antonio@gmail.com', 1000000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000015', 'sergio', 'sergio@gmail.com', 1000000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000016', 'maria', 'maria@gmail.com', 1000000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000017', 'cristina', 'cristina@gmail.com', 300000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000018', 'lucia', 'lucia@gmail.com', 300000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000019', 'carlota', 'carlota@gmail.com', 200000)")
      statement.execute("INSERT INTO user_metadata (id, name, email, quota) VALUES ('00000000-0000-0000-0000-000000000020', 'emilio', 'emilio@gmail.com', 200000)")

    } catch {
      case e => e.printStackTrace()
    }
    connection.close()
  }

}
