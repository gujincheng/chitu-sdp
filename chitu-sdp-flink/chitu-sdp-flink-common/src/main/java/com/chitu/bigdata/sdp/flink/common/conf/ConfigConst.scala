package com.chitu.bigdata.sdp.flink.common.conf

object ConfigConst {

  /**
   *
   * about parameter...
   */

  val KEY_APP_HOME = "app.home"

  val KEY_HOST = "host"

  val KEY_PORT = "port"

  val KEY_DB = "db"

  val KEY_USER = "user"

  val KEY_PASSWORD = "password"

  val KEY_TIMEOUT = "timeout"

  val KEY_JOB_ID = "jobId"

  val KEY_SEMANTIC = "semantic"

  /**
   * sign....
   */
  val SIGN_COLON = ":"

  val SIGN_SEMICOLON = ";"

  val SIGN_COMMA = ","

  val SIGN_EMPTY = ""

  /**
   * kerberos
   */
  val KEY_KERBEROS = "kerberos"

  /**
   * hadoop.security.authentication
   */
  val KEY_HADOOP_SECURITY_AUTHENTICATION = "hadoop.security.authentication"

  val KEY_SECURITY_KERBEROS_ENABLE = "security.kerberos.login.enable"

  val KEY_SECURITY_KERBEROS_KEYTAB = "security.kerberos.login.keytab"

  val KEY_SECURITY_KERBEROS_PRINCIPAL = "security.kerberos.login.principal"

  val KEY_SECURITY_KERBEROS_KRB5_CONF = "security.kerberos.login.krb5"

  val KEY_JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf"


  // flink
  def KEY_APP_CONF(prefix: String = null): String = if (prefix == null) "conf" else s"${prefix}conf"

  def KEY_FLINK_CONF(prefix: String = null): String = if (prefix == null) "flink.conf" else s"${prefix}flink.conf"

  def KEY_APP_NAME(prefix: String = null): String = if (prefix == null) "app.name" else s"${prefix}app.name"

  def KEY_FLINK_SQL(prefix: String = null): String = if (prefix == null) "sql" else s"${prefix}sql"

  def KEY_FLINK_PARALLELISM(prefix: String = null): String = if (prefix == null) "parallelism.default" else s"${prefix}parallelism.default"

  def KEY_YAML_CONF(prefix: String = null): String = if (prefix == null) "yaml.conf" else s"${prefix}yaml.conf"

  val KEY_FLINK_DEPLOYMENT_PROPERTY_PREFIX = "flink.deployment.property."

  val KEY_FLINK_DEPLOYMENT_OPTION_PREFIX = "flink.deployment.option."

  val KEY_FLINK_APP_NAME = "yarn.application.name"




  /**
   * about config Kafka
   */

  val KAFKA_SINK_PREFIX = "kafka.sink."

  val KEY_KAFKA_TOPIC = "topic"

  val KEY_ALIAS = "alias"

  /**
   * about config jdbc...
   */
  val KEY_JDBC_PREFIX = "jdbc."
  val KEY_JDBC_DRIVER = "driverClassName"
  val KEY_JDBC_DATABASE = "database"
  val KEY_JDBC_URL = "jdbcUrl"
  val KEY_JDBC_USER = "username"
  val KEY_JDBC_PASSWORD = "password"
  val KEY_JDBC_INSERT_BATCH = "batch.size"
  val DEFAULT_JDBC_INSERT_BATCH = 1

  /**
   * about config HBase
   */
  val HBASE_PREFIX = "hbase."


  /**
   * about influx
   */
  val INFLUX_PREFIX = "influx."





  val SDP_HDFS_WORKSPACE_DEFAULT = "/sdp"

  val KEY_SDP_HDFS_WORKSPACE = "sdp.hdfs.workspace"

  val YAML_TABLE_CONF_PREF ="table."



  lazy val HDFS_WORKSPACE: String = {
    val workspace = System.getProperties.getProperty(KEY_SDP_HDFS_WORKSPACE, SDP_HDFS_WORKSPACE_DEFAULT)
    require(!workspace.startsWith("hdfs://"))
    workspace
  }

  lazy val APP_PLUGINS = s"$HDFS_WORKSPACE/plugins"

  /**
   * 存放不同版本flink相关的jar
   */

  lazy val APP_WORKSPACE = s"$HDFS_WORKSPACE/workspace"

  lazy val APP_FLINK = s"$HDFS_WORKSPACE/flink"

  lazy val APP_SAVEPOINTS = s"$HDFS_WORKSPACE/savepoints"

  /**
   * 存放全局公共的jar
   */
  lazy val APP_JARS = s"$HDFS_WORKSPACE/jars"

}



