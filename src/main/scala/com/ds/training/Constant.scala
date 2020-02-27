package com.ds.training

import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}

import org.apache.spark.sql.types.{StringType, StructField, StructType}


object Constant {
  //  val SEGMENT_KEY_TENANT_MAP : String = "segment.key.mapping"

  val PUSH_GATEWAY: String = "prometheus.pushgateway"

  val STREAMING_JOB_NAME: String = "spark.app.name"

  val KAFKA_BROKERS: String = "kafka.brokers"

  val TOPIC_NAME = "kafka.input.topic"

  val BEGIN_DATE: String = "BEGIN_DATE"
  val PROPS_PATH = "PROPS_PATH"

  val S3_SOURCE_ACCESS_KEY = "s3.source.access.key"
  val S3_SOURCE_SECRET_KEY = "s3.source.secret.key"


  val SEGMENT_HOST = "segment.http.host"
  val SEGMENT_TRACK_URL = "segment.http.url.track"
  val SEGMENT_IDENTIFY_URL = "segment.http.url.identify"
  val SEGMENT_BATCH_URL = "segment.http.url.batch"
  val SEGMENT_BATCH_SIZE = "segment.http.batch.size"

  val TENANT_RELATED_DATA = "tenant-related-data"
  val EVENT_DATA_FILTERS = "event-data-field-filters"
  val EVENT_TYPE_FIELD_FILTERS = "event-type-field-filters"
  val DB_CONN_URL = "db.conn.url"
  val DB_CONN_USER_NAME = "db.conn.user"
  val DB_CONN_PASSWORD = "db.conn.pwd"

  val PROMETHEUS_HOST = "prometheus.http.host"

  val MA_USER_ACTIVITY = "MA.USER_ACTIVITY"
  val SA_OBJECT_CHANGED = "SA.OBJECT_CHANGED"
  val MA_SESSION_CREATION = "MA.SESSION_CREATION"
  val SIGN_UP_DRIVERS_LICENSE_ENTERED = "SIGN_UP_DRIVERS_LICENSE_ENTERED"
  val CUSTOMER_FAILED_BACKGROUND_CHECKS = "CUSTOMER_FAILED_BACKGROUND_CHECKS"
  val CUSTOMER_APPROVED = "CUSTOMER_APPROVED"
  val SESSION_CREATION = "SESSION_CREATION"

  val CHANGE_NAME = 1
  val DO_NOTHING = 2
  val SET_VALUES = 3

  val IS_APPROVE_TO_DRIVE = "is_approved_to_drive"

  val USER_AGREEMENT_CONSENT_DECISIONS = "USER-AGREEMENTS-CONSENT-DECISIONS"
  val USER_AGREEMENT_POLICY_DOCUMENT_AGREEMENTS = "USER-AGREEMENTS-POLICY-DOCUMENT-AGREEMENTS"
  val TRIGGER_BATCH_DURATION = "streaming.job.trigger"

  val EXCLUDE_TENANTS = "streaming.exclude.tenants"

  val PROMOTIONS_ACTIVITY = "PROMOTIONS_ACTIVITY"
  val PAYMENT_LIFECYCLE = "PAYMENT_LIFECYCLE"

  val APP_SESSION_STARTED = "APP_SESSION_STARTED"

  val PROMO_PAYMENT_EVENT_NAMES = Seq(PROMOTIONS_ACTIVITY, PAYMENT_LIFECYCLE)

  val SEGMENT_EVENT_NAMES_DICT: Map[String, String] = Map(
    "BLOCKED" -> "Member Blocked",
    "CANCELLATION" -> "Trip Cancellation",
    "CANCEL_RESERVATION" -> "Cancel Reservation",
    "CUSTOMER_APPROVED" -> "Unblocked",
    "CUSTOMER_REGISTERED" -> "Sign Up Completed",
    "CUSTOMER_RESERVATION" -> "Reservation",
    "DISTANCE_NEAREST_VEHICLE" -> "Distance to Nearest Vehicle",
    "FIRST_RENTAL_COMPLETED" -> "1st Trip Completed",
    "HAS_TRIP_AFTER_FIRST_MONTH" -> "Has Trip After 1st Month",
    "INVITES" -> "Referral Activated",
    "LOGIN" -> "Login",
    "MEMBER_SUPPORT" -> "Support Call",
    "PAYMENT" -> "Payment Collected",
    "PROMO_CODE_ENTRY" -> "Promo Code Entered",
    "PROMO_CODE_REDEEMED" -> "Promo Redeemed",
    "RATING" -> "Trip Rating",
    "RENTAL_RESERVATION" -> "Trip Reserved",
    "RENTAL_STARTED" -> "Trip Started",
    "RENTAL_COMPLETED" -> "Trip Completed",
    "RENTAL_CANCELLATION" -> "Trip Cancelled",
    "RESERVE_VEHICLE" -> "Reserve Vehicle",
    SESSION_CREATION -> "Session Creation",
    "SIGN_UP_INITIATED" -> "Sign Up Initiated",
    "SIGN_UP_CREDIT_CARD_ENTERED" -> "Sign Up Payment Entered",
    "SIGN_UP_PAYMENT_SKIPPED" -> "Sign Up Payment Skipped",
    "REFERRAL_LINK_SHARE" -> "Referral Link Shared",
    "MEMBER_CREDIT_CARD_UPDATE" -> "Payment Updated",
    CUSTOMER_FAILED_BACKGROUND_CHECKS -> "Sign Up Declined",
    SIGN_UP_DRIVERS_LICENSE_ENTERED -> "Sign Up Driver’s License Entered",
    "DRIVERS_LICENSE_UPDATED" -> "Driver’s License Updated",
    "LOCATION_ENABLED" -> "Location Enabled",
    "APP_CRASH" -> "App Crash"
  )

  val CONDITIONAL_EVENTS: Seq[String] = Seq(
    CUSTOMER_APPROVED,
    "CUSTOMER_FAILED_BACKGROUND_CHECKS",
    "SESSION_CREATION",
    "SIGN_UP_DRIVERS_LICENSE_ENTERED"
  )

  val ALLOWED_TYPE_TRANSFORMATION = Seq(
    MA_USER_ACTIVITY,
    SA_OBJECT_CHANGED
  )


  val CONDITIONAL_SEGMENT_EVENT_NAMES_DICT = Map {
    CUSTOMER_APPROVED -> "Sign Up Approved"
  }


  val USER_AGREEMENT = Seq(
    USER_AGREEMENT_CONSENT_DECISIONS,
    USER_AGREEMENT_POLICY_DOCUMENT_AGREEMENTS
  )

  val ADVERTISE_ID_EVENTS_NAMES = Seq {
    MA_USER_ACTIVITY
  }


  //  val CHECKPOINT_DIR = "spark.kafka.checkpoint.dir"

  val DB_LISTENER_TOPIC_NAME = "db.listener.topic.name"

  val START_OFFSET = "spark.startingOffsets"

  val SOURCE_HAS_NO_NAME = "SOURCE_HAS_NO_NAME"
  val dateTimeFormatInputData: DateTimeFormatter = new DateTimeFormatterBuilder()
    .parseCaseInsensitive
    .append(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
    .appendOptional(new DateTimeFormatterBuilder().appendPattern(".SSSSSS").toFormatter())
    .appendOffsetId
    .toFormatter()
  val dateTimeFormatOutputData: DateTimeFormatter = new DateTimeFormatterBuilder()
    .parseCaseInsensitive
    .append(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
    .appendLiteral('Z')
    .toFormatter()

  val AIRPORT_TOPIC_NAME = "topic.name.airport"
  val USER_TOPIC_NAME = "topic.name.user"
  val RESULT_TOPIC_NAME = "topic.name.result"

  val AIRPORT_CSV_PATH = "file.path.airport"

  val EARTH_RADIUS = 6371
  val prefix = "/Users/dshingav/openSourceCode/CoordinatesComparison/"
  val USER_DATA_DIR = "src/main/resources/user/"
  val AIRPORT_DATA_DIR = "src/main/resources/airport/"
  val USER_DATA_FILE_PATH = "src/main/resources/user-geo-sample.csv"
  val AIRPORT_DATA_FILE_PATH = "src/main/resources/optd-airports-sample.csv"

  val AIRPORT_ID_COLUMN = "airportId"
  val AIRPORT_LAT_COLUMN = "airportLat"
  val AIRPORT_LNG_COLUMN = "airportLng"

  val USER_ID_COLUMN = "userId"
  val USER_LAT_COLUMN = "userLat"
  val USER_LNG_COLUMN = "userLng"

  val stdSchema = StructType(
    Array(StructField("id", StringType),
      StructField("lat", StringType),
      StructField("lng", StringType)))

}
