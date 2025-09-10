package rocks.frieler.kraftsql.bq.engine.api

import com.google.cloud.bigquery.ConnectionProperty
import com.google.cloud.bigquery.QueryJobConfiguration

internal fun QueryJobConfiguration.Builder.setConnectionProperty(property: ConnectionProperty) : QueryJobConfiguration.Builder =
    setConnectionProperties((this.build().connectionProperties?.filterNot { it.key == property.key } ?: emptyList()) + property)
