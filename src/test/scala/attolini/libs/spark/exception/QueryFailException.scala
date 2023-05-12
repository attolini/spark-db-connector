package attolini.libs.spark.exception

class QueryFailException(private val message: String = "", private val cause: Throwable = new Throwable("QueryFail"))
    extends RuntimeException(message, cause)
