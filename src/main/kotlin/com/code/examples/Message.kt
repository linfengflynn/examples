package com.code.examples

import com.kuaishou.old.exception.ServiceException

class Message<T> private constructor(
    val status: Int,
    val message: String,
    val data: T? = null
) {

    private constructor(result: ResultCode, message: String? = null, data: T? = null) : this(
        result.code, message ?: result.message, data
    )

    companion object {

        @JvmStatic
        fun <T> ok(): Message<T> {
            return Message(result = ResultCode.SUCCESS)
        }

        @JvmStatic
        fun <T> ok(data: T?): Message<T> {
            return Message(
                result = ResultCode.SUCCESS,
                data = data
            )
        }

        @JvmStatic
        fun <T> error(result: ResultCode): Message<T> {
            return Message(result = result)
        }

        @JvmStatic
        fun <T> error(result: ResultCode, message: String?): Message<T> {
            return Message(result = result, message = message)
        }

        @JvmStatic
        fun <T> error(result: ResultCode, data: T?): Message<T> {
            return Message(result = result, data = data)
        }

        @JvmStatic
        fun <T> error(result: ResultCode, message: String?, data: T?): Message<T> {
            return Message(result = result, message = message, data = data)
        }

        @JvmStatic
        fun <T> error(e: Throwable): Message<T> {
            return when(e) {
                is ServiceException -> Message(e.code, e.overrideMessage)
                else -> Message(ResultCode.SERVICE_BUSY.code, ResultCode.SERVICE_BUSY.message)
            }
        }
    }
}