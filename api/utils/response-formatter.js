/**
 * レスポンスフォーマッター
 * API レスポンスの統一フォーマット
 */

/**
 * 成功レスポンスのフォーマット
 */
function formatResponse(data, message = "Success", metadata = {}) {
  return {
    success: true,
    message,
    data,
    metadata: {
      timestamp: new Date().toISOString(),
      ...metadata,
    },
  }
}

/**
 * エラーレスポンスのフォーマット
 */
function formatError(message, details = null, errorCode = null) {
  const errorResponse = {
    success: false,
    error: {
      message,
      timestamp: new Date().toISOString(),
    },
  }

  if (details) {
    errorResponse.error.details = details
  }

  if (errorCode) {
    errorResponse.error.code = errorCode
  }

  return errorResponse
}

/**
 * ページネーション付きレスポンスのフォーマット
 */
function formatPaginatedResponse(data, pagination, message = "Success") {
  return {
    success: true,
    message,
    data,
    pagination,
    metadata: {
      timestamp: new Date().toISOString(),
    },
  }
}

/**
 * バリデーションエラーのフォーマット
 */
function formatValidationError(errors) {
  return {
    success: false,
    error: {
      message: "Validation failed",
      type: "validation_error",
      details: errors.map((error) => ({
        field: error.param || error.path,
        message: error.msg || error.message,
        value: error.value,
      })),
      timestamp: new Date().toISOString(),
    },
  }
}

module.exports = {
  formatResponse,
  formatError,
  formatPaginatedResponse,
  formatValidationError,
}
