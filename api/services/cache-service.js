/**
 * キャッシュサービス
 * メモリ内キャッシュによる高速化
 */

const NodeCache = require("node-cache")
const winston = require("winston")

const logger = winston.createLogger({
  level: "info",
  format: winston.format.json(),
  transports: [new winston.transports.Console()],
})

class CacheService {
  constructor() {
    // デフォルト設定: TTL 10分、チェック間隔 2分
    this.cache = new NodeCache({
      stdTTL: 600, // 10分
      checkperiod: 120, // 2分
      useClones: false,
    })

    // キャッシュ統計
    this.stats = {
      hits: 0,
      misses: 0,
      sets: 0,
      deletes: 0,
    }

    // イベントリスナー
    this.cache.on("set", (key, value) => {
      this.stats.sets++
      logger.debug("キャッシュ設定", { key, size: JSON.stringify(value).length })
    })

    this.cache.on("del", (key, value) => {
      this.stats.deletes++
      logger.debug("キャッシュ削除", { key })
    })

    this.cache.on("expired", (key, value) => {
      logger.debug("キャッシュ期限切れ", { key })
    })

    logger.info("キャッシュサービス初期化完了")
  }

  /**
   * キャッシュから値を取得
   */
  get(key) {
    try {
      const value = this.cache.get(key)

      if (value !== undefined) {
        this.stats.hits++
        logger.debug("キャッシュヒット", { key })
        return value
      } else {
        this.stats.misses++
        logger.debug("キャッシュミス", { key })
        return null
      }
    } catch (error) {
      logger.error("キャッシュ取得でエラー", { error: error.message, key })
      return null
    }
  }

  /**
   * キャッシュに値を設定
   */
  set(key, value, ttl = null) {
    try {
      const success = this.cache.set(key, value, ttl)

      if (success) {
        logger.debug("キャッシュ設定成功", { key, ttl })
      } else {
        logger.warning("キャッシュ設定失敗", { key })
      }

      return success
    } catch (error) {
      logger.error("キャッシュ設定でエラー", { error: error.message, key })
      return false
    }
  }

  /**
   * キャッシュから値を削除
   */
  delete(key) {
    try {
      const deleteCount = this.cache.del(key)
      logger.debug("キャッシュ削除", { key, deleteCount })
      return deleteCount > 0
    } catch (error) {
      logger.error("キャッシュ削除でエラー", { error: error.message, key })
      return false
    }
  }

  /**
   * パターンマッチでキャッシュを削除
   */
  deletePattern(pattern) {
    try {
      const keys = this.cache.keys()
      const matchingKeys = keys.filter((key) => key.includes(pattern))

      if (matchingKeys.length > 0) {
        const deleteCount = this.cache.del(matchingKeys)
        logger.info("パターンマッチキャッシュ削除", { pattern, deleteCount })
        return deleteCount
      }

      return 0
    } catch (error) {
      logger.error("パターンマッチキャッシュ削除でエラー", { error: error.message, pattern })
      return 0
    }
  }

  /**
   * キャッシュをクリア
   */
  clear() {
    try {
      this.cache.flushAll()
      logger.info("キャッシュ全削除完了")
      return true
    } catch (error) {
      logger.error("キャッシュ全削除でエラー", { error: error.message })
      return false
    }
  }

  /**
   * キャッシュ統計情報を取得
   */
  getStats() {
    const cacheStats = this.cache.getStats()

    return {
      ...this.stats,
      keys: cacheStats.keys,
      hits_ratio:
        this.stats.hits + this.stats.misses > 0
          ? ((this.stats.hits / (this.stats.hits + this.stats.misses)) * 100).toFixed(2) + "%"
          : "0%",
      memory_usage: process.memoryUsage(),
      cache_keys: this.cache.keys().length,
    }
  }

  /**
   * TTL付きキャッシュ取得または設定
   */
  async getOrSet(key, fetchFunction, ttl = 600) {
    try {
      // キャッシュから取得試行
      let value = this.get(key)

      if (value !== null) {
        return value
      }

      // キャッシュにない場合は関数を実行
      logger.debug("キャッシュミスのため関数実行", { key })
      value = await fetchFunction()

      // 結果をキャッシュに保存
      this.set(key, value, ttl)

      return value
    } catch (error) {
      logger.error("getOrSetでエラー", { error: error.message, key })
      throw error
    }
  }

  /**
   * 複数キーの一括取得
   */
  mget(keys) {
    try {
      const results = {}

      keys.forEach((key) => {
        const value = this.get(key)
        if (value !== null) {
          results[key] = value
        }
      })

      logger.debug("複数キー取得", {
        requestedKeys: keys.length,
        foundKeys: Object.keys(results).length,
      })

      return results
    } catch (error) {
      logger.error("複数キー取得でエラー", { error: error.message })
      return {}
    }
  }

  /**
   * 複数キーの一括設定
   */
  mset(keyValuePairs, ttl = null) {
    try {
      let successCount = 0

      Object.entries(keyValuePairs).forEach(([key, value]) => {
        if (this.set(key, value, ttl)) {
          successCount++
        }
      })

      logger.debug("複数キー設定", {
        totalKeys: Object.keys(keyValuePairs).length,
        successCount,
      })

      return successCount
    } catch (error) {
      logger.error("複数キー設定でエラー", { error: error.message })
      return 0
    }
  }
}

module.exports = CacheService
