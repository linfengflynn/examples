package com.code.examples

import com.github.benmanes.caffeine.cache.CacheLoader
import com.github.benmanes.caffeine.cache.Caffeine
import com.kk.social.dao.UserDao
import com.kuaikan.liveim.logger
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.ApplicationListener
import org.springframework.stereotype.Component
import java.util.concurrent.TimeUnit

/**
 * Date: 2019/04/12
 * Description:
 */
@Component
class CaffeineCaches(private val userDao: UserDao) : ApplicationListener<ApplicationReadyEvent> {

    private val logger by logger()

    private val usersRecommendReasonCache = Caffeine.newBuilder()
        .expireAfterWrite(10, TimeUnit.MINUTES)
        .build(CacheLoader<Int, Map<Int, String>> {
            logger.info("load users' recommend reasons cache...")
            loadAllUsersRecommendReasons()
        })

    private val usersActivenessCache = Caffeine.newBuilder()
        .expireAfterWrite(1, TimeUnit.DAYS)
        .build(object : CacheLoader<Int, Long> {
            override fun load(uid: Int): Long {
                val start = System.currentTimeMillis()
                val result = userDao.getActivenessById(uid)
                logger.info("calc activeness for user: {}, cost: {}ms", uid, System.currentTimeMillis() - start)
                return result
            }

            override fun loadAll(uids: MutableIterable<Int>): Map<Int, Long> {
                val start = System.currentTimeMillis()
                val result = uids.map { it to userDao.getActivenessById(it) }.associate { it }
                logger.info("calc activeness batch, cost: {}ms, result: {}", System.currentTimeMillis() - start, result)
                return result
            }
        })

    fun getUsersActivenessMap(uids: Collection<Int>): Map<Int, Long> {
        return try {
            usersActivenessCache.getAll(uids)
        } catch (e: Exception) {
            logger.error("CaffeineCaches getUsersActivenessMap failed!")
            emptyMap()
        }
    }

    private fun loadAllUsersRecommendReasons(): Map<Int, String> {
        val windowed = userDao.getRecommendedUserIds().distinct().windowed(20, 20, true)
        val recommendUsers = runBlocking {
            withTimeoutOrNull(200) {
                windowed.map { async { userDao.getUsersRecommendReasonMap(it) } }.map { it.await() }
            }.orEmpty()
        }
        return recommendUsers.fold(mutableMapOf()) { acc, map ->
            acc.putAll(map)
            acc
        }
    }

    fun getUsersRecommendReasonMap(): Map<Int, String> {
        return try {
            usersRecommendReasonCache.get(0) ?: emptyMap()
        } catch (e: Exception) {
            logger.error("CaffeineCaches getUsersRecommendReasonMap failed!")
            emptyMap()
        }
    }

    override fun onApplicationEvent(event: ApplicationReadyEvent) {
        val caches = runBlocking {
            val c1 = async { getUsersRecommendReasonMap() }
            val c2 = async {
                val uids = userDao.getAllAuthorIds()
                getUsersActivenessMap(uids)
            }
            listOf(c1.await(), c2.await())
        }
        logger.info("UsersRecommendReasonMap loaded, {}", caches[0])
        logger.info("UsersActivenessMap loaded, {}", caches[1])
    }

}