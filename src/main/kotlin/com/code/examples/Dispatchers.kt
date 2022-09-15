import com.kuaishou.framework.concurrent.DynamicThreadExecutor.dynamic
import com.kuaishou.operation.vc.poi.config.PoiKfgUtil.THREAD_NUM
import kotlinx.coroutines.asCoroutineDispatcher

/**
 * @author joki <linfeng@kuaishou.com>
 */

val Mapping = dynamic(THREAD_NUM::get, "poi-mapping-thread-pool").asCoroutineDispatcher()

val Info = dynamic(THREAD_NUM::get, "poi-info-thread-pool").asCoroutineDispatcher()