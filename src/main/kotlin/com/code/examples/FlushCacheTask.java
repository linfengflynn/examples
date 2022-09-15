package com.code.examples;

import static com.kuaishou.operation.vc.poi.config.PoiKfgUtil.TASK_PAGE_SIZE;
import static com.kuaishou.operation.vc.poi.config.PoiKfgUtil.WARMUP_SWITCH;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.jooq.JooqAutoConfiguration;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.kuaishou.framework.concurrent.ExecutorsEx;
import com.kuaishou.infra.boot.KsSpringApplication;
import com.kuaishou.infra.boot.autoconfigure.KsBootApplication;
import com.kuaishou.infra.scheduler.client.Task;
import com.kuaishou.infra.scheduler.client.TaskContext;
import com.kuaishou.operation.vc.poi.redis.KuaiShouPoiRedisReadService;
import com.kuaishou.operation.vc.poi.redis.KuaiShouPoiRedisWriteService;
import com.kuaishou.operation.vc.poi.service.PoiIncrementService;
import com.kuaishou.vc.kuaishou.poi.jooq.tables.pojos.JPubPoiChinaDfIncrementTemp;

import io.vavr.collection.List;
import io.vavr.concurrent.Future;
import io.vavr.control.Option;
import kuaishou.common.BizDef;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import lombok.var;

/**
 * @author joki <linfeng@kuaishou.com>
 */
@Slf4j
@KsBootApplication(scanBasePackages = "com.kuaishou", exclude = JooqAutoConfiguration.class)
public class FlushCacheTask implements Task {

    private final ListeningExecutorService executor = MoreExecutors.listeningDecorator(
            ExecutorsEx.newBlockingThreadPool(1000, 1000, "poi-auto-bind-thread-pool")
    );

    @Autowired
    private KuaiShouPoiRedisWriteService redisWriteService;
    @Autowired
    private KuaiShouPoiRedisReadService redisReadService;
    @Autowired
    private PoiIncrementService poiIncreService;

    @Override
    public void execute(TaskContext context) {
        log.info("flush cache task start...");
        val stopwatch = Stopwatch.createStarted();
        val pdate = StringUtils.isBlank(context.args())
                ? DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDateTime.now().minusDays(1))
                : context.args();
        var pageSize = Integer.MAX_VALUE;
        var startId = 0L;
        log.info("this time will process {} records of pdate {}", poiIncreService.getCountByPdate(pdate), pdate);
        while (pageSize >= TASK_PAGE_SIZE.get()) {
            if (!WARMUP_SWITCH.get()) {
                return;
            }
            val sw = Stopwatch.createStarted();
            val page = poiIncreService.getListByPage(pdate, startId, TASK_PAGE_SIZE.get())
                    .sortBy(JPubPoiChinaDfIncrementTemp::getId);
            pageSize = page.size();
            startId = page.lastOption()
                    .map(JPubPoiChinaDfIncrementTemp::getId)
                    .getOrElse(Long.MAX_VALUE);
            val sum = page.map(it -> Future.of(executor, () -> {
                val poiId = it.getPoiIdLong();
                val map = redisReadService.getPoiInfoByIdLists(List.of(poiId).toJavaList());
                return Option.of(map.getOrDefault(poiId, null))
                        .map(entity -> {
                            if (Option.of(entity.getKuaiShouPoiModel()).isEmpty()) {
                                return 0;
                            }
                            entity.getKuaiShouPoiModel().setSource(it.getSource());
                            redisWriteService.savePoi(poiId, entity);
                            return 1;
                        }).getOrElse(0);
            })).map(Future::get)
            .sum();
            log.info("this loop write {} records, cost: {}ms.", sum, sw.elapsed(TimeUnit.MILLISECONDS));
        }
        log.info("flush cache task end, cost: {}ms", stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    @Override
    public String name() {
        return "FlushCacheTask";
    }

    @Override
    public BizDef bizDef() {
        return BizDef.OPERATION_VERTICAL_TYPE_LOCAL;
    }

    public static void main(String[] args) {
        KsSpringApplication.justRun(args);
    }
}