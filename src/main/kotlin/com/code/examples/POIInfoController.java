package com.code.examples;

import com.kuaishou.op.vc.poi.client.VcPoiBaseClient;
import com.kuaishou.operation.vc.merchant.model.KuaishouPoiVO;
import com.kuaishou.operation.vc.merchant.model.Message;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.vavr.collection.List;
import io.vavr.control.Try;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Pattern;
import java.util.concurrent.TimeUnit;

import static com.kuaishou.operation.vc.merchant.config.PoiKfgUtil.TIMEOUT_CFG;

/**
 * @author joki <linfeng@kuaishou.com>
 */
@RestController
@RequestMapping("/rest/op/vc/merchant/poi-info")
@Slf4j
@Validated
@Api(value = "POI-INFO")
public class POIInfoController {

    @Autowired
    private VcPoiBaseClient poiClient;


    @GetMapping("/{poiIdStr}")
    @ApiOperation(value = "获取快手poi信息", httpMethod = "GET")
    public Message<KuaishouPoiVO> getKuaishouPoiInfoById(
            @NotBlank(message = "poiId cannot be blank")
            @Pattern(regexp = "^[0-9]*$", message = "poiId is illegal")
            @PathVariable String poiIdStr
    ) {
        val poiId = Long.parseLong(poiIdStr);
        return Try.of(() ->
                        new KuaishouPoiVO(
                                poiClient.batchQueryPoiBase(List.of(poiId).toJavaList())
                                        .get(TIMEOUT_CFG.get().getDefaultTime(), TimeUnit.MILLISECONDS)
                                        .getPoiBaseMap()
                                        .getOrDefault(poiId, null)
                        )
        ).onFailure(e -> log.error("get poi detail failed.", e))
        .toEither()
        .fold(Message::error, Message::ok);
    }
}