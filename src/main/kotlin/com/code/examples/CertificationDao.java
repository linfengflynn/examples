package com.code.examples;

import com.kuaishou.framework.util.ObjectMapperUtils;
import com.kuaishou.op.vc.jooq.tables.pojos.JVcCertificationInfo;
import com.kuaishou.op.vc.merchant.enums.PoiStatusEnum;
import com.kuaishou.op.vc.merchant.enums.ShopTypeEnum;
import com.kuaishou.operation.vc.merchant.enums.ManageStatus;
import com.kuaishou.operation.vc.merchant.model.AgentCertificationInfoParam;
import com.kuaishou.operation.vc.merchant.model.CertificationInfoParam;
import io.vavr.control.Option;
import org.springframework.stereotype.Repository;

import java.util.*;

import static com.kuaishou.op.vc.jooq.Tables.VC_CERTIFICATION_INFO;

/**
 * @author joki <linfeng@kuaishou.com>
 */
@Repository
public class CertificationDao extends AbstractDAO {

    public Option<JVcCertificationInfo> getNormalCertById(long id) {
        return Option.of(getSlaveDsl().selectFrom(VC_CERTIFICATION_INFO)
                .where(VC_CERTIFICATION_INFO.ID.eq(id))
                .and(VC_CERTIFICATION_INFO.MANAGE_STATUS.eq(ManageStatus.NORMAL.getCode()))
                .fetchOneInto(JVcCertificationInfo.class));
    }

    public Option<JVcCertificationInfo> getCertRecordById(long id) {
        return Option.of(getSlaveDsl().selectFrom(VC_CERTIFICATION_INFO)
                .where(VC_CERTIFICATION_INFO.ID.eq(id))
                .fetchOneInto(JVcCertificationInfo.class));
    }

    public Option<JVcCertificationInfo> getLatestCertByAssociatedId(long associatedId) {
        return Option.of(getSlaveDsl().selectFrom(VC_CERTIFICATION_INFO)
                .where(VC_CERTIFICATION_INFO.ASSOCIATED_ID.eq(associatedId))
                .and(VC_CERTIFICATION_INFO.MANAGE_STATUS.eq(ManageStatus.NORMAL.getCode()))
                .orderBy(VC_CERTIFICATION_INFO.UPDATE_TIME.desc())
                .limit(1)
                .fetchOneInto(JVcCertificationInfo.class));
    }

    public long createCert(CertificationInfoParam param) {
        return getMasterDsl().insertInto(VC_CERTIFICATION_INFO)
                .set(VC_CERTIFICATION_INFO.ASSOCIATED_ID, Long.parseLong(param.getPoiId()))
                .set(VC_CERTIFICATION_INFO.SHOP_TYPE, ShopTypeEnum.SELF.getCode())
                .set(VC_CERTIFICATION_INFO.CATE, param.getCategory())
                .set(VC_CERTIFICATION_INFO.ID_CARD_PICS, ObjectMapperUtils.toJSON(param.getIdCardPics()))
                .set(VC_CERTIFICATION_INFO.CERT_PIC, param.getCertPic())
                .set(VC_CERTIFICATION_INFO.CORP_NAME, param.getCorpName())
                .set(VC_CERTIFICATION_INFO.CERT_INFO_EXTRA, param.getExtra())
                .set(VC_CERTIFICATION_INFO.CENSOR_STATUS, PoiStatusEnum.WAIT_CONFIRM.getCode())
                .set(VC_CERTIFICATION_INFO.CREATE_TIME, System.currentTimeMillis())
                .set(VC_CERTIFICATION_INFO.UPDATE_TIME, System.currentTimeMillis())
                .set(VC_CERTIFICATION_INFO.MANAGE_STATUS, ManageStatus.NORMAL.getCode())
                .set(VC_CERTIFICATION_INFO.BUSINESS_PHONE, param.getBusinessPhone())
                .returning(VC_CERTIFICATION_INFO.ID)
                .fetch() // jdbc's bug, not jooq, see https://github.com/jOOQ/jOOQ/issues/6764
                .get(0)
                .getId();
    }

    public void updateCert(String certId, CertificationInfoParam param) {
        getMasterDsl().update(VC_CERTIFICATION_INFO)
                .set(VC_CERTIFICATION_INFO.ASSOCIATED_ID, Long.parseLong(param.getPoiId()))
                .set(VC_CERTIFICATION_INFO.SHOP_TYPE, ShopTypeEnum.SELF.getCode())
                .set(VC_CERTIFICATION_INFO.CATE, param.getCategory())
                .set(VC_CERTIFICATION_INFO.ID_CARD_PICS, ObjectMapperUtils.toJSON(param.getIdCardPics()))
                .set(VC_CERTIFICATION_INFO.CERT_PIC, param.getCertPic())
                .set(VC_CERTIFICATION_INFO.CORP_NAME, param.getCorpName())
                .set(VC_CERTIFICATION_INFO.CERT_INFO_EXTRA, param.getExtra())
                .set(VC_CERTIFICATION_INFO.CENSOR_STATUS, PoiStatusEnum.WAIT_CONFIRM.getCode())
                .set(VC_CERTIFICATION_INFO.UPDATE_TIME, System.currentTimeMillis())
                .set(VC_CERTIFICATION_INFO.MANAGE_STATUS, ManageStatus.NORMAL.getCode())
                .set(VC_CERTIFICATION_INFO.BUSINESS_PHONE, param.getBusinessPhone())
                .where(VC_CERTIFICATION_INFO.ID.eq(Long.parseLong(certId)))
                .execute();
    }

    public long createAgentCert(long shopId, AgentCertificationInfoParam param) {
        return getMasterDsl().insertInto(VC_CERTIFICATION_INFO)
                .set(VC_CERTIFICATION_INFO.ASSOCIATED_ID, shopId)
                .set(VC_CERTIFICATION_INFO.SHOP_TYPE, ShopTypeEnum.AGENT.getCode())
                .set(VC_CERTIFICATION_INFO.CATE, param.getCategory())
                .set(VC_CERTIFICATION_INFO.ID_CARD_PICS, ObjectMapperUtils.toJSON(param.getIdCardPics()))
                .set(VC_CERTIFICATION_INFO.CERT_PIC, param.getCertPic())
                .set(VC_CERTIFICATION_INFO.CORP_NAME, param.getCorpName())
                .set(VC_CERTIFICATION_INFO.CERT_INFO_EXTRA, param.getExtra())
                .set(VC_CERTIFICATION_INFO.CENSOR_STATUS, PoiStatusEnum.WAIT_CONFIRM.getCode())
                .set(VC_CERTIFICATION_INFO.CREATE_TIME, System.currentTimeMillis())
                .set(VC_CERTIFICATION_INFO.UPDATE_TIME, System.currentTimeMillis())
                .set(VC_CERTIFICATION_INFO.MANAGE_STATUS, ManageStatus.NORMAL.getCode())
                .set(VC_CERTIFICATION_INFO.BUSINESS_PHONE, param.getBusinessPhone())
                .returning(VC_CERTIFICATION_INFO.ID)
                .fetch()
                .get(0)
                .getId();
    }


    public void updateAgentCert(long shopId, String censorId, AgentCertificationInfoParam param) {
        getMasterDsl().update(VC_CERTIFICATION_INFO)
                .set(VC_CERTIFICATION_INFO.SHOP_TYPE, ShopTypeEnum.AGENT.getCode())
                .set(VC_CERTIFICATION_INFO.CATE, param.getCategory())
                .set(VC_CERTIFICATION_INFO.ID_CARD_PICS, ObjectMapperUtils.toJSON(param.getIdCardPics()))
                .set(VC_CERTIFICATION_INFO.CERT_PIC, param.getCertPic())
                .set(VC_CERTIFICATION_INFO.CORP_NAME, param.getCorpName())
                .set(VC_CERTIFICATION_INFO.CERT_INFO_EXTRA, param.getExtra())
                .set(VC_CERTIFICATION_INFO.CENSOR_STATUS, PoiStatusEnum.WAIT_CONFIRM.getCode())
                .set(VC_CERTIFICATION_INFO.UPDATE_TIME, System.currentTimeMillis())
                .set(VC_CERTIFICATION_INFO.MANAGE_STATUS, ManageStatus.NORMAL.getCode())
                .set(VC_CERTIFICATION_INFO.BUSINESS_PHONE, param.getBusinessPhone())
                .where(VC_CERTIFICATION_INFO.ID.eq(Long.parseLong(censorId)))
                .and(VC_CERTIFICATION_INFO.ASSOCIATED_ID.eq(shopId))
                .execute();
    }

    public void updateCensorStatus(long censorId, PoiStatusEnum status, String reason) {
        getMasterDsl().update(VC_CERTIFICATION_INFO)
                .set(VC_CERTIFICATION_INFO.CENSOR_STATUS, status.getCode())
                .set(VC_CERTIFICATION_INFO.DENY_REASON, reason)
                .set(VC_CERTIFICATION_INFO.UPDATE_TIME, System.currentTimeMillis())
                .where(VC_CERTIFICATION_INFO.ID.eq(censorId))
                .and(VC_CERTIFICATION_INFO.MANAGE_STATUS.eq(ManageStatus.NORMAL.getCode()))
                .execute();
    }

    public List<JVcCertificationInfo> listByAssociatedIds(Collection<Long> associatedIds) {
        return Optional.ofNullable(getSlaveDsl().selectFrom(VC_CERTIFICATION_INFO)
                .where(VC_CERTIFICATION_INFO.ASSOCIATED_ID.in(associatedIds))
                .and(VC_CERTIFICATION_INFO.MANAGE_STATUS.eq(ManageStatus.NORMAL.getCode()))
                .fetchInto(JVcCertificationInfo.class))
                .orElse(Collections.emptyList());
    }

    public List<JVcCertificationInfo> listByAssociatedIdAndCensorStatus(long associatedId, int censorStatus) {
        return Optional.ofNullable(getSlaveDsl().selectFrom(VC_CERTIFICATION_INFO)
                .where(VC_CERTIFICATION_INFO.ASSOCIATED_ID.eq(associatedId))
                .and(VC_CERTIFICATION_INFO.MANAGE_STATUS.eq(ManageStatus.NORMAL.getCode()))
                .and(VC_CERTIFICATION_INFO.CENSOR_STATUS.eq(censorStatus))
                .fetchInto(JVcCertificationInfo.class))
                .orElse(Collections.emptyList());
    }

    public void updateManageStatus(long id, ManageStatus manageStatus) {
        getMasterDsl().update(VC_CERTIFICATION_INFO)
                .set(VC_CERTIFICATION_INFO.UPDATE_TIME, System.currentTimeMillis())
                .set(VC_CERTIFICATION_INFO.MANAGE_STATUS, manageStatus.getCode())
                .where(VC_CERTIFICATION_INFO.ID.eq(id))
                .execute();
    }

    public List<JVcCertificationInfo> listAllByAssociatedId(long associated) {
        return Optional.ofNullable(getSlaveDsl().selectFrom(VC_CERTIFICATION_INFO)
                .where(VC_CERTIFICATION_INFO.ASSOCIATED_ID.eq(associated))
                .fetchInto(JVcCertificationInfo.class))
                .orElse(Collections.emptyList());
    }

    /**
     * 仅线下后门接口使用，线上不要调用
     */
    public int deleteByIds(Set<Long> certIds) {
        return getMasterDsl().deleteFrom(VC_CERTIFICATION_INFO).where(VC_CERTIFICATION_INFO.ID.in(certIds)).execute();
    }

    /**
     * 仅后门接口使用，线上业务不要调用
     */
    public List<JVcCertificationInfo> listAllByCensorStatus(int censorStatus) {
        return Optional.ofNullable(getSlaveDsl().selectFrom(VC_CERTIFICATION_INFO)
                .where(VC_CERTIFICATION_INFO.MANAGE_STATUS.eq(ManageStatus.NORMAL.getCode()))
                .and(VC_CERTIFICATION_INFO.CENSOR_STATUS.eq(censorStatus))
                .fetchInto(JVcCertificationInfo.class))
                .orElse(Collections.emptyList());
    }

}