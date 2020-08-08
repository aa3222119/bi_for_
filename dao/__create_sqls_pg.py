"""CREATE TABLE "public"."tbi_l6_mt_predict_model_record_1_r" (
  "id" uuid NOT NULL,
  "merchant_code" varchar(20) COLLATE "pg_catalog"."default" NOT NULL DEFAULT ''::character varying,
  "mid" uuid NOT NULL,
  "pay_time" timestamp(6) NOT NULL,
  "predict_type" int4,
  "predict_time" timestamp(6),
  "predict_dt" numeric(20,6),
  "p_accuracy" numeric(20,6),
  "real_time" timestamp(6),
  "real_dt" numeric(20,6),
  "r_err" numeric(20,6),
  "d_err" int4,
  "err_rate1" numeric(20,6),
  "err_rate2" numeric(20,6),
  "model_para" text COLLATE "pg_catalog"."default",
  "model_choice" text COLLATE "pg_catalog"."default",
  "obligate" text COLLATE "pg_catalog"."default",
  "create_time" timestamp(6) NOT NULL DEFAULT clock_timestamp(),
  "update_time" timestamp(6) NOT NULL DEFAULT clock_timestamp(),
  "status_time" timestamp(6) NOT NULL DEFAULT clock_timestamp(),
  "status" int2 NOT NULL DEFAULT 0,
  CONSTRAINT "pk_tbi_l6_mt_predict_model_record_1_r" PRIMARY KEY ("id")
)
;

ALTER TABLE "public"."tbi_l6_mt_predict_model_record_1_r"
  OWNER TO "postgres";

CREATE INDEX "ix_tbi_l6_mt_predict_model_record_1_r_mid" ON "public"."tbi_l6_mt_predict_model_record_1_r" USING btree (
  "mid" "pg_catalog"."uuid_ops" ASC NULLS LAST
);

CREATE INDEX "ix_tbi_l6_mt_predict_model_record_1_r_mid_pay_time" ON "public"."tbi_l6_mt_predict_model_record_1_r" USING btree (
  "mid" "pg_catalog"."uuid_ops" ASC NULLS LAST,
  "pay_time" "pg_catalog"."timestamp_ops" ASC NULLS LAST
);

CREATE INDEX "ix_tbi_l6_mt_predict_model_record_1_r_pay_time" ON "public"."tbi_l6_mt_predict_model_record_1_r" USING btree (
  "pay_time" "pg_catalog"."timestamp_ops" ASC NULLS LAST
);

CREATE INDEX "ix_tbi_l6_mt_predict_model_record_1_r_update_time" ON "public"."tbi_l6_mt_predict_model_record_1_r" USING btree (
  "update_time" "pg_catalog"."timestamp_ops" ASC NULLS LAST
);

CREATE TRIGGER "tr_tbi_l6_mt_predict_model_record_1_r_beforeupdate" BEFORE UPDATE ON "public"."tbi_l6_mt_predict_model_record_1_r"
FOR EACH ROW
EXECUTE PROCEDURE "public"."fn_change_update_time"();
"""

"""
CREATE TABLE "public"."tbi_l6_mt_predict_model_record_sma_r" (
  "id" uuid NOT NULL,
  "merchant_code" varchar(20) COLLATE "pg_catalog"."default" NOT NULL DEFAULT ''::character varying,
  "mid" uuid NOT NULL,
  "pay_time" timestamp(6) NOT NULL,
  "predict_type" int4,
  "predict_time" timestamp(6),
  "predict_dt" numeric(20,6),
  "p_accuracy" numeric(20,6),
  "real_time" timestamp(6),
  "real_dt" numeric(20,6),
  "r_err" numeric(20,6),
  "d_err" int4,
  "err_rate1" numeric(20,6),
  "err_rate2" numeric(20,6),
  "model_para" text COLLATE "pg_catalog"."default",
  "model_choice" text COLLATE "pg_catalog"."default",
  "obligate" text COLLATE "pg_catalog"."default",
  "create_time" timestamp(6) NOT NULL DEFAULT clock_timestamp(),
  "update_time" timestamp(6) NOT NULL DEFAULT clock_timestamp(),
  "status_time" timestamp(6) NOT NULL DEFAULT clock_timestamp(),
  "status" int2 NOT NULL DEFAULT 0,
  CONSTRAINT "pk_tbi_l6_mt_predict_model_record_sma_r" PRIMARY KEY ("id")
)
;

ALTER TABLE "public"."tbi_l6_mt_predict_model_record_sma_r" 
  OWNER TO "postgres";

CREATE INDEX "ix_tbi_l6_mt_predict_model_record_sma_r_mid" ON "public"."tbi_l6_mt_predict_model_record_sma_r" USING btree (
  "mid" "pg_catalog"."uuid_ops" ASC NULLS LAST
);

CREATE INDEX "ix_tbi_l6_mt_predict_model_record_sma_r_mid_pay_time" ON "public"."tbi_l6_mt_predict_model_record_sma_r" USING btree (
  "mid" "pg_catalog"."uuid_ops" ASC NULLS LAST,
  "pay_time" "pg_catalog"."timestamp_ops" ASC NULLS LAST
);

CREATE INDEX "ix_tbi_l6_mt_predict_model_record_sma_r_pay_time" ON "public"."tbi_l6_mt_predict_model_record_sma_r" USING btree (
  "pay_time" "pg_catalog"."timestamp_ops" ASC NULLS LAST
);

CREATE INDEX "ix_tbi_l6_mt_predict_model_record_sma_r_update_time" ON "public"."tbi_l6_mt_predict_model_record_sma_r" USING btree (
  "update_time" "pg_catalog"."timestamp_ops" ASC NULLS LAST
);

CREATE TRIGGER "tr_tbi_l6_mt_predict_model_record_sma_r_beforeupdate" BEFORE UPDATE ON "public"."tbi_l6_mt_predict_model_record_sma_r"
FOR EACH ROW
EXECUTE PROCEDURE "public"."fn_change_update_time"();
"""

"""
CREATE TABLE "public"."tbi_l6_mt_predict_record_r" (
  "id" uuid NOT NULL,
  "merchant_code" varchar(20) COLLATE "pg_catalog"."default" NOT NULL DEFAULT ''::character varying,
  "mid" uuid NOT NULL,
  "pay_time" timestamp(6) NOT NULL,
  "predict_type" int4,
  "predict_time" timestamp(6),
  "predict_dt" numeric(20,6),
  "p_accuracy" numeric(20,6),
  "real_time" timestamp(6),
  "real_dt" numeric(20,6),
  "r_err" numeric(20,6),
  "d_err" int4,
  "err_rate1" numeric(20,6),
  "err_rate2" numeric(20,6),
  "model_para" text COLLATE "pg_catalog"."default",
  "model_choice" text COLLATE "pg_catalog"."default",
  "obligate" text COLLATE "pg_catalog"."default",
  "create_time" timestamp(6) NOT NULL DEFAULT clock_timestamp(),
  "update_time" timestamp(6) NOT NULL DEFAULT clock_timestamp(),
  "status_time" timestamp(6) NOT NULL DEFAULT clock_timestamp(),
  "status" int2 NOT NULL DEFAULT 0,
  CONSTRAINT "pk_tbi_l6_mt_predict_record_r" PRIMARY KEY ("id")
)
;

ALTER TABLE "public"."tbi_l6_mt_predict_record_r" 
  OWNER TO "postgres";

CREATE INDEX "ix_tbi_l6_mt_predict_record_r_mid" ON "public"."tbi_l6_mt_predict_record_r" USING btree (
  "mid" "pg_catalog"."uuid_ops" ASC NULLS LAST
);

CREATE INDEX "ix_tbi_l6_mt_predict_record_r_mid_pay_time" ON "public"."tbi_l6_mt_predict_record_r" USING btree (
  "mid" "pg_catalog"."uuid_ops" ASC NULLS LAST,
  "pay_time" "pg_catalog"."timestamp_ops" ASC NULLS LAST
);

CREATE INDEX "ix_tbi_l6_mt_predict_record_r_pay_time" ON "public"."tbi_l6_mt_predict_record_r" USING btree (
  "pay_time" "pg_catalog"."timestamp_ops" ASC NULLS LAST
);

CREATE INDEX "ix_tbi_l6_mt_predict_record_r_update_time" ON "public"."tbi_l6_mt_predict_record_r" USING btree (
  "update_time" "pg_catalog"."timestamp_ops" ASC NULLS LAST
);

CREATE TRIGGER "tr_tbi_l6_mt_predict_record_r_beforeupdate" BEFORE UPDATE ON "public"."tbi_l6_mt_predict_record_r"
FOR EACH ROW
EXECUTE PROCEDURE "public"."fn_change_update_time"();
"""

"""
CREATE TABLE "public"."tbi_l7_m_deed_profile1_r" (
  "id" uuid NOT NULL,
  "merchant_code" varchar(20) COLLATE "pg_catalog"."default" NOT NULL DEFAULT ''::character varying,
  "mid" uuid NOT NULL,
  "re_pay_count" int4 NOT NULL DEFAULT 0,
  "order_count" int4 NOT NULL DEFAULT 0,
  "origin_amount_sum" int8 NOT NULL DEFAULT 0,
  "pay_amount_sum" int8 NOT NULL DEFAULT 0,
  "origin_amount_std0" numeric(20,4),
  "pay_amount_std0" numeric(20,4),
  "fuel_origin_amount" int8 NOT NULL DEFAULT 0,
  "fuel_pay_amount" int8 NOT NULL DEFAULT 0,
  "fuel_count" int4 NOT NULL DEFAULT 0,
  "fuel_quantity" int8 NOT NULL DEFAULT 0,
  "fuel_quantity_std0" numeric(20,4),
  "nonoil_origin_amount" int8 NOT NULL DEFAULT 0,
  "nonoil_pay_amount" int8 NOT NULL DEFAULT 0,
  "nonoil_count" int4 NOT NULL DEFAULT 0,
  "nonoil_quantity" int8 NOT NULL DEFAULT 0,
  "coupon_count" int8 NOT NULL DEFAULT 0,
  "coupon_amount" int8 NOT NULL DEFAULT 0,
  "pay_toc_msa" numeric(20,4),
  "weekday_msa" numeric(20,4),
  "monthday_msa" numeric(20,4),
  "diff_time_avg" numeric(20,4),
  "diff_time_msa" numeric(20,4),
  "diff_time_std0" numeric(20,4),
  "diff_time_std0_fixed" numeric(20,4),
  "diff_time_max" numeric(20,4),
  "diff_time_max_fixed" numeric(20,4),
  "obligate" text COLLATE "pg_catalog"."default",
  "create_time" timestamp(6) NOT NULL DEFAULT clock_timestamp(),
  "update_time" timestamp(6) NOT NULL DEFAULT '1990-01-01 00:00:00'::timestamp without time zone,
  "status_time" timestamp(6) NOT NULL DEFAULT clock_timestamp(),
  "status" int2 NOT NULL DEFAULT 0,
  CONSTRAINT "pk_tbi_l7_m_deed_profile1_r" PRIMARY KEY ("id")
)
;

ALTER TABLE "public"."tbi_l7_m_deed_profile1_r" 
  OWNER TO "postgres";

CREATE INDEX "ix_tbi_l7_m_deed_profile1_r_mid" ON "public"."tbi_l7_m_deed_profile1_r" USING btree (
  "mid" "pg_catalog"."uuid_ops" ASC NULLS LAST
);

CREATE TRIGGER "tr_tbi_l7_m_deed_profile1_r_beforeupdate" BEFORE UPDATE ON "public"."tbi_l7_m_deed_profile1_r"
FOR EACH ROW
EXECUTE PROCEDURE "public"."fn_change_update_time"();
"""


"""
CREATE TABLE "public"."tbi_l7_m_predict_r" (
  "id" uuid NOT NULL,
  "merchant_code" varchar(20) COLLATE "pg_catalog"."default" NOT NULL DEFAULT ''::character varying,
  "mid" uuid NOT NULL,
  "pay_time" timestamp(6) NOT NULL,
  "predict_type" int4,
  "predict_time" timestamp(6),
  "predict_dt" numeric(20,6),
  "p_accuracy" numeric(20,6),
  "model_para" text COLLATE "pg_catalog"."default",
  "model_choice" text COLLATE "pg_catalog"."default",
  "obligate" text COLLATE "pg_catalog"."default",
  "create_time" timestamp(6) NOT NULL DEFAULT clock_timestamp(),
  "update_time" timestamp(6) NOT NULL DEFAULT clock_timestamp(),
  "status_time" timestamp(6) NOT NULL DEFAULT clock_timestamp(),
  "status" int2 NOT NULL DEFAULT 0,
  CONSTRAINT "pk_tbi_l7_m_predict_r" PRIMARY KEY ("id")
)
;

ALTER TABLE "public"."tbi_l7_m_predict_r" 
  OWNER TO "postgres";

CREATE INDEX "ix_tbi_l7_m_predict_r_mid" ON "public"."tbi_l7_m_predict_r" USING btree (
  "mid" "pg_catalog"."uuid_ops" ASC NULLS LAST
);

CREATE INDEX "ix_tbi_l7_m_predict_r_pay_time" ON "public"."tbi_l7_m_predict_r" USING btree (
  "pay_time" "pg_catalog"."timestamp_ops" ASC NULLS LAST
);

CREATE INDEX "ix_tbi_l7_m_predict_r_update_time" ON "public"."tbi_l7_m_predict_r" USING btree (
  "update_time" "pg_catalog"."timestamp_ops" ASC NULLS LAST
);

CREATE TRIGGER "tr_tbi_l7_m_predict_r_beforeupdate" BEFORE UPDATE ON "public"."tbi_l7_m_predict_r"
FOR EACH ROW
EXECUTE PROCEDURE "public"."fn_change_update_time"();
"""


"""CREATE TABLE "public"."tbi_l3_smp_consumption_xd" (
  "id" uuid NOT NULL,
  "merchant_code" varchar(20) COLLATE "pg_catalog"."default" NOT NULL DEFAULT ''::character varying,
  "station_code" varchar(20) COLLATE "pg_catalog"."default" NOT NULL DEFAULT ''::character varying,
  "mid" uuid NOT NULL,
  "member_level_id" uuid NOT NULL,
  "member_level_name" varchar(100) COLLATE "pg_catalog"."default" NOT NULL DEFAULT ''::character varying,
  "plate_no" varchar(20) COLLATE "pg_catalog"."default" NOT NULL DEFAULT ''::character varying,
  "period" varchar(20) COLLATE "pg_catalog"."default" NOT NULL DEFAULT ''::character varying,
  "trans_date" date NOT NULL,
  "first_time" timestamp(6) NOT NULL,
  "last_time" timestamp(6) NOT NULL,
  "trans_count" int4 NOT NULL DEFAULT 0,
  "detail_count" int4 NOT NULL DEFAULT 0,
  "original_amount" int8 NOT NULL DEFAULT 0,
  "net_amount" int8 NOT NULL DEFAULT 0,
  "discount_amount" int8 NOT NULL DEFAULT 0,
  "discount_count" int8 NOT NULL DEFAULT 0,
  "quantity" int8 NOT NULL DEFAULT 0,
  "quantity_min" int8 NOT NULL DEFAULT 0,
  "quantity_max" int8 NOT NULL DEFAULT 0,
  "price_sum" int8 NOT NULL DEFAULT 0,
  "create_time" timestamp(6) NOT NULL DEFAULT clock_timestamp(),
  "update_time" timestamp(6) NOT NULL DEFAULT clock_timestamp(),
  "status_time" timestamp(6) NOT NULL DEFAULT clock_timestamp(),
  "status" int2 NOT NULL DEFAULT 0,
  CONSTRAINT "pk_tbi_l3_smp_consumption_xd" PRIMARY KEY ("id")
)
;

ALTER TABLE "public"."tbi_l3_smp_consumption_xd" 
  OWNER TO "postgres";

CREATE INDEX "ix_tbi_l3_smp_consumption_xd_mid" ON "public"."tbi_l3_smp_consumption_xd" USING btree (
  "mid" "pg_catalog"."uuid_ops" ASC NULLS LAST
);

CREATE INDEX "ix_tbi_l3_smp_consumption_xd_plate_no" ON "public"."tbi_l3_smp_consumption_xd" USING btree (
  "plate_no" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

CREATE INDEX "ix_tbi_l3_smp_consumption_xd_station_code" ON "public"."tbi_l3_smp_consumption_xd" USING btree (
  "station_code" COLLATE "pg_catalog"."default" "pg_catalog"."text_ops" ASC NULLS LAST
);

CREATE INDEX "ix_tbi_l3_smp_consumption_xd_trans_date" ON "public"."tbi_l3_smp_consumption_xd" USING btree (
  "trans_date" "pg_catalog"."date_ops" ASC NULLS LAST
);

CREATE TRIGGER "tr_tbi_l3_smp_consumption_xd_beforeupdate" BEFORE UPDATE ON "public"."tbi_l3_smp_consumption_xd"
FOR EACH ROW
EXECUTE PROCEDURE "public"."fn_change_update_time"();
"""


"""CREATE OR REPLACE FUNCTION "public"."fn_change_update_time"()
  RETURNS "pg_catalog"."trigger" AS $BODY$
    begin
        new.Update_Time = clock_timestamp();
        return new;
    end;
    $BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100
  """
