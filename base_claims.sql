INSERT INTO
        standardized_dev.health_plan.base_claims(
            acct_yrmo_txt,
            benefit_place_of_service_cd,
            benefit_tier_cd,
            bill_tp_cd,
            claim_form_type_cd,
            claim_line_no,
            claim_no,
            claim_place_of_service_cd,
            encounter_no,
            encounter_service_no,
            facility_id,
            first_service_dt,
            frp_no,
            inpatient_yn_txt,
            last_service_dt,
            paid_date_dt,
            posting_dt,
            record_type_cd,
            remittance_advice_transaction_no,
            service_component_no
            ) 
        WITH adjudicated_services AS (
        SELECT
            adjudicatedsvcnum,
            clmnum,
            cvrggrpplaceofsvc,
            cvrggrptier,
            cvrggrpsvccomponent,
            enctrsvcsvcnum,
            firstsvcdate,
            lastsvcdate
        FROM
            cleansed.adminsystemsclaims.adjudicatedsvcs
    ),
    claims AS (
        SELECT
            clmnum,
            enctrnum,
            frpnum,
            postingdate
        FROM
            cleansed.adminsystemsclaims.claims
    ),
    encounters AS (
        SELECT
            clmvarub82factype,
            cpt4hcpcsgrpplaceofsvc,
            cvub82grpinpat,
            es.enctrnum,
            fac,
            svcnum
        FROM
            cleansed.adminsystemsclaims.enctrsvcs es left outer join
            cleansed.adminsystemsclaims.encounters en on es.enctrnum = en.enctrnum
    ),
    financial_transactions AS (
        SELECT 
            rmt.clmnum,
            rmt.rmtnctransactionnum,
            lr.paiddate,
            lr.rmtnctransactionsnum
        FROM
            cleansed.adminsystemsodsclaims.lrodsdailyfeedclaims lr left outer join
            cleansed.adminsystemsclaims.rmtnctransactions rmt on lr.rmtnctransactionsnum = rmt.rmtnctransactionnum
    )
    SELECT DISTINCT
        coalesce(date_format(ft.paiddate, 'yMM'), 'MISSING') as acct_yrmo_txt,
        cast(coalesce(adjs.cvrggrpplaceofsvc, '~') as STRING) as benefit_place_of_service_cd,
        cast(coalesce(adjs.cvrggrptier, -1) as INT) as benefit_tier_cd,
        cast(coalesce(encs.clmvarub82factype, '~') as STRING) as bill_tp_cd,
        NULL as claim_form_type_cd,
        cast(coalesce(adjs.adjudicatedsvcnum, -1) as INT) as claim_line_no,
        cast(coalesce(cl.clmnum, -1) as INT) as claim_no,
        cast(
                coalesce(encs.cpt4hcpcsgrpplaceofsvc, '~') as STRING
            ) as claim_place_of_service_cd,
        cast(coalesce(cl.enctrnum, -1) as BIGINT) as encounter_no,
        cast(coalesce(adjs.enctrsvcsvcnum, -1) as INT) as encounter_service_no,
        cast(coalesce(encs.fac, -1) as INT) as facility_id,
        coalesce(
            cast(adjs.firstsvcdate as DATE),
            to_date('01-01-0001', 'dd-MM-yyyy')
        ) as first_service_dt,
        cast(coalesce(cl.frpnum, -1) as INT) as frp_no,
        cast(
                coalesce(encs.cvub82grpinpat, 'MISSING') as STRING
            ) as inpatient_yn_txt,
        coalesce(
            cast(adjs.lastsvcdate as DATE),
            to_date('01-01-0001', 'dd-MM-yyyy')
        ) as last_service_dt,
        coalesce(
                cast(ft.paiddate as DATE),
                to_date('01-01-0001', 'dd-MM-yyyy')
            ) as paiddate_dt,
        coalesce(
            cast(cl.postingdate as DATE),
            to_date('01-01-0001', 'dd-MM-yyyy')
        ) as posting_dt,
        NULL as record_type_cd,
        cast(coalesce(ft.rmtnctransactionnum, -1) as INT) as remittance_advice_transaction_no,
        cast(coalesce(adjs.cvrggrpsvccomponent, -1) as INT) as service_component_no
    FROM
        claims cl
        left outer join adjudicated_services adjs on cl.clmnum = adjs.clmnum
        left outer join financial_transactions ft on cl.clmnum = ft.clmnum
        left outer join encounters encs on cl.enctrnum = encs.enctrnum