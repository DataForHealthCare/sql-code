INSERT INTO standardized_dev.adwhicore.base_claims(
    adm_source_cd,
    adm_tp_cd,
    admin_fee_amt,
    admin_grp_id,
    admit_dt,
    authorization_no,
    authorization_status_id,
    ben_pkg_id,
    benefit_place_of_service_cd,
    benefit_tier_cd,
    bill_tp_cd,
    billed_amt,
    claim_line_no,
    claim_no,
    claim_place_of_service_cd,
    coding_system_cd,
    coinsurance_amt,
    copay_amt,
    cpt_cd,
    deductible_amt,
    delivery_network_id,
    denied_reason_cd,
    encounter_no,
    encounter_service_no,
    facility_id,
    facility_network_id,
    financial_arrangement_id,
    first_service_dt,
    frp_no,
    gross_approved_amt,
    gross_member_liability_amt,
    icd_dx_cd_primary_cd,
    inpatient_yn_txt,
    last_service_dt,
    license_corp_id,
    line_dx1_cd,
    line_dx2_cd,
    line_dx3_cd,
    line_dx4_cd,
    member_id,
    mncare_tax_amt,
    ndc_cd,
    net_approved_amt,
    net_member_liability_amt,
    network_access_fee_amt,
    outlier_amt,
    paid_amt,
    pat_stat_cd,
    phcs_admin_fee_amt,
    plan_liability_amt,
    posting_dt,
    pre_paid_amt,
    pri_icd_proc_cd,
    primary_care_site_id,
    procedure_modifier1_txt,
    procedure_modifier2_txt,
    procedure_modifier3_txt,
    procedure_modifier4_txt,
    product_id,
    provider_disallow_amt,
    provider_disallow_reason_cd,
    recv_dt,
    reduced_by_other_insurance_amount_member_amt,
    reduced_by_other_insurance_amount_payor_amt,
    remittance_advice_transaction_no,
    revenue_cd,
    revenue_cpt4_2_cd,
    service_component_no,
    shared_savings_amt,
    unit_qty_txt,
    usual_customary_exclusion_amt,
    void_status_cd
)
WITH encounter_diagnoses AS (
    SELECT
        diagnum,
        dxcode,
        enctrnum,
        prmryorsecondary
     FROM
        global_temp.enctrdiagnoses_view ed
),
encounter_services AS (
    SELECT
        cpt4hcpcsgrpplaceofsvc,
        dxseqnbr1,
        dxseqnbr2,
        dxseqnbr3,
        dxseqnbr4,
        enctrnum,
        svcnum
    FROM
        global_temp.enctrsvcs_view es
    WHERE
        es.del_dt is null
)
SELECT
    cast(coalesce(encs.cvub82grpsourceofadmission, '~') as STRING) as adm_source_cd,
    cast(coalesce(encs.cvub82grptypeofadmission, '~') as STRING) as adm_tp_cd,
    cast(coalesce(cl.adminfeeamt, 0) as DECIMAL(15, 4)) as admin_fee_amt,
    cast(coalesce(epkgs.clinadmgrpnum, -1) as INT) admin_grp_id,
    cast(coalesce(encs.cvub82grpadmissiondate, '01-JAN-0001') as DATE) as admit_dt,
    cast(coalesce(adjs.cvrggrpnhnauthnum, '~') as STRING) as authorization_no,
    cast(coalesce(adjs.cvrggrpauthstatus, -1) as INT) as authorization_status_id,
    cast(coalesce(epkgs.pkgnum, -1) as INT) ben_pkg_id,
    cast(coalesce(adjs.cvrggrpplaceofsvc, '~') as STRING) as benefit_place_of_service_cd,
    cast(coalesce(adjs.cvrggrptier, -1) as INT) as benefit_tier_cd,
    cast(coalesce(encs.clmvarub82factype, '~') as STRING) as bill_tp_cd,
    cast(coalesce(adjs.charges, 0) as DECIMAL(15, 4)) as billed_amt,
    cast(coalesce(adjs.adjudicatedsvcnum, -1) as INT) as claim_line_no,
    cast(coalesce(cl.clmnum, -1) as INT) as claim_no,
    cast(coalesce(esvcs.cpt4hcpcsgrpplaceofsvc, '~') as STRING) as claim_place_of_service_cd,
    cast(coalesce(adjs.codingsystem, '~') as STRING) as coding_system_cd,
    cast(coalesce(adjs.cgbeforeadjcoinsuranceamt, 0) as DECIMAL(15, 4)) as coinsurance_amt,
    cast(coalesce(adjs.cgbeforeadjcopayamt, 0) as DECIMAL(15, 4)) as copay_amt,
    case 
        when adjs.codingsystem = 'CPT4' then cast(coalesce(adjs.procedurecode, '~') as STRING)
        else cast('-1' as STRING)
    end as cpt_cd,
    cast(coalesce(adjs.cgbeforeadjdeductibleamt, 0) as DECIMAL(15, 4)) as deductible_amt,
    cast(coalesce(epkgs.deliverynetwk, -1) as INT) delivery_network_id,
    cast(coalesce(adjs.reasondenied, -1) as INT) as denied_reason_cd,
    cast(coalesce(cl.enctrnum, -1) as INT) as encounter_no,
    cast(coalesce(adjs.enctrsvcsvcnum, -1) as INT) as encounter_service_no,
    cast(coalesce(encs.fac, -1) as INT) as facility_id,
    cast(coalesce(adjs.facnetwk, -1) as INT) as facility_network_id,
    cast(coalesce(epkgs.finclarrgnum, -1) as INT) financial_arrangement_id,
    cast(coalesce(adjs.firstsvcdate, '01-JAN-001') as DATE) as first_service_dt,
    cast(coalesce(cl.frpnum, -1) as INT) as frp_no,
    cast(coalesce(adjs.cvrggrpgrossamtappr, 0) as DECIMAL(15, 4)) as gross_approved_amt,
    cast(coalesce(adjs.cgbeforeadjtotalmbrliab, 0) as DECIMAL(15, 4)) as gross_member_liability_amt,
    case
        when edia.prmryorsecondary = 'P' then cast(coalesce(edia.dxcode, '~') as STRING)
        else cast('-1' as STRING)
    end as icd_dx_cd_primary_cd,
    cast(coalesce(encs.cvub82grpinpat, 'MISSING') as STRING) as inpatient_yn_txt,
    cast(coalesce(adjs.lastsvcdate, '01-JAN-001') as DATE) as last_service_dt,
    cast(coalesce(epkgs.corp, -1) as INT) license_corp_id,
    cast(coalesce(lcd1.diagnum, -1) as INT) as line_dx1_cd,
    cast(coalesce(lcd2.diagnum, -1) as INT) as line_dx2_cd,
    cast(coalesce(lcd3.diagnum, -1) as INT) as line_dx3_cd,
    cast(coalesce(lcd4.diagnum, -1) as INT) as line_dx4_cd,
    cast(coalesce(encs.personnum, -1) as INT) as member_id,
    cast(coalesce(adjs.cgbeforeadjmncaretaxpmtinc, 0) as DECIMAL(15, 4)) as mncare_tax_amt,
    case
        when adjs.codingsystem = 'NDC' then cast(coalesce(adjs.procedurecode, '~') as STRING)
        else cast('-1' as STRING)
    end as ndc_cd,
    cast(coalesce(adjs.cgbeforeadjnetamtappr, 0) as DECIMAL(15, 4)) as net_approved_amt,
    cast(coalesce(adjs.cgbeforeadjnetmbrliab, 0) as DECIMAL(15, 4)) as net_member_liability_amt,
    cast(coalesce(clnet.netwkaccessfeeamt, '~') as STRING) as network_access_fee_amt,
    cast(coalesce(adjs.cvrggrpoutlieramt , '~') as STRING) as outlier_amt,
    (
        cast(coalesce(adjs.cgbeforeadjamtadvanced + adjs.cgbeforeadjamttobepaid, 0) as DECIMAL(15, 4))
    ) as paid_amt,
    cast(encs.cvub82grppatstatus as STRING) as pat_stat_cd,
    case
        when clnet.accessfeeidentifier = '1' then cast(clnet.netwkaccessfeeamt as STRING)
        else cast('~' as STRING)
    end as phcs_admin_fee_amt,
    cast(coalesce(adjs.cgbeforeadjplanliab, 0) as DECIMAL(15, 4)) as plan_liability_amt,
    cast(coalesce(cl.postingdate, '01-JAN-0001') as DATE) as posting_dt,
    cast(coalesce(adjs.cgbeforeadjamtprepaid, 0) as DECIMAL(15, 4)) as pre_paid_amt,
    case
        when erub.principalorothr = 'PRINCIPAL' then cast(coalesce(erub.pxcode, '~') as STRING)
        else cast('-1' as STRING)
    end as pri_icd_proc_cd,
    cast(coalesce(epkgs.prmryclin, -1) as INT) primary_care_site_id,
    cast(coalesce(adjs.modifier, 'MISSING') as STRING) as procedure_modifier1_txt,
    cast(coalesce(adjs.modifier2, 'MISSING') as STRING) as procedure_modifier2_txt,
    cast(coalesce(adjs.modifier3, 'MISSING') as STRING) as procedure_modifier3_txt,
    cast(coalesce(adjs.modifier4, 'MISSING') as STRING) as procedure_modifier4_txt,
    cast(coalesce(epkgs.product, -1) as INT) product_id,
    cast(coalesce(adjs.cvrggrpprovdisallowed, 0) as DECIMAL(15, 4)) as provider_disallow_amt,
    cast(coalesce(adjs.cvrggrpprovdisallowedcode, -1) as INT) as provider_disallow_reason_cd,
    cast(coalesce(encs.clmvardatereceived, '01-JAN-0001') as DATE) as recv_dt,
    (
        cast(coalesce(adjs.cgbeforeadjmbrliabpdbynofltins + adjs.cgbeforeadjmbrliabpdbyothrins, 0) as DECIMAL(15, 4))
    ) as reduced_by_other_insurance_amount_member_amt,
    (
        cast(coalesce(adjs.cvrggrpprovliabredbynofaultins + adjs.cvrggrpprovliabredbyothins, 0) as DECIMAL(15, 4))
    ) as reduced_by_other_insurance_amount_payor_amt,
    cast(coalesce(rmt.rmtnctransactionnum, -1) as INT) as remittance_advice_transaction_no,
    case
        when adjs.codingsystem = 'REV' then cast(coalesce(adjs.procedurecode, '~') as STRING)
        else cast('-1' as STRING)
    end as revenue_cd,
    cast(coalesce(adjs.ubcpt4, '~') as STRING) as revenue_cpt4_2_cd,
    cast(coalesce(adjs.cvrggrpsvccomponent, -1) as INT) as service_component_no,
    cast(coalesce(adjs.cgbeforeadjtcocsharedsvngs, 0) as DECIMAL(15, 4)) as shared_savings_amt,
    cast(coalesce(adjs.units, 'MISSING') as STRING) as unit_qty_txt,
    cast(coalesce(adjs.cvrggrpamtexcludedduetoucr, 0) as DECIMAL(15, 4)) as usual_customary_exclusion_amt,
    cast(coalesce(cl.voidstatus, '~') as STRING) as void_status_cd
FROM
    global_temp.claims_view cl
    left outer join global_temp.adjudicatedsvcs_view adjs on cl.clmnum = adjs.clmnum
            and adjs.del_dt is null
    left outer join global_temp.clmnetwkaccessfeedetail_view clnet on cl.clmnum = clnet.clmnum
    left outer join encounter_services esvcs on cl.enctrnum = esvcs.enctrnum
            and esvcs.svcnum = adjs.enctrsvcsvcnum
    left outer join encounter_diagnoses lcd1 on cl.enctrnum = lcd1.enctrnum
            and esvcs.svcnum = adjs.enctrsvcsvcnum
            and lcd1.diagnum = esvcs.dxseqnbr1
    left outer join encounter_diagnoses lcd2 on cl.enctrnum = lcd2.enctrnum
            and esvcs.svcnum = adjs.enctrsvcsvcnum
            and lcd2.diagnum = esvcs.dxseqnbr2
    left outer join encounter_diagnoses lcd3 on cl.enctrnum = lcd3.enctrnum
            and esvcs.svcnum = adjs.enctrsvcsvcnum
            and lcd3.diagnum = esvcs.dxseqnbr3
    left outer join encounter_diagnoses lcd4 on cl.enctrnum = lcd4.enctrnum
            and esvcs.svcnum = adjs.enctrsvcsvcnum
            and lcd4.diagnum = esvcs.dxseqnbr4
    left outer join encounter_diagnoses edia on cl.enctrnum = edia.enctrnum
            and esvcs.svcnum = adjs.enctrsvcsvcnum
    left outer join global_temp.encounters_view encs on cl.enctrnum = encs.enctrnum
            and encs.del_dt is null
    left outer join global_temp.enctrfrppackages_view epkgs on cl.enctrnum = epkgs.enctrnum
            and cl.frpnum = epkgs.frpnum
    left outer join global_temp.enctrub82orprocedures_view erub on cl.enctrnum = erub.enctrnum
    left outer join global_temp.rmtnctransactions_view rmt on cl.clmnum = rmt.clmnum
