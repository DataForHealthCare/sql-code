with encounter_services as (
    select
        enctrnum,
        svcnum,
        dxseqnbr1,
        dxseqnbr2,
        dxseqnbr3,
        dxseqnbr4,
        cpt4hcpcsgrpplaceofsvc
    from
        cleansed.adminsystemsclaims.enctrsvcs
),
encounter_diagnoses as (
    select
        enctrnum,
        diagnum,
        dxcode,
        prmryorsecondary
    from
        cleansed.adminsystemsclaims.enctrdiagnoses
)
select
    cl.adminfeeamt admin_fee_amount,
    cl.clmnum as claim_no,
    cl.enctrnum as encounter_no,
    cl.frpnum as frp_no,
    cl.postingdate as posting_dt,
    cl.voidstatus as void_status_cd,
    adjs.adjudicatedsvcnum as claim_line_no,
    adjs.cgbeforeadjamtprepaid as pre_paid_amt,
    adjs.cgbeforeadjcoinsuranceamt as coinsurance_amount,
    adjs.cgbeforeadjcopayamt as copay_amount,
    adjs.cgbeforeadjdeductibleamt as deductible_amount,
    adjs.cgbeforeadjmncaretaxpmtinc as mncare_tax_amount,
    adjs.cgbeforeadjnetamtappr as net_approved_amount,
    adjs.cgbeforeadjnetmbrliab as net_member_liability_amount,
    adjs.cgbeforeadjplanliab as plan_liability_amount,
    adjs.cgbeforeadjtcocsharedsvngs as shared_savings_amount,
    adjs.cgbeforeadjtotalmbrliab as gross_member_liability_amount,
    adjs.charges as billed_amount,
    adjs.codingsystem as coding_system_cd,
    adjs.cvrggrpamtexcludedduetoucr as usual_customary_exclusion,
    adjs.cvrggrpauthstatus as authorization_status_id,
    adjs.cvrggrpnhnauthnum as authorization_no,
    adjs.cvrggrpoutlieramt as outlier_amount,
    adjs.cvrggrpplaceofsvc as benefit_place_of_service_cd,
    adjs.cvrggrpprovdisallowed as provider_disallow_amount,
    adjs.cvrggrpprovdisallowedcode as provider_disallow_reason,
    adjs.cvrggrpsvccomponent as service_component_no,
    adjs.cvrggrptier as benefit_tier_cd,
    adjs.enctrsvcsvcnum as encounter_service_no,
    adjs.facnetwk as facility_network_id,
    adjs.firstsvcdate as first_service_dt,
    adjs.cvrggrpgrossamtappr as gross_approved_amount,
    adjs.lastsvcdate as last_service_dt,
    adjs.modifier as procedure_modifier1,
    adjs.modifier2 as procedure_modifier2,
    adjs.modifier3 as procedure_modifier3,
    adjs.modifier4 as procedure_modifier4,
    adjs.reasondenied as denied_reason_cd,
    adjs.ubcpt4 as revenue_cpt4_2_cd,
    adjs.units as unit_qty,
    (
        adjs.cgbeforeadjamtadvanced + adjs.cgbeforeadjamttobepaid
    ) as paid_amount,
    (
        adjs.cgbeforeadjmbrliabpdbynofltins + adjs.cgbeforeadjmbrliabpdbyothrins
    ) as reduced_by_other_insurance_amount_member,
    (
        adjs.cvrggrpprovliabredbynofaultins + adjs.cvrggrpprovliabredbyothins
    ) as reduced_by_other_insurance_amount_payor,
    case
        when adjs.codingsystem = 'CPT4' then adjs.procedurecode
        else '-1'
    end as cpt_cd,
    case
        when adjs.codingsystem = 'NDC' then adjs.procedurecode
        else '-1'
    end as ndc_cd,
    case
        when adjs.codingsystem = 'REV' then adjs.procedurecode
        else '-1'
    end as revenue_cd,
    clnet.netwkaccessfeeamt as network_access_fee_amt,
    case
        when clnet.accessfeeidentifier = '1' then clnet.netwkaccessfeeamt
        else '-1'
    end as phcs_admin_fee_amount,
    encs.personnum as member_id,
    encs.fac as facility_id,
    encs.cvub82grpinpat as inpatient_yn,
    encs.clmvarub82factype as bill_tp_cd,
    encs.cvub82grpadmissiondate as admit_dt,
    encs.clmvardatereceived as recv_dt,
    encs.cvub82grpsourceofadmission as adm_source_cd,
    encs.cvub82grptypeofadmission as adm_tp_cd,
    encs.cvub82grppatstatus as pat_stat_cd,
    epkgs.clinadmgrpnum as admin_grp_id,
    epkgs.pkgnum as ben_pkg_id,
    epkgs.deliverynetwk as delivery_network_id,
    epkgs.finclarrgnum as financial_arrangement_id,
    epkgs.corp as license_corp_id,
    epkgs.prmryclin as primary_care_site_id,
    epkgs.product as product_id,
    case
        when erub.principalorothr = 'PRINCIPAL' then erub.pxcode
        else '-1'
    end as pri_icd_proc_cd,
    case
        when edia.prmryorsecondary = 'P' then edia.dxcode
        else '-1'
    end as icd_dx_cd_primary,
    esvcs.cpt4hcpcsgrpplaceofsvc as claim_place_of_service_cd,
    lcd1.diagnum as line_dx1_cd,
    lcd2.diagnum as line_dx2_cd,
    lcd3.diagnum as line_dx3_cd,
    lcd4.diagnum as line_dx4_cd
from
    cleansed.adminsystemsclaims.claims cl
    left outer join cleansed.adminsystemsclaims.adjudicatedsvcs adjs on cl.clmnum = adjs.clmnum
    left outer join cleansed.adminsystemsclaims.clmnetwkaccessfeedetail clnet on cl.clmnum = clnet.clmnum
    left outer join cleansed.adminsystemsclaims.encounters encs on cl.enctrnum = encs.enctrnum
    left outer join cleansed.adminsystemsclaims.enctrfrppackages epkgs on cl.enctrnum = epkgs.enctrnum
    and cl.frpnum = epkgs.frpnum
    left outer join cleansed.adminsystemsclaims.enctrub82orprocedures erub on cl.enctrnum = erub.enctrnum
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