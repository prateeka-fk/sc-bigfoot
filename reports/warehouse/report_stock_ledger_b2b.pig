USE grn_join_ii_pd = grn_join_ii_pd_b2b;
USE ia_sl_join_ii_pd = ia_sl_join_ii_pd_b2b;
USE iiv_join_ivr_ii_pd  = iiv_join_ivr_ii_pd_b2b ;
USE s_join_ss_si_pd = s_join_ss_si_pd_pkey_b2b ;
USE iwt_join_pd_sl_b2b = iwt_join_pd_sl_b2b ;
USE ot_inventory_migration_b2b = YetiMerge_ot_inventory_migration_b2b ;

DEFINE UnixToISO org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO();

filtered_iwt_join_pd_sl_b2b = filter iwt_join_pd_sl_b2b by 
	(chararray)STRSPLIT(UnixToISO((long)iwt_join_pd_join_source__iwt_join_pd__wh_inventory_transfer_b2b__updated_at), 'T').$0 >= '[:START_DATE:]' 
    and (chararray)STRSPLIT(UnixToISO((long)iwt_join_pd_join_source__iwt_join_pd__wh_inventory_transfer_b2b__updated_at), 'T').$0 <= '[:END_DATE:]' 
    and ('[:PRODUCT_ID:]' == 'ALL' or '[:PRODUCT_ID:]' == iwt_join_pd_join_source__iwt_join_pd__product_detail_b2b_prod__fsn); 

filtered_ot = filter ot_inventory_migration_b2b by 
	(chararray)STRSPLIT(UnixToISO((long)updated_at), 'T').$0 >= '[:START_DATE:]' 
    and (chararray)STRSPLIT(UnixToISO((long)updated_at), 'T').$0 <= '[:END_DATE:]';      
    
ot_ia = join filtered_ot by inventory_item_id, ia_sl_join_ii_pd by ia_join_ii_pd__ii_join_pd__inventory_item_b2b_prod__id;        

group_ot_ia_by_ii = GROUP ot_ia by filtered_ot::inventory_item_id;      

group_ot_ia_by_ii1 = foreach group_ot_ia_by_ii{
	A = ORDER ot_ia by ia_join_ii_pd__inventory_audit_log_b2b_prod__id asc;
        B = LIMIT A 1;
        generate flatten(B.ia_sl_join_ii_pd::ia_join_ii_pd__ii_join_pd__inventory_item_b2b_prod__warehouse_id), 
		 flatten(B.filtered_ot::id),
		 flatten(B.ia_sl_join_ii_pd::ia_join_ii_pd__ii_join_pd__inventory_item_b2b_prod__fsn),
		 flatten(B.ia_sl_join_ii_pd::ia_join_ii_pd__ii_join_pd__inventory_item_b2b_prod__sku),
		 flatten(B.ia_sl_join_ii_pd::ia_join_ii_pd__inventory_audit_log_b2b_prod__quantity),
		 flatten(B.ia_sl_join_ii_pd::ia_join_ii_pd__ii_join_pd__product_detail_b2b_prod__cms_vertical),
		 flatten(B.filtered_ot::updated_at);
};                                                                                    

raw_filtered_grn_join_ii = filter grn_join_ii_pd by (chararray)STRSPLIT(UnixToISO((long)grn_b2b_prod__updated_at), 'T').$0 >= '[:START_DATE:]' and (chararray)STRSPLIT(UnixToISO((long)grn_b2b_prod__updated_at), 'T').$0 <= '[:END_DATE:]' and ('[:PRODUCT_ID:]' == 'ALL' or '[:PRODUCT_ID:]' == ii_join_pd__product_detail_b2b_prod__fsn);

report_rto_reship_received = filter s_join_ss_si_pd by ( shipment_status_b2b_prod__status == 'returned' or shipment_status_b2b_prod__status == 'reship_received' ) and shipment_b2b_prod__shipment_type == 'outgoing' and (chararray)STRSPLIT(UnixToISO((long)shipment_status_b2b_prod__updated_at), 'T').$0 >= '[:START_DATE:]' and (chararray)STRSPLIT(UnixToISO((long)shipment_status_b2b_prod__updated_at), 'T').$0 <= '[:END_DATE:]' and ('[:PRODUCT_ID:]' == 'ALL' or '[:PRODUCT_ID:]' == raw_si_pd__product_detail_b2b_prod__fsn);

report_return_received = filter s_join_ss_si_pd by shipment_status_b2b_prod__status == 'received' and shipment_b2b_prod__shipment_type == 'incoming' and (chararray)STRSPLIT(UnixToISO((long)shipment_status_b2b_prod__updated_at), 'T').$0 >= '[:START_DATE:]' and (chararray)STRSPLIT(UnixToISO((long)shipment_status_b2b_prod__updated_at), 'T').$0 <= '[:END_DATE:]' and ('[:PRODUCT_ID:]' == 'ALL' or '[:PRODUCT_ID:]' == raw_si_pd__product_detail_b2b_prod__fsn);

report_shipment_dispatched = filter s_join_ss_si_pd by shipment_status_b2b_prod__status == 'dispatched' and shipment_b2b_prod__shipment_type == 'outgoing' and (chararray)STRSPLIT(UnixToISO((long)shipment_status_b2b_prod__updated_at), 'T').$0 >= '[:START_DATE:]' and (chararray)STRSPLIT(UnixToISO((long)shipment_status_b2b_prod__updated_at), 'T').$0 <= '[:END_DATE:]' and ('[:PRODUCT_ID:]' == 'ALL' or '[:PRODUCT_ID:]' == raw_si_pd__product_detail_b2b_prod__fsn);

raw_report_variances = filter iiv_join_ivr_ii_pd by (chararray)STRSPLIT(UnixToISO((long)iiv_join_ivr__inventory_item_variance_b2b_prod__updated_at), 'T').$0 >= '[:START_DATE:]' and (chararray)STRSPLIT(UnixToISO((long)iiv_join_ivr__inventory_item_variance_b2b_prod__updated_at), 'T').$0 <= '[:END_DATE:]' and ('[:PRODUCT_ID:]' == 'ALL' or '[:PRODUCT_ID:]' == ivm_join_ii_pd__ii_join_pd__product_detail_b2b_prod__fsn);

raw_op_report_audits_1 = filter ia_sl_join_ii_pd by (chararray)STRSPLIT(UnixToISO((long)ia_join_ii_pd__inventory_audit_log_b2b_prod__updated_at), 'T').$0 < '[:START_DATE:]' 
						and ('[:PRODUCT_ID:]' == 'ALL' or '[:PRODUCT_ID:]' == ia_join_ii_pd__ii_join_pd__product_detail_b2b_prod__fsn);
                                            
raw_op_report_audits_2 = GROUP raw_op_report_audits_1 by ia_join_ii_pd__inventory_audit_log_b2b_prod__inventory_item_id;
                         
raw_op_report_audits_3 = foreach raw_op_report_audits_2 {
	A = ORDER raw_op_report_audits_1 by ia_join_ii_pd__inventory_audit_log_b2b_prod__id desc;
	B = LIMIT A 1;
	audit = filter B by ia_join_ii_pd__inventory_audit_log_b2b_prod__quantity > 0 and storage_location_b2b_prod__label != 'outbound_shipment_table';

	generate flatten(audit.ia_join_ii_pd__ii_join_pd__inventory_item_b2b_prod__warehouse_id), 
		 flatten(audit.ia_join_ii_pd__ii_join_pd__inventory_item_b2b_prod__fsn), 
		 flatten(audit.ia_join_ii_pd__ii_join_pd__inventory_item_b2b_prod__sku), 
		 flatten(audit.ia_join_ii_pd__ii_join_pd__product_detail_b2b_prod__cms_vertical),  
		 flatten(audit.ia_join_ii_pd__inventory_audit_log_b2b_prod__quantity);
       };

raw_op_group_fsn_audits = GROUP raw_op_report_audits_3 by (ia_join_ii_pd__ii_join_pd__inventory_item_b2b_prod__warehouse_id, ia_join_ii_pd__ii_join_pd__inventory_item_b2b_prod__fsn, ia_join_ii_pd__ii_join_pd__inventory_item_b2b_prod__sku, ia_join_ii_pd__ii_join_pd__product_detail_b2b_prod__cms_vertical);

raw_op_report_audits_start = foreach raw_op_group_fsn_audits generate group.$0 as warehouse_id, group.$1 as fsn, group.$2 as sku, group.$3 as cms_vertical, SUM(raw_op_report_audits_3.ia_join_ii_pd__inventory_audit_log_b2b_prod__quantity) as qty;


raw_op_report_audits_1 = filter ia_sl_join_ii_pd by (chararray)STRSPLIT(UnixToISO((long)ia_join_ii_pd__inventory_audit_log_b2b_prod__updated_at), 'T').$0 <= '[:END_DATE:]' 
						and ('[:PRODUCT_ID:]' == 'ALL' or '[:PRODUCT_ID:]' == ia_join_ii_pd__ii_join_pd__product_detail_b2b_prod__fsn);

raw_op_report_audits_2 = GROUP raw_op_report_audits_1 by ia_join_ii_pd__inventory_audit_log_b2b_prod__inventory_item_id;

raw_op_report_audits_3 = foreach raw_op_report_audits_2 {
	A = ORDER raw_op_report_audits_1 by ia_join_ii_pd__inventory_audit_log_b2b_prod__id desc;
	B = LIMIT A 1;
	audit = filter B by ia_join_ii_pd__inventory_audit_log_b2b_prod__quantity > 0 and storage_location_b2b_prod__label != 'outbound_shipment_table';

	generate flatten(audit.ia_join_ii_pd__ii_join_pd__inventory_item_b2b_prod__warehouse_id), 
		 flatten(audit.ia_join_ii_pd__ii_join_pd__inventory_item_b2b_prod__fsn), 
		 flatten(audit.ia_join_ii_pd__ii_join_pd__inventory_item_b2b_prod__sku),
		 flatten(audit.ia_join_ii_pd__ii_join_pd__product_detail_b2b_prod__cms_vertical),
		 flatten(audit.ia_join_ii_pd__inventory_audit_log_b2b_prod__quantity);
       };

raw_op_group_fsn_audits = GROUP raw_op_report_audits_3 by (ia_join_ii_pd__ii_join_pd__inventory_item_b2b_prod__warehouse_id, ia_join_ii_pd__ii_join_pd__inventory_item_b2b_prod__fsn, ia_join_ii_pd__ii_join_pd__inventory_item_b2b_prod__sku, ia_join_ii_pd__ii_join_pd__product_detail_b2b_prod__cms_vertical);

raw_op_report_audits_end = foreach raw_op_group_fsn_audits generate group.$0 as warehouse_id, group.$1 as fsn, group.$2 as sku, group.$3 as cms_vertical, SUM(raw_op_report_audits_3.ia_join_ii_pd__inventory_audit_log_b2b_prod__quantity) as qty;

report_grns = foreach raw_filtered_grn_join_ii generate ii_join_pd__inventory_item_b2b_prod__warehouse_id as warehouse_id, CONCAT('GRN',grn_b2b_prod__id) as internal_id, grn_b2b_prod__purchase_order_id as external_id, grn_b2b_prod__fsn as fsn, grn_b2b_prod__sku as sku, ii_join_pd__product_detail_b2b_prod__cms_vertical as cms_vertical, (long)grn_b2b_prod__quantity as qty, UnixToISO((long)grn_b2b_prod__updated_at) as time, 'inwarded' as transaction_type, '' as created_by;

report_returns_rto = foreach report_rto_reship_received generate shipment_b2b_prod__origin_party_id as warehouse_id, CONCAT('SH-',shipment_b2b_prod__id) as internal_id, shipment_b2b_prod__tracking_id as external_id, raw_si_pd__shipment_item_b2b_prod__fsn as fsn, raw_si_pd__shipment_item_b2b_prod__sku as sku, raw_si_pd__product_detail_b2b_prod__cms_vertical as cms_vertical, (long)raw_si_pd__shipment_item_b2b_prod__quantity as qty, UnixToISO((long)shipment_status_b2b_prod__updated_at) as time, shipment_status_b2b_prod__status as transaction_type, '' as created_by;

report_returns_sales = foreach report_return_received generate shipment_b2b_prod__destination_party_id as warehouse_id, CONCAT('SH-',shipment_b2b_prod__id) as internal_id, shipment_b2b_prod__return_id as external_id, raw_si_pd__shipment_item_b2b_prod__fsn as fsn, raw_si_pd__shipment_item_b2b_prod__sku as sku, raw_si_pd__product_detail_b2b_prod__cms_vertical as cms_vertical, (long)raw_si_pd__shipment_item_b2b_prod__quantity as qty, UnixToISO((long)shipment_status_b2b_prod__updated_at) as time, 'Sales return' as transaction_type, '' as created_by;

report_dispatched = foreach report_shipment_dispatched generate shipment_b2b_prod__origin_party_id as warehouse_id, CONCAT('SH-',shipment_b2b_prod__id) as internal_id, shipment_b2b_prod__tracking_id as external_id, raw_si_pd__shipment_item_b2b_prod__fsn as fsn, raw_si_pd__shipment_item_b2b_prod__sku as sku, raw_si_pd__product_detail_b2b_prod__cms_vertical as cms_vertical, (long)-1*raw_si_pd__shipment_item_b2b_prod__quantity as qty, UnixToISO((long)shipment_status_b2b_prod__updated_at) as time, 'dispatched' as transaction_type, '' as created_by;

report_variances = foreach raw_report_variances generate ivm_join_ii_pd__ii_join_pd__inventory_item_b2b_prod__warehouse_id as warehouse_id, CONCAT('VAR',iiv_join_ivr__inventory_item_variance_b2b_prod__id) as internal_id, 'NA' as external_id, iiv_join_ivr__inventory_item_variance_b2b_prod__fsn as fsn, iiv_join_ivr__inventory_item_variance_b2b_prod__sku as sku, ivm_join_ii_pd__ii_join_pd__product_detail_b2b_prod__cms_vertical as cms_vertical, (long)iiv_join_ivr__inventory_item_variance_b2b_prod__variance as qty, UnixToISO((long)iiv_join_ivr__inventory_item_variance_b2b_prod__updated_at) as time, iiv_join_ivr__inventory_variance_reason_b2b_prod__reason_type as transaction_type, iiv_join_ivr__inventory_item_variance_b2b_prod__created_by as created_by;

 report_iwt_transferred_to = foreach filtered_iwt_join_pd_sl_b2b generate iwt_join_pd_join_source__iwt_join_pd__wh_inventory_transfer_b2b__dest_warehouse_id as warehouse_id, CONCAT('VARPOS',iwt_join_pd_join_source__iwt_join_pd__wh_inventory_transfer_b2b__id) as internal_id, 'NA' as external_id, iwt_join_pd_join_source__iwt_join_pd__product_detail_b2b_prod__fsn as fsn, iwt_join_pd_join_source__iwt_join_pd__product_detail_b2b_prod__sku as sku, iwt_join_pd_join_source__iwt_join_pd__product_detail_b2b_prod__cms_vertical as cms_vertical, iwt_join_pd_join_source__iwt_join_pd__wh_inventory_transfer_b2b__quantity as qty, UnixToISO((long)iwt_join_pd_join_source__iwt_join_pd__wh_inventory_transfer_b2b__updated_at) as time,'IWT : Transferred to this wh' as transaction_type, '' as created_by; 

report_iwt_transferred_from = foreach filtered_iwt_join_pd_sl_b2b generate iwt_join_pd_join_source__iwt_join_pd__wh_inventory_transfer_b2b__source_warehouse_id as warehouse_id, CONCAT('VARNEG',iwt_join_pd_join_source__iwt_join_pd__wh_inventory_transfer_b2b__id) as internal_id, 'NA' as external_id, iwt_join_pd_join_source__iwt_join_pd__product_detail_b2b_prod__fsn as fsn, iwt_join_pd_join_source__iwt_join_pd__product_detail_b2b_prod__sku as sku, iwt_join_pd_join_source__iwt_join_pd__product_detail_b2b_prod__cms_vertical as cms_vertical, -iwt_join_pd_join_source__iwt_join_pd__wh_inventory_transfer_b2b__quantity as qty, UnixToISO((long)iwt_join_pd_join_source__iwt_join_pd__wh_inventory_transfer_b2b__updated_at) as time,'IWT : Transferred from this wh' as transaction_type, '' as created_by;

report_ot_migration = foreach group_ot_ia_by_ii1 generate null::ia_sl_join_ii_pd::ia_join_ii_pd__ii_join_pd__inventory_item_b2b_prod__warehouse_id as warehouse_id, CONCAT('OT',null::filtered_ot::id) as internal_id, 'NA' as external_id, null::ia_sl_join_ii_pd::ia_join_ii_pd__ii_join_pd__inventory_item_b2b_prod__fsn as fsn, null::ia_sl_join_ii_pd::ia_join_ii_pd__ii_join_pd__inventory_item_b2b_prod__sku as sku, null::ia_sl_join_ii_pd::ia_join_ii_pd__ii_join_pd__product_detail_b2b_prod__cms_vertical as cms_vertical, null::ia_sl_join_ii_pd::ia_join_ii_pd__inventory_audit_log_b2b_prod__quantity as quantity, UnixToISO((long)null::filtered_ot::updated_at) as time, 'OT to FLO migration' as transaction_type, '' as created_by;

report_op_audits_start = foreach raw_op_report_audits_start generate warehouse_id, 'NA' as internal_id, 'NA' as external_id, fsn, sku, cms_vertical, (long)qty, '[:START_DATE:]' as time ,'Opening Stock' as transaction_type, '' as created_by;

report_op_audits_end = foreach raw_op_report_audits_end generate warehouse_id, 'NA' as internal_id, 'NA' as external_id, fsn, sku, cms_vertical, qty, CONCAT('[:END_DATE:]',' 23:59:59') as time ,'Closing Stock' as transaction_type, '' as created_by;

reports_closing_negative = foreach report_op_audits_end generate warehouse_id, internal_id, external_id, fsn, sku, cms_vertical, -qty as qty, time, transaction_type, created_by;
                               
reports_opening_plus_transactions = UNION report_grns, report_returns_rto, report_returns_sales, report_dispatched, report_variances, report_op_audits_start, reports_closing_negative, report_iwt_transferred_to, report_iwt_transferred_from, report_ot_migration;

reports_group = GROUP reports_opening_plus_transactions by (warehouse_id, fsn, sku, cms_vertical);
                                
reports_adjustments = foreach reports_group generate group.$0 as warehouse_id, 'NA' as internal_id, 'NA' as external_id, group.fsn, group.sku, group.cms_vertical, -SUM(reports_opening_plus_transactions.qty) as qty, CONCAT('[:END_DATE:]',' 23:59:58') as time, 'System Error' as transaction_type, '' as created_by;
                              
reports_adjustments_filtered = filter reports_adjustments by qty != 0;
                              
reports_union = UNION report_grns, report_returns_rto, report_returns_sales, report_dispatched, report_variances, report_op_audits_start, reports_adjustments_filtered, report_op_audits_end, report_iwt_transferred_to, report_iwt_transferred_from, report_ot_migration;
                             
filtered_RESULT = filter reports_union by ('[:WAREHOUSE_ID:]' == 'ALL' or '[:WAREHOUSE_ID:]' == warehouse_id);
                                
RESULT = ORDER filtered_RESULT by time, warehouse_id;
