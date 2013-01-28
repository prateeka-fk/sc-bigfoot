USE grn_join_ii_pd = grn_join_ii_pd;
USE ia_sl_join_ii_pd = ia_sl_join_ii_pd;
USE iiv_join_ivr_ii_pd  = iiv_join_ivr_ii_pd ;
USE s_join_ss_si_pd = s_join_ss_si_pd_pkey ;

DEFINE UnixToISO org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO();

raw_filtered_grn_join_ii = filter grn_join_ii_pd by (chararray)STRSPLIT(UnixToISO((long)grn_b2c_prod__updated_at), 'T').$0 >= '[:START_DATE:]' and (chararray)STRSPLIT(UnixToISO((long)grn_b2c_prod__updated_at), 'T').$0 <= '[:END_DATE:]' and ('[:PRODUCT_ID:]' == 'ALL' or '[:PRODUCT_ID:]' == ii_join_pd__product_detail_b2c_prod__fsn);

report_rto_reship_received = filter s_join_ss_si_pd by ( shipment_status_b2c_prod__status == 'returned' or shipment_status_b2c_prod__status == 'reship_received' ) and shipment_b2c_prod__shipment_type == 'outgoing' and (chararray)STRSPLIT(UnixToISO((long)shipment_status_b2c_prod__updated_at), 'T').$0 >= '[:START_DATE:]' and (chararray)STRSPLIT(UnixToISO((long)shipment_status_b2c_prod__updated_at), 'T').$0 <= '[:END_DATE:]' and ('[:PRODUCT_ID:]' == 'ALL' or '[:PRODUCT_ID:]' == raw_si_pd__product_detail_b2c_prod__fsn);

report_return_received = filter s_join_ss_si_pd by shipment_status_b2c_prod__status == 'received' and shipment_b2c_prod__shipment_type == 'incoming' and (chararray)STRSPLIT(UnixToISO((long)shipment_status_b2c_prod__updated_at), 'T').$0 >= '[:START_DATE:]' and (chararray)STRSPLIT(UnixToISO((long)shipment_status_b2c_prod__updated_at), 'T').$0 <= '[:END_DATE:]' and ('[:PRODUCT_ID:]' == 'ALL' or '[:PRODUCT_ID:]' == raw_si_pd__product_detail_b2c_prod__fsn);

report_shipment_dispatched = filter s_join_ss_si_pd by shipment_status_b2c_prod__status == 'dispatched' and shipment_b2c_prod__shipment_type == 'outgoing' and (chararray)STRSPLIT(UnixToISO((long)shipment_status_b2c_prod__updated_at), 'T').$0 >= '[:START_DATE:]' and (chararray)STRSPLIT(UnixToISO((long)shipment_status_b2c_prod__updated_at), 'T').$0 <= '[:END_DATE:]' and ('[:PRODUCT_ID:]' == 'ALL' or '[:PRODUCT_ID:]' == raw_si_pd__product_detail_b2c_prod__fsn);

raw_report_variances = filter iiv_join_ivr_ii_pd by (chararray)STRSPLIT(UnixToISO((long)iiv_join_ivr__inventory_item_variance_b2c_prod__updated_at), 'T').$0 >= '[:START_DATE:]' and (chararray)STRSPLIT(UnixToISO((long)iiv_join_ivr__inventory_item_variance_b2c_prod__updated_at), 'T').$0 <= '[:END_DATE:]' and ('[:PRODUCT_ID:]' == 'ALL' or '[:PRODUCT_ID:]' == ivm_join_ii_pd__ii_join_pd__product_detail_b2c_prod__fsn);

raw_op_report_audits_1 = filter ia_sl_join_ii_pd by (chararray)STRSPLIT(UnixToISO((long)ia_join_ii_pd__inventory_audit_log_b2c_prod__updated_at), 'T').$0 < '[:START_DATE:]' 
						and ('[:PRODUCT_ID:]' == 'ALL' or '[:PRODUCT_ID:]' == ia_join_ii_pd__ii_join_pd__product_detail_b2c_prod__fsn);
                                            
raw_op_report_audits_2 = GROUP raw_op_report_audits_1 by ia_join_ii_pd__inventory_audit_log_b2c_prod__inventory_item_id;
                         
raw_op_report_audits_3 = foreach raw_op_report_audits_2 {
	A = ORDER raw_op_report_audits_1 by ia_join_ii_pd__inventory_audit_log_b2c_prod__id desc;
	B = LIMIT A 1;
	audit = filter B by ia_join_ii_pd__inventory_audit_log_b2c_prod__quantity > 0 and storage_location_b2c_prod__label != 'outbound_shipment_table';

	generate flatten(audit.ia_join_ii_pd__ii_join_pd__inventory_item_b2c_prod__warehouse_id), 
		 flatten(audit.ia_join_ii_pd__ii_join_pd__inventory_item_b2c_prod__fsn), 
		 flatten(audit.ia_join_ii_pd__ii_join_pd__inventory_item_b2c_prod__sku), 
		 flatten(audit.ia_join_ii_pd__ii_join_pd__product_detail_b2c_prod__cms_vertical),  
		 flatten(audit.ia_join_ii_pd__inventory_audit_log_b2c_prod__quantity);
       };

raw_op_group_fsn_audits = GROUP raw_op_report_audits_3 by (ia_join_ii_pd__ii_join_pd__inventory_item_b2c_prod__warehouse_id, ia_join_ii_pd__ii_join_pd__inventory_item_b2c_prod__fsn, ia_join_ii_pd__ii_join_pd__inventory_item_b2c_prod__sku, ia_join_ii_pd__ii_join_pd__product_detail_b2c_prod__cms_vertical);

raw_op_report_audits_start = foreach raw_op_group_fsn_audits generate group.$0 as warehouse_id, group.$1 as fsn, group.$2 as sku, group.$3 as cms_vertical, SUM(raw_op_report_audits_3.ia_join_ii_pd__inventory_audit_log_b2c_prod__quantity) as qty;


raw_op_report_audits_1 = filter ia_sl_join_ii_pd by (chararray)STRSPLIT(UnixToISO((long)ia_join_ii_pd__inventory_audit_log_b2c_prod__updated_at), 'T').$0 <= '[:END_DATE:]' 
						and ('[:PRODUCT_ID:]' == 'ALL' or '[:PRODUCT_ID:]' == ia_join_ii_pd__ii_join_pd__product_detail_b2c_prod__fsn);

raw_op_report_audits_2 = GROUP raw_op_report_audits_1 by ia_join_ii_pd__inventory_audit_log_b2c_prod__inventory_item_id;

raw_op_report_audits_3 = foreach raw_op_report_audits_2 {
	A = ORDER raw_op_report_audits_1 by ia_join_ii_pd__inventory_audit_log_b2c_prod__id desc;
	B = LIMIT A 1;
	audit = filter B by ia_join_ii_pd__inventory_audit_log_b2c_prod__quantity > 0 and storage_location_b2c_prod__label != 'outbound_shipment_table';

	generate flatten(audit.ia_join_ii_pd__ii_join_pd__inventory_item_b2c_prod__warehouse_id), 
		 flatten(audit.ia_join_ii_pd__ii_join_pd__inventory_item_b2c_prod__fsn), 
		 flatten(audit.ia_join_ii_pd__ii_join_pd__inventory_item_b2c_prod__sku),
		 flatten(audit.ia_join_ii_pd__ii_join_pd__product_detail_b2c_prod__cms_vertical),
		 flatten(audit.ia_join_ii_pd__inventory_audit_log_b2c_prod__quantity);
       };

raw_op_group_fsn_audits = GROUP raw_op_report_audits_3 by (ia_join_ii_pd__ii_join_pd__inventory_item_b2c_prod__warehouse_id, ia_join_ii_pd__ii_join_pd__inventory_item_b2c_prod__fsn, ia_join_ii_pd__ii_join_pd__inventory_item_b2c_prod__sku, ia_join_ii_pd__ii_join_pd__product_detail_b2c_prod__cms_vertical);

raw_op_report_audits_end = foreach raw_op_group_fsn_audits generate group.$0 as warehouse_id, group.$1 as fsn, group.$2 as sku, group.$3 as cms_vertical, SUM(raw_op_report_audits_3.ia_join_ii_pd__inventory_audit_log_b2c_prod__quantity) as qty;

report_grns = foreach raw_filtered_grn_join_ii generate ii_join_pd__inventory_item_b2c_prod__warehouse_id as warehouse_id, CONCAT('GRN',grn_b2c_prod__id) as internal_id, grn_b2c_prod__purchase_order_id as external_id, grn_b2c_prod__fsn as fsn, grn_b2c_prod__sku as sku, ii_join_pd__product_detail_b2c_prod__cms_vertical as cms_vertical, (long)grn_b2c_prod__quantity as qty, UnixToISO((long)grn_b2c_prod__updated_at) as time, 'inwarded' as transaction_type, '' as created_by;

report_returns_rto = foreach report_rto_reship_received generate shipment_b2c_prod__origin_party_id as warehouse_id, CONCAT('SH-',shipment_b2c_prod__id) as internal_id, shipment_b2c_prod__tracking_id as external_id, raw_si_pd__shipment_item_b2c_prod__fsn as fsn, raw_si_pd__shipment_item_b2c_prod__sku as sku, raw_si_pd__product_detail_b2c_prod__cms_vertical as cms_vertical, (long)raw_si_pd__shipment_item_b2c_prod__quantity as qty, UnixToISO((long)shipment_status_b2c_prod__updated_at) as time, shipment_status_b2c_prod__status as transaction_type, '' as created_by;

report_returns_sales = foreach report_return_received generate shipment_b2c_prod__destination_party_id as warehouse_id, CONCAT('SH-',shipment_b2c_prod__id) as internal_id, shipment_b2c_prod__return_id as external_id, raw_si_pd__shipment_item_b2c_prod__fsn as fsn, raw_si_pd__shipment_item_b2c_prod__sku as sku, raw_si_pd__product_detail_b2c_prod__cms_vertical as cms_vertical, (long)raw_si_pd__shipment_item_b2c_prod__quantity as qty, UnixToISO((long)shipment_status_b2c_prod__updated_at) as time, 'Sales return' as transaction_type, '' as created_by;

report_dispatched = foreach report_shipment_dispatched generate shipment_b2c_prod__origin_party_id as warehouse_id, CONCAT('SH-',shipment_b2c_prod__id) as internal_id, shipment_b2c_prod__tracking_id as external_id, raw_si_pd__shipment_item_b2c_prod__fsn as fsn, raw_si_pd__shipment_item_b2c_prod__sku as sku, raw_si_pd__product_detail_b2c_prod__cms_vertical as cms_vertical, (long)-1*raw_si_pd__shipment_item_b2c_prod__quantity as qty, UnixToISO((long)shipment_status_b2c_prod__updated_at) as time, 'dispatched' as transaction_type, '' as created_by;

report_variances = foreach raw_report_variances generate ivm_join_ii_pd__ii_join_pd__inventory_item_b2c_prod__warehouse_id as warehouse_id, CONCAT('VAR',iiv_join_ivr__inventory_item_variance_b2c_prod__id) as internal_id, 'NA' as external_id, iiv_join_ivr__inventory_item_variance_b2c_prod__fsn as fsn, iiv_join_ivr__inventory_item_variance_b2c_prod__sku as sku, ivm_join_ii_pd__ii_join_pd__product_detail_b2c_prod__cms_vertical as cms_vertical, (long)iiv_join_ivr__inventory_item_variance_b2c_prod__variance as qty, UnixToISO((long)iiv_join_ivr__inventory_item_variance_b2c_prod__updated_at) as time, iiv_join_ivr__inventory_variance_reason_b2c_prod__reason_type as transaction_type, iiv_join_ivr__inventory_item_variance_b2c_prod__created_by as created_by;

report_op_audits_start = foreach raw_op_report_audits_start generate warehouse_id, 'NA' as internal_id, 'NA' as external_id, fsn, sku, cms_vertical, (long)qty, '[:START_DATE:]' as time ,'Opening Stock' as transaction_type, '' as created_by;

report_op_audits_end = foreach raw_op_report_audits_end generate warehouse_id, 'NA' as internal_id, 'NA' as external_id, fsn, sku, cms_vertical, qty, CONCAT('[:END_DATE:]',' 23:59:59') as time ,'Closing Stock' as transaction_type, '' as created_by;

reports_closing_negative = foreach report_op_audits_end generate warehouse_id, internal_id, external_id, fsn, sku, cms_vertical, -qty as qty, time, transaction_type, created_by;
                               
reports_opening_plus_transactions = UNION report_grns, report_returns_rto, report_returns_sales, report_dispatched, report_variances, report_op_audits_start, reports_closing_negative;

reports_group = GROUP reports_opening_plus_transactions by (warehouse_id, fsn, sku, cms_vertical);
                                
reports_adjustments = foreach reports_group generate group.$0 as warehouse_id, 'NA' as internal_id, 'NA' as external_id, group.fsn, group.sku, group.cms_vertical, -SUM(reports_opening_plus_transactions.qty) as qty, CONCAT('[:END_DATE:]',' 23:59:58') as time, 'System Error' as transaction_type, '' as created_by;
                              
reports_adjustments_filtered = filter reports_adjustments by qty != 0;
                              
reports_union = UNION report_grns, report_returns_rto, report_returns_sales, report_dispatched, report_variances, report_op_audits_start, reports_adjustments_filtered, report_op_audits_end;
                             
filtered_wh_vertical_RESULT = filter reports_union by ('[:VERTICAL:]' == 'ALL' or '[:VERTICAL:]' == cms_vertical) and ('[:WAREHOUSE_ID:]' == 'ALL' or '[:WAREHOUSE_ID:]' == warehouse_id);
                                
RESULT = ORDER filtered_wh_vertical_RESULT by time, warehouse_id;
