DEFINE UnixToISO org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO();

USE grn_join_ii = warehouse_b2b_production_inventory_grns_view;
USE iwt_join_sl_b2b = warehouse_b2b_production_warehouse_inventory_transfers_view;
USE ot_inventory_migration_b2b = YetiMerge_warehouse_b2b_production_opentaps_inventory_migrations;
USE ia_sl_join_ii = warehouse_b2b_production_inventory_transactions_view;
USE s_join_ss_si = warehouse_b2b_production_shipments_view;
USE iiv_join_ivr_ii = warehouse_b2b_production_inventory_variances_view;

-- GRN INWARDED --
raw_filtered_grn_join_ii = filter grn_join_ii by (chararray)STRSPLIT(UnixToISO((long)warehouse_b2b_production_goods_receipt_notes__updated_at), 'T').$0 >= '[:START_DATE:]' and (chararray)STRSPLIT(UnixToISO((long)warehouse_b2b_production_goods_receipt_notes__updated_at), 'T').$0 <= '[:END_DATE:]' and ('[:PRODUCT_ID:]' == 'ALL' or '[:PRODUCT_ID:]' == warehouse_b2b_production_inventory_items__fsn);

-- INTER WAREHOUSE TRANSFER --
filtered_iwt_join_sl_b2b = filter iwt_join_sl_b2b by (chararray)STRSPLIT(UnixToISO((long)iwt_join_source__warehouse_b2b_production_warehouse_inventory_transfers__updated_at), 'T').$0 >= '[:START_DATE:]' and (chararray)STRSPLIT(UnixToISO((long)iwt_join_source__warehouse_b2b_production_warehouse_inventory_transfers__updated_at), 'T').$0 <= '[:END_DATE:]' and ('[:PRODUCT_ID:]' == 'ALL' or '[:PRODUCT_ID:]' == iwt_join_source__warehouse_b2b_production_warehouse_inventory_transfers__fsn); 

-- OPENTAPS MIGRATION --
filtered_ot = filter ot_inventory_migration_b2b by (chararray)STRSPLIT(UnixToISO((long)updated_at), 'T').$0 >= '[:START_DATE:]' and (chararray)STRSPLIT(UnixToISO((long)updated_at), 'T').$0 <= '[:END_DATE:]';      
ot_ia = join filtered_ot by inventory_item_id, ia_sl_join_ii by ia_join_ii__warehouse_b2b_production_inventory_items__id;        
group_ot_ia_by_ii = GROUP ot_ia by filtered_ot::inventory_item_id;      

group_ot_ia_by_ii1 = foreach group_ot_ia_by_ii {
A = ORDER ot_ia by ia_join_ii__warehouse_b2b_production_inventory_audit_logs__id;
B = LIMIT A 1;
generate flatten(B.ia_sl_join_ii::ia_join_ii__warehouse_b2b_production_inventory_items__warehouse_id), 
flatten(B.filtered_ot::id),
flatten(B.ia_sl_join_ii::ia_join_ii__warehouse_b2b_production_inventory_items__fsn),
flatten(B.ia_sl_join_ii::ia_join_ii__warehouse_b2b_production_inventory_items__sku),
flatten(B.ia_sl_join_ii::ia_join_ii__warehouse_b2b_production_inventory_audit_logs__quantity),
flatten(B.filtered_ot::updated_at);
};                                                                                    

-- SHIPMENTS DISPATCHED --
report_shipment_dispatched = filter s_join_ss_si by warehouse_b2b_production_shipment_statuses__status == 'dispatched' and warehouse_b2b_production_shipments__shipment_type == 'outgoing' and (chararray)STRSPLIT(UnixToISO((long)warehouse_b2b_production_shipment_statuses__updated_at), 'T').$0 >= '[:START_DATE:]' and (chararray)STRSPLIT(UnixToISO((long)warehouse_b2b_production_shipment_statuses__updated_at), 'T').$0 <= '[:END_DATE:]' and ('[:PRODUCT_ID:]' == 'ALL' or '[:PRODUCT_ID:]' == warehouse_b2b_production_shipment_items__fsn);

-- VARIANCES --
report_variances = filter iiv_join_ivr_ii by (chararray)STRSPLIT(UnixToISO((long)iiv_join_ivr__warehouse_b2b_production_inventory_item_variances__updated_at), 'T').$0 >= '[:START_DATE:]' and (chararray)STRSPLIT(UnixToISO((long)iiv_join_ivr__warehouse_b2b_production_inventory_item_variances__updated_at), 'T').$0 <= '[:END_DATE:]' and ('[:PRODUCT_ID:]' == 'ALL' or '[:PRODUCT_ID:]' == ivm_join_ii__warehouse_b2b_production_inventory_items__fsn);

-- OPENING STOCK --
raw_op_report_audits_1 = filter ia_sl_join_ii by (chararray)STRSPLIT(UnixToISO((long)ia_join_ii__warehouse_b2b_production_inventory_audit_logs__updated_at), 'T').$0 < '[:START_DATE:]' and ('[:PRODUCT_ID:]' == 'ALL' or '[:PRODUCT_ID:]' == ia_join_ii__warehouse_b2b_production_inventory_items__fsn);

raw_op_report_audits_2 = GROUP raw_op_report_audits_1 by ia_join_ii__warehouse_b2b_production_inventory_audit_logs__inventory_item_id;
raw_op_report_audits_3 = foreach raw_op_report_audits_2 {

A = ORDER raw_op_report_audits_1 by ia_join_ii__warehouse_b2b_production_inventory_audit_logs__id desc; 
B = LIMIT A 1;
audit = filter B by ia_join_ii__warehouse_b2b_production_inventory_audit_logs__quantity > 0 and warehouse_b2b_production_storage_locations__label != 'outbound_shipment_table';
generate flatten(audit.ia_join_ii__warehouse_b2b_production_inventory_items__warehouse_id), flatten(audit.ia_join_ii__warehouse_b2b_production_inventory_items__fsn), flatten(audit.ia_join_ii__warehouse_b2b_production_inventory_items__sku),  
flatten(audit.ia_join_ii__warehouse_b2b_production_inventory_audit_logs__quantity);

};

raw_op_group_fsn_audits = GROUP raw_op_report_audits_3 by (ia_join_ii__warehouse_b2b_production_inventory_items__warehouse_id, ia_join_ii__warehouse_b2b_production_inventory_items__fsn, ia_join_ii__warehouse_b2b_production_inventory_items__sku);

raw_op_report_audits_start = foreach raw_op_group_fsn_audits generate group.$0 as warehouse_id, group.$1 as fsn, group.$2 as sku, SUM(raw_op_report_audits_3.ia_join_ii__warehouse_b2b_production_inventory_audit_logs__quantity) as qty;

-- CLOSING STOCK --
raw_op_report_audits_1 = filter ia_sl_join_ii by (chararray)STRSPLIT(UnixToISO((long)ia_join_ii__warehouse_b2b_production_inventory_audit_logs__updated_at), 'T').$0 <= '[:END_DATE:]' and ('[:PRODUCT_ID:]' == 'ALL' or '[:PRODUCT_ID:]' == ia_join_ii__warehouse_b2b_production_inventory_items__fsn);

raw_op_report_audits_2 = GROUP raw_op_report_audits_1 by ia_join_ii__warehouse_b2b_production_inventory_audit_logs__inventory_item_id;
raw_op_report_audits_3 = foreach raw_op_report_audits_2 {

A = ORDER raw_op_report_audits_1 by ia_join_ii__warehouse_b2b_production_inventory_audit_logs__id desc; 
B = LIMIT A 1;
audit = filter B by ia_join_ii__warehouse_b2b_production_inventory_audit_logs__quantity > 0 and warehouse_b2b_production_storage_locations__label != 'outbound_shipment_table';
generate flatten(audit.ia_join_ii__warehouse_b2b_production_inventory_items__warehouse_id), flatten(audit.ia_join_ii__warehouse_b2b_production_inventory_items__fsn), flatten(audit.ia_join_ii__warehouse_b2b_production_inventory_items__sku),  
flatten(audit.ia_join_ii__warehouse_b2b_production_inventory_audit_logs__quantity);

};

raw_op_group_fsn_audits = GROUP raw_op_report_audits_3 by (ia_join_ii__warehouse_b2b_production_inventory_items__warehouse_id, ia_join_ii__warehouse_b2b_production_inventory_items__fsn, ia_join_ii__warehouse_b2b_production_inventory_items__sku);

raw_op_report_audits_end = foreach raw_op_group_fsn_audits generate group.$0 as warehouse_id, group.$1 as fsn, group.$2 as sku, SUM(raw_op_report_audits_3.ia_join_ii__warehouse_b2b_production_inventory_audit_logs__quantity) as qty;

-- FINAL REPORTS FORMATTING --
report_grns = foreach raw_filtered_grn_join_ii generate warehouse_b2b_production_inventory_items__warehouse_id as warehouse_id, CONCAT('GRN',warehouse_b2b_production_goods_receipt_notes__id) as internal_id, warehouse_b2b_production_goods_receipt_notes__purchase_order_id as external_id, warehouse_b2b_production_inventory_items__fsn as fsn, warehouse_b2b_production_inventory_items__sku as sku, (long)warehouse_b2b_production_goods_receipt_notes__quantity as qty, UnixToISO((long)warehouse_b2b_production_goods_receipt_notes__updated_at) as time, 'inwarded' as transaction_type, '' as created_by;

report_dispatched = foreach report_shipment_dispatched generate warehouse_b2b_production_shipments__origin_party_id as warehouse_id, CONCAT('SH-',warehouse_b2b_production_shipments__id) as internal_id, warehouse_b2b_production_shipments__tracking_id as external_id, warehouse_b2b_production_shipment_items__fsn as fsn, warehouse_b2b_production_shipment_items__sku as sku, (long)-1*warehouse_b2b_production_shipment_items__quantity as qty, UnixToISO((long)warehouse_b2b_production_shipment_statuses__updated_at) as time, 'dispatched' as transaction_type, '' as created_by;

report_variances = foreach report_variances generate ivm_join_ii__warehouse_b2b_production_inventory_items__warehouse_id as warehouse_id, CONCAT('VAR',iiv_join_ivr__warehouse_b2b_production_inventory_item_variances__id) as internal_id, 'NA' as external_id, ivm_join_ii__warehouse_b2b_production_inventory_items__fsn as fsn, ivm_join_ii__warehouse_b2b_production_inventory_items__sku as sku, (long)iiv_join_ivr__warehouse_b2b_production_inventory_item_variances__variance as qty, UnixToISO((long)iiv_join_ivr__warehouse_b2b_production_inventory_item_variances__updated_at) as time, iiv_join_ivr__warehouse_b2b_production_inventory_variance_reasons__reason_type as transaction_type, iiv_join_ivr__warehouse_b2b_production_inventory_item_variances__created_by as created_by;

report_iwt_transferred_to = foreach filtered_iwt_join_sl_b2b generate iwt_join_source__warehouse_b2b_production_warehouse_inventory_transfers__dest_warehouse_id as warehouse_id, CONCAT('VARPOS',iwt_join_source__warehouse_b2b_production_warehouse_inventory_transfers__id) as internal_id, 'NA' as external_id, iwt_join_source__warehouse_b2b_production_warehouse_inventory_transfers__fsn as fsn, iwt_join_source__warehouse_b2b_production_warehouse_inventory_transfers__sku as sku, iwt_join_source__warehouse_b2b_production_warehouse_inventory_transfers__quantity as qty, UnixToISO((long)iwt_join_source__warehouse_b2b_production_warehouse_inventory_transfers__updated_at) as time,'IWT : Transferred to this wh' as transaction_type, '' as created_by; 

report_iwt_transferred_from = foreach filtered_iwt_join_sl_b2b generate iwt_join_source__warehouse_b2b_production_warehouse_inventory_transfers__source_warehouse_id as warehouse_id, CONCAT('VARNEG',iwt_join_source__warehouse_b2b_production_warehouse_inventory_transfers__id) as internal_id, 'NA' as external_id, iwt_join_source__warehouse_b2b_production_warehouse_inventory_transfers__fsn as fsn, iwt_join_source__warehouse_b2b_production_warehouse_inventory_transfers__sku as sku, -iwt_join_source__warehouse_b2b_production_warehouse_inventory_transfers__quantity as qty, UnixToISO((long)iwt_join_source__warehouse_b2b_production_warehouse_inventory_transfers__updated_at) as time,'IWT : Transferred from this wh' as transaction_type, '' as created_by; 

report_ot_migration = foreach group_ot_ia_by_ii1 generate null::ia_sl_join_ii::ia_join_ii__warehouse_b2b_production_inventory_items__warehouse_id as warehouse_id, CONCAT('OT',null::filtered_ot::id) as internal_id, 'NA' as external_id, null::ia_sl_join_ii::ia_join_ii__warehouse_b2b_production_inventory_items__fsn as fsn, null::ia_sl_join_ii::ia_join_ii__warehouse_b2b_production_inventory_items__sku as sku, null::ia_sl_join_ii::ia_join_ii__warehouse_b2b_production_inventory_audit_logs__quantity as quantity, UnixToISO((long)null::filtered_ot::updated_at) as time, 'OT to FLO migration' as transaction_type, '' as created_by;

report_op_audits_start = foreach raw_op_report_audits_start generate warehouse_id, 'NA' as internal_id, 'NA' as external_id, fsn, sku, (long)qty, '[:START_DATE:]' as time ,'Opening Stock' as transaction_type, '' as created_by;

report_op_audits_end = foreach raw_op_report_audits_end generate warehouse_id, 'NA' as internal_id, 'NA' as external_id, fsn, sku, qty, CONCAT('[:END_DATE:]',' 23:59:59') as time ,'Closing Stock' as transaction_type, '' as created_by;

reports_closing_negative = foreach report_op_audits_end generate warehouse_id, internal_id, external_id, fsn, sku, -qty as qty, time, transaction_type, created_by;

reports_opening_plus_transactions = UNION report_grns, report_dispatched, report_variances, report_op_audits_start, reports_closing_negative, report_iwt_transferred_to, report_iwt_transferred_from, report_ot_migration;

reports_group = GROUP reports_opening_plus_transactions by (warehouse_id, fsn, sku);

reports_adjustments = foreach reports_group generate group.$0 as warehouse_id, 'NA' as internal_id, 'NA' as external_id, group.fsn, group.sku, -SUM(reports_opening_plus_transactions.qty) as qty, CONCAT('[:END_DATE:]',' 23:59:58') as time, 'System Error' as transaction_type, '' as created_by;

reports_adjustments_filtered = filter reports_adjustments by qty != 0;

reports_union = UNION report_grns, report_dispatched, report_variances, report_op_audits_start, reports_adjustments_filtered, report_op_audits_end, report_iwt_transferred_to, report_iwt_transferred_from, report_ot_migration;

filtered_RESULT = filter reports_union by ('[:WAREHOUSE_ID:]' == 'ALL' or '[:WAREHOUSE_ID:]' == warehouse_id);



RESULT = ORDER filtered_RESULT by warehouse_id, time;













