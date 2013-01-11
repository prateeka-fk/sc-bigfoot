USE ia_sl_join_ii_pd = ia_sl_join_ii_pd_b2b;

DEFINE UnixToISO org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO();
			                              
raw_op_report_audits_1 = filter ia_sl_join_ii_pd by UnixToISO((long)ia_join_ii_pd__inventory_audit_log_b2b_prod__updated_at) < '[:DATE:]' and ('[:WAREHOUSE_ID:]' == 'ALL' or '[:WAREHOUSE_ID:]' == ia_join_ii_pd__ii_join_pd__inventory_item_b2b_prod__warehouse_id) and ('[:VERTICAL:]' == 'ALL' or '[:VERTICAL:]' == ia_join_ii_pd__ii_join_pd__product_detail_b2b_prod__cms_vertical) and ('[:PRODUCT_ID:]' == 'ALL' or '[:PRODUCT_ID:]' == ia_join_ii_pd__ii_join_pd__product_detail_b2b_prod__fsn);

raw_op_report_audits_2 = GROUP raw_op_report_audits_1 by ia_join_ii_pd__inventory_audit_log_b2b_prod__inventory_item_id;
																raw_op_report_audits_3 = foreach raw_op_report_audits_2 {
                                           A = ORDER raw_op_report_audits_1 by ia_join_ii_pd__inventory_audit_log_b2b_prod__updated_at desc, ia_join_ii_pd__inventory_audit_log_b2b_prod__id desc;
					   B = LIMIT A 1;
					   audit = filter B by ia_join_ii_pd__inventory_audit_log_b2b_prod__quantity > 0 and storage_location_b2b_prod__label != 'outbound_shipment_table';
					 generate flatten(audit.ia_join_ii_pd__ii_join_pd__inventory_item_b2b_prod__warehouse_id), flatten(audit.ia_join_ii_pd__ii_join_pd__inventory_item_b2b_prod__fsn), flatten(audit.ia_join_ii_pd__ii_join_pd__inventory_item_b2b_prod__sku),flatten(audit.storage_location_b2b_prod__label),
flatten(audit.storage_location_b2b_prod__location_type),                        flatten(audit.ia_join_ii_pd__ii_join_pd__product_detail_b2b_prod__cms_vertical),  flatten(audit.ia_join_ii_pd__inventory_audit_log_b2b_prod__quantity);
				};
																raw_op_group_fsn_audits = GROUP raw_op_report_audits_3 by (ia_join_ii_pd__ii_join_pd__inventory_item_b2b_prod__warehouse_id, ia_join_ii_pd__ii_join_pd__inventory_item_b2b_prod__fsn, ia_join_ii_pd__ii_join_pd__inventory_item_b2b_prod__sku, ia_join_ii_pd__ii_join_pd__product_detail_b2b_prod__cms_vertical, storage_location_b2b_prod__location_type, storage_location_b2b_prod__label);
																raw_op_report_audits_start = foreach raw_op_group_fsn_audits generate group.$0 as warehouse_id, group.$1 as fsn, group.$2 as sku, group.$3 as cms_vertical, SUM(raw_op_report_audits_3.ia_join_ii_pd__inventory_audit_log_b2b_prod__quantity) as qty, group.$4 as location_type, group.$5 as label;

report_op_audits_start = foreach raw_op_report_audits_start generate warehouse_id, fsn, sku, cms_vertical, (long)qty, location_type, label, '[:DATE:]' as time ;

RESULT = ORDER report_op_audits_start by time, warehouse_id;
