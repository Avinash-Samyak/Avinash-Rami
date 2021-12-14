﻿//------------------------------------------------------------------------------
// <auto-generated>
//    This code was generated from a template.
//
//    Manual changes to this file may cause unexpected behavior in your application.
//    Manual changes to this file will be overwritten if the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

namespace sde.Models
{
    using System;
    using System.Data.Entity;
    using System.Data.Entity.Infrastructure;
    
    public partial class sdeEntities : DbContext
    {
        public sdeEntities()
            : base("name=sdeEntities")
        {
        }
    
        protected override void OnModelCreating(DbModelBuilder modelBuilder)
        {
            throw new UnintentionalCodeFirstException();
        }
    
        public DbSet<backorder_cancellation> backorder_cancellation { get; set; }
        public DbSet<bcas_inventoryadjustment> bcas_inventoryadjustment { get; set; }
        public DbSet<bcas_journalcalclist> bcas_journalcalclist { get; set; }
        public DbSet<bcas_ordersfulfill> bcas_ordersfulfill { get; set; }
        public DbSet<bcas_ordersfulfill_history> bcas_ordersfulfill_history { get; set; }
        public DbSet<bcas_otherstransaction> bcas_otherstransaction { get; set; }
        public DbSet<bcas_salestransaction> bcas_salestransaction { get; set; }
        public DbSet<bcas_sep_loaded> bcas_sep_loaded { get; set; }
        public DbSet<bcas_sep_sales> bcas_sep_sales { get; set; }
        public DbSet<cpas_dataposting> cpas_dataposting { get; set; }
        public DbSet<cpas_dataposting_adj_out> cpas_dataposting_adj_out { get; set; }
        public DbSet<cpas_dataposting_check> cpas_dataposting_check { get; set; }
        public DbSet<cpas_dataposting_hs_in> cpas_dataposting_hs_in { get; set; }
        public DbSet<cpas_dataposting_parent> cpas_dataposting_parent { get; set; }
        public DbSet<cpas_journal> cpas_journal { get; set; }
        public DbSet<cpas_otherstransaction> cpas_otherstransaction { get; set; }
        public DbSet<cpas_otherstransaction_error> cpas_otherstransaction_error { get; set; }
        public DbSet<cpas_payment> cpas_payment { get; set; }
        public DbSet<cpas_presales> cpas_presales { get; set; }
        public DbSet<cpas_salestransaction> cpas_salestransaction { get; set; }
        public DbSet<cpas_stockposting> cpas_stockposting { get; set; }
        public DbSet<cpas_stockposting_history> cpas_stockposting_history { get; set; }
        public DbSet<cpas_testposting> cpas_testposting { get; set; }
        public DbSet<default_forwarder> default_forwarder { get; set; }
        public DbSet<dummyso> dummysoes { get; set; }
        public DbSet<errorlog_cpas_stockposting> errorlog_cpas_stockposting { get; set; }
        public DbSet<excesspo> excesspoes { get; set; }
        public DbSet<forwarderadd> forwarderadds { get; set; }
        public DbSet<item_tem> item_tem { get; set; }
        public DbSet<map_bin> map_bin { get; set; }
        public DbSet<map_businesschannel> map_businesschannel { get; set; }
        public DbSet<map_chartofaccount> map_chartofaccount { get; set; }
        public DbSet<map_country> map_country { get; set; }
        public DbSet<map_currency> map_currency { get; set; }
        public DbSet<map_customer> map_customer { get; set; }
        public DbSet<map_item> map_item { get; set; }
        public DbSet<map_itemprice> map_itemprice { get; set; }
        public DbSet<map_location> map_location { get; set; }
        public DbSet<map_subsidiary> map_subsidiary { get; set; }
        public DbSet<mdm_item> mdm_item { get; set; }
        //ANET-24 Threshold Handling
        //Added by Brash Developer on 11-Aug-2021-->
        public DbSet<map_master_error> map_master_error { get; set; }
        //ANET-24 Threshold Handling
        public DbSet<netsuite_adjustment> netsuite_adjustment { get; set; }
        public DbSet<netsuite_adjustment2> netsuite_adjustment2 { get; set; }
        public DbSet<netsuite_adjustmentdetail2> netsuite_adjustmentdetail2 { get; set; }
        public DbSet<netsuite_adjustmentitem> netsuite_adjustmentitem { get; set; }
        public DbSet<netsuite_dropshipfulfillment> netsuite_dropshipfulfillment { get; set; }
        public DbSet<netsuite_dropshipso> netsuite_dropshipso { get; set; }
        public DbSet<netsuite_invoice> netsuite_invoice { get; set; }
        public DbSet<netsuite_job> netsuite_job { get; set; }
        public DbSet<netsuite_jobitem> netsuite_jobitem { get; set; }
        public DbSet<netsuite_jobitem_history> netsuite_jobitem_history { get; set; }
        public DbSet<netsuite_jobmo> netsuite_jobmo { get; set; }
        public DbSet<netsuite_jobmo_address> netsuite_jobmo_address { get; set; }
        public DbSet<netsuite_jobmo_address_temp> netsuite_jobmo_address_temp { get; set; }
        public DbSet<netsuite_jobmo_pack> netsuite_jobmo_pack { get; set; }
        public DbSet<netsuite_jobmo_pack_history> netsuite_jobmo_pack_history { get; set; }
        public DbSet<netsuite_jobmocls> netsuite_jobmocls { get; set; }
        public DbSet<netsuite_jobordmaster> netsuite_jobordmaster { get; set; }
        public DbSet<netsuite_jobordmaster_pack> netsuite_jobordmaster_pack { get; set; }
        public DbSet<netsuite_jobordmaster_pack_history> netsuite_jobordmaster_pack_history { get; set; }
        public DbSet<netsuite_jobordmaster_packdetail> netsuite_jobordmaster_packdetail { get; set; }
        public DbSet<netsuite_jobordmaster_packdetail_history> netsuite_jobordmaster_packdetail_history { get; set; }
        public DbSet<netsuite_newso> netsuite_newso { get; set; }
        public DbSet<netsuite_newso_history> netsuite_newso_history { get; set; }
        public DbSet<netsuite_newso_test> netsuite_newso_test { get; set; }
        public DbSet<netsuite_pr> netsuite_pr { get; set; }
        public DbSet<netsuite_pritem> netsuite_pritem { get; set; }
        public DbSet<netsuite_return> netsuite_return { get; set; }
        public DbSet<netsuite_returnitem> netsuite_returnitem { get; set; }
        public DbSet<netsuite_returnrefund> netsuite_returnrefund { get; set; }
        public DbSet<netsuite_setting> netsuite_setting { get; set; }
        public DbSet<netsuite_syncso> netsuite_syncso { get; set; }
        public DbSet<netsuite_syncso_tem> netsuite_syncso_tem { get; set; }
        public DbSet<netsuite_syncupdateso> netsuite_syncupdateso { get; set; }
        public DbSet<netsuite_transfer> netsuite_transfer { get; set; }
        public DbSet<netsuite_transferdetail> netsuite_transferdetail { get; set; }
        public DbSet<netsuitedataformq> netsuitedataformqs { get; set; }
        public DbSet<patchdata> patchdatas { get; set; }
        public DbSet<patchmo> patchmoes { get; set; }
        public DbSet<requestmq> requestmqs { get; set; }
        public DbSet<requestnetsuite> requestnetsuites { get; set; }
        public DbSet<requestnetsuite_alarm> requestnetsuite_alarm { get; set; }
        public DbSet<requestnetsuite_history> requestnetsuite_history { get; set; }
        public DbSet<requestnetsuite_task> requestnetsuite_task { get; set; }
        public DbSet<requestnetsuite_task_history> requestnetsuite_task_history { get; set; }
        public DbSet<scheduler> schedulers { get; set; }
        public DbSet<unfulfillso> unfulfillsoes { get; set; }
        public DbSet<unscan> unscans { get; set; }
        public DbSet<userprofile> userprofiles { get; set; }
        public DbSet<user> users { get; set; }
        public DbSet<wms_cashsale> wms_cashsale { get; set; }
        public DbSet<wms_cashsaleitem> wms_cashsaleitem { get; set; }
        public DbSet<wms_directadjustment> wms_directadjustment { get; set; }
        public DbSet<wms_directadjustmentitem> wms_directadjustmentitem { get; set; }
        public DbSet<wms_directtransfer> wms_directtransfer { get; set; }
        public DbSet<wms_directtransferitem> wms_directtransferitem { get; set; }
        public DbSet<wms_excessporeceive> wms_excessporeceive { get; set; }
        public DbSet<wms_jobitem_unscan> wms_jobitem_unscan { get; set; }
        public DbSet<wms_jobordscan> wms_jobordscan { get; set; }
        public DbSet<wms_jobordscan_history> wms_jobordscan_history { get; set; }
        public DbSet<wms_jobordscan_pack> wms_jobordscan_pack { get; set; }
        public DbSet<wms_jobordscan_pack_history> wms_jobordscan_pack_history { get; set; }
        public DbSet<wms_poreceive> wms_poreceive { get; set; }
        public DbSet<wms_poreceiveitem> wms_poreceiveitem { get; set; }
        public DbSet<wms_rareceive> wms_rareceive { get; set; }
        public DbSet<wms_rareceiveitem> wms_rareceiveitem { get; set; }
        public DbSet<cpas_dataposting_history> cpas_dataposting_history { get; set; }
        public DbSet<cpas_dataposting_wrong> cpas_dataposting_wrong { get; set; }
        public DbSet<cpas_otherstransaction_history> cpas_otherstransaction_history { get; set; }
        public DbSet<cpas_payment_history> cpas_payment_history { get; set; }
        public DbSet<cpas_salestransaction_history> cpas_salestransaction_history { get; set; }
        public DbSet<dashboard_salesorder> dashboard_salesorder { get; set; }
        public DbSet<temp_resync> temp_resync { get; set; }
        public DbSet<temp_unfulfillso> temp_unfulfillso { get; set; }
        public DbSet<tempmo> tempmoes { get; set; }
        public DbSet<tosyncitem> tosyncitems { get; set; }
        public DbSet<pmd_product_category> pmd_product_category { get; set; }
        public DbSet<pmd_product_series> pmd_product_series { get; set; }
        public DbSet<view_wms_jobordscan> view_wms_jobordscan { get; set; }
    }
}