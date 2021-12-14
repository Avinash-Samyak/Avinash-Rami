using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using sde.comNetsuiteServices;
using System.Collections;

namespace sde.Models
{
	public class linq_recovery
    {
        #region TRADE-ReRun
        public string rerun_trade_sales_order_fulfill(NetSuiteService service, sdeEntities entities, string refNo, string refRange)
        {
            DateTime rangeFrom = DateTime.Parse(refRange.Substring(0, 19));
            DateTime rangeTo = DateTime.Parse(refRange.Substring(20, 19));

            Boolean status = false;
            String errorMsg = "";

            /////
            //Int32 daCount = 0;
            //Int32 itemCount = 0;
            Int32 rowCount = 0;

            try
            {
                Int32 ordCount = 0;
                Guid gjob_id = Guid.NewGuid();


                var query1 = (from q1 in entities.wms_jobordscan
                              join q2 in entities.netsuite_jobmo on q1.jos_moNo equals q2.nsjm_moNo
                              where (q1.jos_rangeTo > rangeFrom && q1.jos_rangeTo <= rangeTo)
                              && q1.jos_moNo == refNo
                              && q1.jos_businessChannel_code == "ET"
                              select new
                              {
                                  q1.jos_moNo,
                                  q2.nsjm_moNo_internalID,
                                  q1.jos_job_ID //to solve 2 sync per day - WY-10.MAR.2015
                              })
                                .Distinct()
                                .ToList();

                //status = true;
                ItemFulfillment[] iffList = new ItemFulfillment[query1.Count()];

                foreach (var q1 in query1)
                {
                    try
                    {

                        InitializeRef refSO = new InitializeRef();
                        refSO.type = InitializeRefType.salesOrder;
                        refSO.internalId = q1.nsjm_moNo_internalID;
                        refSO.typeSpecified = true;

                        InitializeRecord recSO = new InitializeRecord();
                        recSO.type = InitializeType.itemFulfillment;
                        recSO.reference = refSO;

                        ReadResponse rrSO = service.initialize(recSO);
                        Record rSO = rrSO.record;

                        ItemFulfillment iff1 = (ItemFulfillment)rSO;
                        ItemFulfillment iff2 = new ItemFulfillment();

                        String consigmentNote = string.Empty; //To Add at Memo - WY-29.SEPT.2014
                        Int32 countCNote = 0;

                        if (iff1 != null)
                        {
                            ItemFulfillmentItemList ifitemlist = iff1.itemList;

                            RecordRef refCreatedFrom = new RecordRef();
                            refCreatedFrom.internalId = iff1.createdFrom.internalId;
                            iff2.createdFrom = refCreatedFrom;

                            iff2.tranDate = DateTime.Now;
                            iff2.tranDateSpecified = true;

                            ////Added for Advanced Inventory - WY-23.JUNE.2015
                            //iff2.shipStatus = ItemFulfillmentShipStatus._shipped;
                            //iff2.shipStatusSpecified = true;

                            //Retrieve Consignment Note - WY-29.SEPT.2014
                            var cNote = (from qcNote in entities.wms_jobordscan
                                         where qcNote.jos_businessChannel_code == "ET"
                                         && (qcNote.jos_rangeTo > rangeFrom && qcNote.jos_rangeTo <= rangeTo)
                                         && qcNote.jos_moNo == q1.jos_moNo
                                         select new
                                         {
                                             qcNote.jos_deliveryRef
                                         }).Distinct().ToList();

                            for (int i = 0; i < cNote.Count; i++)
                            {
                                consigmentNote = consigmentNote + cNote[i].jos_deliveryRef;
                                countCNote = countCNote + 1;
                                if (!(countCNote == cNote.Count))
                                {
                                    consigmentNote = consigmentNote + ",";
                                }
                            }
                            iff2.memo = consigmentNote; //To Add at Memo - WY-29.SEPT.2014

                            ItemFulfillmentItem[] ifitems = new ItemFulfillmentItem[ifitemlist.item.Length];

                            var query2 = (from josp in entities.wms_jobordscan_pack
                                          join jomp in entities.netsuite_jobordmaster_pack on josp.josp_pack_ID equals jomp.nsjomp_jobOrdMaster_pack_ID
                                          join jompd in entities.netsuite_jobordmaster_packdetail on jomp.nsjomp_jobOrdMaster_pack_ID equals jompd.nsjompd_jobOrdMaster_pack_ID
                                          where josp.josp_ordFulFill > 0 && josp.josp_moNo == q1.jos_moNo
                                          && josp.josp_rangeTo > rangeFrom && josp.josp_rangeTo <= rangeTo
                                          && josp.josp_jobID == q1.jos_job_ID //to solve 2 sync per day - WY-10.MAR.2015
                                          select new
                                          {
                                              josp.josp_pack_ID,
                                              jompd.nsjompd_item_internalID,
                                              jomp.nsjomp_jobOrdMaster_ID,
                                              qty = josp.josp_ordFulFill
                                          }).ToList();

                            var groupQ2 = from p in query2
                                          let k = new
                                          {
                                              //jospPackID = p.josp_pack_ID,
                                              //jobOrdMasterID = p.nsjomp_jobOrdMaster_ID,
                                              itemInternalID = p.nsjompd_item_internalID,
                                              //fulFillQty = p.qty
                                          }
                                          group p by k into g
                                          select new
                                          {
                                              //jospPackID = g.Key.jospPackID,
                                              //jobOrdMasterID = g.Key.jobOrdMasterID,
                                              itemInternalID = g.Key.itemInternalID,
                                              fulFillQty = g.Sum(p => p.qty)
                                          };

                            List<String> deCommitItem = new List<String>();
                            List<Int32> deCommitQty = new List<Int32>();
                            Hashtable htDBItems = new Hashtable(); //Add all Netsuite items into Hash Table - WY-26.Dec.2014
                            foreach (var item in groupQ2)
                            {
                                deCommitItem.Add(item.itemInternalID);
                                deCommitQty.Add(Convert.ToInt32(item.fulFillQty));
                                htDBItems.Add(item.itemInternalID, Convert.ToInt32(item.fulFillQty));//Add all Netsuite items into Hash Table - WY-26.Dec.2014
                            }

                            /*
                            var scanpack = (from josp in entities.wms_jobordscan_pack
                                            join jomp in entities.netsuite_jobordmaster_pack on josp.josp_pack_ID equals jomp.nsjomp_jobOrdMaster_pack_ID
                                            //where josp.josp_jobOrdMaster_ID == order.nsjom_jobOrdMaster_ID
                                            where josp.josp_pack_ID == jomp.nsjomp_jobOrdMaster_pack_ID && jomp.nsjomp_jobOrdMaster_ID == order.nsjom_jobOrdMaster_ID
                                            select new { jomp.nsjomp_ordPack, jomp.nsjomp_item_internalID, josp.josp_pack_ID, josp.josp_ordFulFill, jomp.nsjomp_location_internalID }).ToList();
                            */

                            if (ifitems.Count() > 0)
                            {
                                //String josp_packID = null;
                                for (int i = 0; i < ifitemlist.item.Length; i++)
                                {
                                    ItemFulfillmentItem iffi = new ItemFulfillmentItem();
                                    Boolean isExist = false;

                                    iffi.item = ifitemlist.item[i].item;
                                    iffi.quantity = 0;
                                    iffi.quantitySpecified = true;
                                    iffi.itemIsFulfilled = false;
                                    iffi.itemIsFulfilledSpecified = true;
                                    iffi.orderLine = ifitemlist.item[i].orderLine;
                                    iffi.orderLineSpecified = true;

                                    isExist = htDBItems.Contains(ifitemlist.item[i].item.internalId);
                                    if (isExist)
                                    {
                                        int fulfilQty = (int)htDBItems[ifitemlist.item[i].item.internalId];
                                        fulfilQty = fulfilQty - Convert.ToInt32(ifitemlist.item[i].quantityRemaining);

                                        htDBItems.Remove(ifitemlist.item[i].item.internalId);
                                        htDBItems.Add(ifitemlist.item[i].item.internalId, fulfilQty);
                                    }

                                    for (int j = 0; j < deCommitItem.Count(); j++)
                                    {
                                        if (ifitemlist.item[i].item.internalId.Equals(deCommitItem[j]))
                                        {
                                            //josp_packID = item.jospPackID;

                                            /*
                                            RecordRef refLocation = new RecordRef();
                                            refLocation.internalId = item.nsjomp_location_internalID;
                                            iffi.location = refLocation;
                                            */

                                            //cpng start
                                            RecordRef refLocation = new RecordRef();
                                            refLocation.internalId = @Resource.TRADE_DEFAULT_LOCATION;
                                            iffi.location = refLocation;
                                            //cpng end

                                            if (ifitemlist.item[i].quantityRemaining > deCommitQty[j])
                                            {
                                                iffi.quantity = Convert.ToInt32(deCommitQty[j]);
                                                iffi.quantitySpecified = true;

                                                iffi.itemIsFulfilled = true;
                                                iffi.itemIsFulfilledSpecified = true;

                                                Double excessQty = Convert.ToDouble(ifitemlist.item[i].quantityRemaining - deCommitQty[j]);

                                                refNo = "WMSJOBORDSCANPACK.MONO." + q1.jos_moNo + "." + convertDateToString(rangeFrom) + "-" + convertDateToString(rangeTo);

                                                var insertExcess = "insert into unscan (ef_moNo,ef_nsjom_jobOrdMasterID,ef_josp_packID,ef_item_internalID,ef_excessQty,ef_refNo,ef_createdDate,ef_rangeTo) values " +
                                                    "('" + q1.jos_moNo + "', '', '','" + ifitemlist.item[i].item.internalId + "','" + excessQty + "','" + refNo + "'," +
                                                    "'" + convertDateToString(DateTime.Now) + "','" + convertDateToString(rangeTo) + "')";
                                                //this.DataFromNetsuiteLog.Debug("SOFulfillmentUpdate: " + insertExcess);
                                                entities.Database.ExecuteSqlCommand(insertExcess);

                                                /*
                                                ExcessFulfillment exFul = new ExcessFulfillment();
                                                exFul.moNo = q1.jos_moNo;
                                                exFul.josp_packID = item.jospPackID;
                                                exFul.jobOrdMasterID = q1.nsjom_jobOrdMaster_ID;
                                                exFul.itemInternalID = ifitemlist.item[i].item.internalId;
                                                //exFul.locationInternalID = item.nsjomp_location_internalID;
                                                exFul.excessQty = ifitemlist.item[i].quantityRemaining - item.fulFillQty;
                                                exFul.refNo = "WMSJOBORDSCANPACK.JOSP_PACK_ID." + josp_packID;
                                                exFulList.Add(exFul);
                                                 * */
                                                //cpng start
                                                InventoryAssignment[] IAA = new InventoryAssignment[1];
                                                InventoryAssignment IA = new InventoryAssignment();
                                                InventoryAssignmentList IAL = new InventoryAssignmentList();
                                                InventoryDetail ID = new InventoryDetail();

                                                IA.quantity = Convert.ToInt32(deCommitQty[j]);
                                                IA.quantitySpecified = true;
                                                IA.binNumber = new RecordRef { internalId = @Resource.TRADE_DEFAULT_BIN_COMMIT };
                                                IAA[0] = IA;
                                                IAL.inventoryAssignment = IAA;
                                                ID.inventoryAssignmentList = IAL;

                                                iffi.inventoryDetail = ID;
                                                //cpng end

                                                deCommitQty[j] = deCommitQty[j] - deCommitQty[j];
                                            }
                                            else
                                            {
                                                iffi.quantity = Convert.ToInt32(ifitemlist.item[i].quantityRemaining);
                                                iffi.quantitySpecified = true;

                                                iffi.itemIsFulfilled = true;
                                                iffi.itemIsFulfilledSpecified = true;

                                                //cpng start
                                                InventoryAssignment[] IAA = new InventoryAssignment[1];
                                                InventoryAssignment IA = new InventoryAssignment();
                                                InventoryAssignmentList IAL = new InventoryAssignmentList();
                                                InventoryDetail ID = new InventoryDetail();

                                                IA.quantity = Convert.ToInt32(ifitemlist.item[i].quantityRemaining);
                                                IA.quantitySpecified = true;
                                                IA.binNumber = new RecordRef { internalId = @Resource.TRADE_DEFAULT_BIN_COMMIT };
                                                IAA[0] = IA;
                                                IAL.inventoryAssignment = IAA;
                                                ID.inventoryAssignmentList = IAL;

                                                iffi.inventoryDetail = ID;
                                                //cpng end

                                                deCommitQty[j] = deCommitQty[j] - Convert.ToInt32(ifitemlist.item[i].quantityRemaining);

                                            }
                                            break;
                                        }
                                    }
                                    ifitems[i] = iffi;
                                }

                                #region Adding Consignment No in Packages - WY-14.JAN.2015
                                ItemFulfillmentPackage[] ifiPackage = new ItemFulfillmentPackage[1];
                                ItemFulfillmentPackage iffp = new ItemFulfillmentPackage();
                                iffp.packageTrackingNumber = consigmentNote;
                                iffp.packageWeight = 0.1;//Default to 0.1 KGS
                                iffp.packageWeightSpecified = true;

                                ItemFulfillmentPackageList iffpl = new ItemFulfillmentPackageList();
                                iffpl.replaceAll = false;
                                ifiPackage[0] = iffp;
                                iffpl.package = ifiPackage;
                                iff2.packageList = iffpl;
                                #endregion

                                ItemFulfillmentItemList ifil1 = new ItemFulfillmentItemList();
                                ifil1.replaceAll = false; //WY-10.NOV.2014
                                ifil1.item = ifitems;
                                iff2.itemList = ifil1;

                                iffList[ordCount] = iff2;

                                rowCount = ordCount + 1;
                                //refNo = "WMSJOBORDSCANPACK.JOSP_PACK_ID." + josp_packID + "." + convertDateToString(rangeFrom) + "-" + convertDateToString(rangeTo);
                                //Change to filter by mono only - WY-29.SEPT.2014
                                refNo = "WMSJOBORDSCANPACK.MONO." + q1.jos_moNo + "." + convertDateToString(rangeFrom) + "-" + convertDateToString(rangeTo);

                                //var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                //    "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'SSA-FULFILLMENT', '" + refNo + "', '" + gjob_id.ToString() + "'," +
                                //    "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','" + q1.nsjm_moNo_internalID + "')";

                                ////this.DataFromNetsuiteLog.Debug("SOFulfillmentUpdate: " + insertTask);
                                //entities.Database.ExecuteSqlCommand(insertTask);

                                ////var updateTask = "update wms_jobordscan set jos_netsuiteProgress = '" + gjob_id.ToString() + "' where jos_netsuiteProgress is null " +
                                ////                    "and jos_job_ID = '" + q1.jos_job_ID + "' " +
                                ////                    "and jos_moNo = '" + q1.jos_moNo + "' " +
                                ////                    "and jos_rangeTo > '" + convertDateToString(rangeFrom) + "' " +
                                ////                    "and jos_rangeTo <= '" + convertDateToString(rangeTo) + "'";

                                ////Change to filter by mono only - WY-29.SEPT.2014
                                //var updateTask = "update wms_jobordscan set jos_netsuiteProgress = '" + gjob_id.ToString() + "' where jos_netsuiteProgress is null " +
                                //                 "and jos_moNo = '" + q1.jos_moNo + "' " +
                                //                 "and jos_rangeTo > '" + convertDateToString(rangeFrom) + "' " +
                                //                 "and jos_rangeTo <= '" + convertDateToString(rangeTo) + "'";

                                ////this.DataFromNetsuiteLog.Debug("SOFulfillmentUpdate: " + updateTask);
                                //entities.Database.ExecuteSqlCommand(updateTask);

                                #region Compare NS and DB Items - WY-26.Dec.2014
                                //this.DataFromNetsuiteLog.Debug("SOFulfillmentUpdate: Start Compare NS and DB Items");
                                foreach (DictionaryEntry entry in htDBItems)
                                {
                                    if (Convert.ToInt32(entry.Value) > 0)
                                    {
                                        var insertItem = "insert into unfulfillso (uf_transactiontype,uf_mono,uf_itemInternalID,uf_fulfillQty,uf_rangeFrom,uf_rangeTo,uf_createdDate) " +
                                                         " values ('SSA-FULFILLMENT','" + q1.jos_moNo + "', '" + entry.Key.ToString() + "','" + entry.Value + "', " +
                                                         " '" + convertDateToString(rangeFrom) + "','" + convertDateToString(rangeTo) + "','" + convertDateToString(DateTime.Now) + "') ";
                                        //this.DataFromNetsuiteLog.Debug("SOFulfillmentUpdate: " + insertItem);
                                        entities.Database.ExecuteSqlCommand(insertItem);
                                    }
                                }
                                //this.DataFromNetsuiteLog.Debug("SOFulfillmentUpdate: End Compare NS and DB Items");
                                #endregion

                                ordCount++;
                                status = true;

                                //this.DataFromNetsuiteLog.Debug("SOFulfillmentUpdate: Sales order internalID_moNo: " + q1.nsjm_moNo_internalID + "_" + q1.jos_moNo);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        //this.DataFromNetsuiteLog.Error("SOFulfillmentUpdate Exception: " + ex.ToString());
                        status = false;
                        if (rowCount == 0)
                        {
                            rowCount++;
                        }
                        break;
                    }
                }//end of ordMaster

                if (status == true)
                {
                    if (iffList != null && iffList.Count() > 0)
                    {
                        WriteResponseList resList = service.addList(iffList);
                        WriteResponse[] res = resList.writeResponse;
                        foreach (WriteResponse result in res)
                        {
                            if (result.status.isSuccess == false)
                            {
                                status = false;
                                if (result.status.statusDetail != null)
                                {
                                    for (int j = 0; j < result.status.statusDetail.Count(); j++)
                                    {
                                        errorMsg += "" + (j + 1) + ". " + result.status.statusDetail[j].code + ": " + result.status.statusDetail[j].message + "";
                                    }
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                errorMsg = ex.ToString();
                status = false;
            }
            /////

            if (status == false)
            {
                return errorMsg;
            }
            else
            {
                return "";
            }
        }
        public string rerun_trade_purchase_order(NetSuiteService service, sdeEntities entities, string refNo, string refRange)
        {
            DateTime rangeFrom = DateTime.Parse(refRange.Substring(0, 19));
            DateTime rangeTo = DateTime.Parse(refRange.Substring(20, 19));

            Boolean status = false;
            String errorMsg = "";

            /////
            //Int32 daCount = 0;
            //Int32 itemCount = 0;
            Int32 rowCount = 0;

            try
            {
                Int32 poCount = 0;
                Guid gjob_id = Guid.NewGuid();

                var por = (from po in entities.wms_poreceive
                           join pr in entities.netsuite_pr
                           on po.po_pr_ID equals pr.nspr_pr_ID
                           where po.po_poreceive_ID == refNo
                           select new { pr.nspr_pr_ID, pr.nspr_pr_internalID, po.po_poreceive_ID, pr.nspr_pr_location_internalID }).ToList();

                //status = true;
                ItemReceipt[] irList = new ItemReceipt[por.Count()];

                foreach (var p in por)
                {
                    try
                    {
                        InitializeRef refPO = new InitializeRef();
                        refPO.type = InitializeRefType.purchaseOrder;
                        refPO.internalId = p.nspr_pr_internalID;
                        refPO.typeSpecified = true;

                        InitializeRecord recPO = new InitializeRecord();
                        recPO.type = InitializeType.itemReceipt;
                        recPO.reference = refPO;

                        ReadResponse rrPO = service.initialize(recPO);
                        Record rPO = rrPO.record;

                        ItemReceipt ir1 = (ItemReceipt)rPO;
                        ItemReceipt ir2 = new ItemReceipt();

                        if (ir1 != null)
                        {
                            RecordRef refCreatedFrom = new RecordRef();
                            refCreatedFrom.internalId = ir1.createdFrom.internalId;
                            ir2.createdFrom = refCreatedFrom;

                            ItemReceiptItemList iril1 = new ItemReceiptItemList();
                            iril1 = ir1.itemList;

                            for (int i = 0; i < iril1.item.Count(); i++)
                            {

                                String itemID = iril1.item[i].item.name;
                                var poi = (from j in entities.wms_poreceiveitem
                                           where j.poi_poreceive_ID == p.po_poreceive_ID
                                           && j.poi_item_ID == itemID
                                           select j).ToList();

                                if (poi.Count() > 0)
                                {
                                    Double receiveQty = 0;
                                    String poiItemID = null;
                                    foreach (var item in poi)
                                    {
                                        receiveQty += Convert.ToDouble(item.poi_poreceiveItem_qty);
                                        poiItemID = item.poi_item_ID;
                                    }

                                    if (iril1.item[i].quantityRemaining >= receiveQty)
                                    {
                                        iril1.item[i].quantity = receiveQty;
                                        iril1.item[i].itemReceive = true;
                                        iril1.item[i].itemReceiveSpecified = true;
                                    }
                                    else if (iril1.item[i].quantityRemaining < receiveQty)
                                    {
                                        Double excessQty = receiveQty - iril1.item[i].quantityRemaining;
                                        iril1.item[i].item.name = poiItemID;
                                        iril1.item[i].quantity = iril1.item[i].quantityRemaining;
                                        iril1.item[i].itemReceive = true;
                                        iril1.item[i].itemReceiveSpecified = true;

                                        var insertExcessPO = "insert into excesspo (ep_priD,ep_poreceiveID,ep_poreceiveItemID,ep_itemInternalID,ep_excessQty,ep_createdDate,ep_rangeTo) " +
                                            "values ('" + p.nspr_pr_ID + "','" + p.po_poreceive_ID + "','" + poiItemID + "','" + iril1.item[i].item.internalId + "'," +
                                            "'" + excessQty + "','" + convertDateToString(DateTime.Now) + "','" + convertDateToString(rangeTo) + "')";

                                        entities.Database.ExecuteSqlCommand(insertExcessPO);
                                    }
                                }
                                else
                                {
                                    iril1.item[i].quantity = 0;
                                    iril1.item[i].itemReceive = false;
                                    iril1.item[i].itemReceiveSpecified = true;
                                }

                                //RecordRef refLocation = new RecordRef();
                                //refLocation.internalId = p.nspr_pr_location_internalID;
                                //iril1.item[i].location = refLocation;
                            }

                            ir2.itemList = iril1;
                            irList[poCount] = ir2;

                            rowCount = poCount + 1;
                            poCount++;
                            status = true;
                        }
                    }
                    catch (Exception ex)
                    {
                        status = false;
                        if (rowCount == 0)
                        {
                            rowCount++;
                        }
                        break;
                    }

                }//end of por

                if (status == true)
                {
                    if (irList != null && irList.Count() > 0)
                    {
                        WriteResponseList resList = service.addList(irList);
                        WriteResponse[] res = resList.writeResponse; 
                        foreach (WriteResponse result in res)
                        {
                            if (result.status.isSuccess == false)
                            {
                                status = false;
                                if (result.status.statusDetail != null)
                                {
                                    for (int j = 0; j < result.status.statusDetail.Count(); j++)
                                    {
                                        errorMsg += "" + (j + 1) + ". " + result.status.statusDetail[j].code + ": " + result.status.statusDetail[j].message + "";
                                    }
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                errorMsg = ex.ToString();
                status = false;
            }
            /////

            if (status == false)
            {
                return errorMsg;
            }
            else
            {
                return "";
            }
        }
        #endregion
        #region BCAS-ReRun
        public string rerun_bcas_order_adjustment(NetSuiteService service, sdeEntities entities, string refNo, string refRange, string refType)
        {
            DateTime rangeFrom = DateTime.Parse(refRange.Substring(0, 19));
            DateTime rangeTo = DateTime.Parse(refRange.Substring(20, 19));

            Boolean status = false;
            String errorMsg = "";

            ////
            Int32 daCount = 0;
            Int32 rowCount = 0;
            InventoryAdjustment invAdj = new InventoryAdjustment();

            try
            {
                var query1 = (from q1 in entities.wms_jobordscan
                              where q1.jos_businessChannel_code == "BC"
                              && (q1.jos_rangeTo > rangeFrom && q1.jos_rangeTo <= rangeTo)
                              && q1.jos_job_ID == refNo
                              && q1.jos_moNo.Substring(0, 1) == refType
                              select new
                              {
                                  q1.jos_job_ID,
                                  q1.jos_businessChannel_code,
                                  q1.jos_country_tag,
                                  q1.jos_rangeTo,
                                  mono = q1.jos_moNo.Substring(0, 1)
                              })
                          .Distinct()
                          .OrderBy(x => x.jos_businessChannel_code)
                          .ThenBy(y => y.jos_country_tag)
                          .ToList();

                InventoryAdjustment[] invAdjList = new InventoryAdjustment[query1.Count()];

                foreach (var q1 in query1)
                {
                    RecordRef refAccount = new RecordRef();
                    refAccount.internalId = @Resource.ADJUSTMENT_ACCOUNT_WRITEOFFDMG;
                    invAdj.account = refAccount;

                    RecordRef refCustomer = new RecordRef();
                    if (q1.jos_country_tag.Equals("ID"))
                    {
                        refCustomer.internalId = @Resource.BCAS_CUSTOMER_ID;
                        invAdj.customer = refCustomer;
                    }
                    else if (q1.jos_country_tag.Equals("MY"))
                    {
                        refCustomer.internalId = @Resource.BCAS_CUSTOMER_MY;
                        invAdj.customer = refCustomer;
                    }
                    else if (q1.jos_country_tag.Equals("SG"))
                    {
                        refCustomer.internalId = @Resource.BCAS_CUSTOMER_SG;
                        invAdj.customer = refCustomer;
                    }

                    RecordRef refSubsidiary = new RecordRef();
                    refSubsidiary.internalId = @Resource.BCAS_DUMMYSALES_MY;
                    invAdj.subsidiary = refSubsidiary;

                    RecordRef refBusinessChannel = new RecordRef();
                    refBusinessChannel.internalId = @Resource.BCAS_DUMMYSALES_BUSINESSCHANNEL;
                    invAdj.@class = refBusinessChannel;

                    RecordRef refCostCenter = new RecordRef();
                    refCostCenter.internalId = @Resource.COSTCENTER_SALESANDMARKETING;
                    invAdj.department = refCostCenter;

                    invAdj.tranDate = DateTime.Now;

                    if (q1.mono.Equals("E"))
                    {
                        invAdj.memo = "BCAS REPLACEMENT SALES";
                    }
                    else
                    {
                        invAdj.memo = "BCAS SALES";
                    }

                    CustomFieldRef[] cfrList = new CustomFieldRef[1];
                    StringCustomFieldRef scfr = new StringCustomFieldRef();
                    scfr.scriptId = @Resource.CUSTOMFIELD_INVADJ_ADJTYPE_SCRIPTID;
                    scfr.internalId = @Resource.CUSTOMFIELD_INVADJ_ADJTYPE_INTERNALID;
                    scfr.value = @Resource.CUSTOMVALUE_INVADJ_ADJTYPE_PHYSICALCOUNT;
                    cfrList[0] = scfr;
                    invAdj.customFieldList = cfrList;

                    #region Other Sales
                    var query3 = (from josp in entities.wms_jobordscan_pack
                                  join jomp in entities.netsuite_jobordmaster_pack on josp.josp_pack_ID equals jomp.nsjomp_jobOrdMaster_pack_ID
                                  join jompd in entities.netsuite_jobordmaster_packdetail on jomp.nsjomp_jobOrdMaster_pack_ID equals jompd.nsjompd_jobOrdMaster_pack_ID
                                  where josp.josp_jobID == q1.jos_job_ID && josp.josp_ordFulFill > 0
                                  && josp.josp_moNo.StartsWith(q1.mono) && josp.josp_rangeTo > rangeFrom && josp.josp_rangeTo <= rangeTo
                                  select new
                                  {
                                      jompd.nsjompd_item_internalID,
                                      jomp.nsjomp_ordQty,
                                      jomp.nsjomp_ordFulfill,
                                      totalQty = (jomp.nsjomp_ordQty * jompd.nsjompd_sku_qty),
                                      fullQty = (josp.josp_ordFulFill * jompd.nsjompd_sku_qty)
                                  }).ToList();

                    var groupQ3 = from p in query3
                                  let k = new
                                  {
                                      itemInternalID = p.nsjompd_item_internalID,
                                      totalQty = p.totalQty,
                                      fulFillQty = p.fullQty
                                  }
                                  group p by k into g
                                  where (g.Sum(p => p.fullQty) - g.Sum(p => p.totalQty)) < 0
                                  select new
                                  {
                                      item = g.Key.itemInternalID,
                                      adjQty = g.Sum(p => p.fullQty) - g.Sum(p => p.totalQty)
                                  };

                    if (groupQ3.Count() > 0)
                    {
                        InventoryAdjustmentInventory[] items = new InventoryAdjustmentInventory[groupQ3.Count()];
                        InventoryAdjustmentInventoryList iail = new InventoryAdjustmentInventoryList();
                        Int32 itemCount = 0;

                        foreach (var i in groupQ3)
                        {
                            InventoryAdjustmentInventory item = new InventoryAdjustmentInventory();

                            RecordRef refItem = new RecordRef();
                            refItem.internalId = i.item;
                            item.item = refItem;

                            RecordRef refLocation = new RecordRef();
                            refLocation.internalId = @Resource.BCAS_DUMMYSALES_LOCATION;
                            item.location = refLocation;

                            item.adjustQtyBy = Convert.ToInt32(i.adjQty);
                            item.adjustQtyBySpecified = true;

                            items[itemCount] = item;
                            itemCount++;
                        }
                        iail.inventory = items;
                        invAdj.inventoryList = iail;
                        invAdjList[daCount] = invAdj;

                        rowCount = daCount + 1;
                        status = true;
                        daCount++;
                    }
                    #endregion
                }

                if (status == true)
                {
                    WriteResponseList resList = service.addList(invAdjList);
                    WriteResponse[] res = resList.writeResponse; 
                    foreach (WriteResponse result in res)
                    {
                        if (result.status.isSuccess == false)
                        {
                            status = false;
                            if (result.status.statusDetail != null)
                            {
                                for (int j = 0; j < result.status.statusDetail.Count(); j++)
                                {
                                    errorMsg += "" + (j + 1) + ". " + result.status.statusDetail[j].code + ": " + result.status.statusDetail[j].message + "";
                                }
                            }
                        }
                    }
                }

            }
            catch (Exception ex)
            {
                errorMsg = ex.ToString();
                status = false;
            }
            ////

            if (status == false)
            {
                return errorMsg;
            }
            else
            {
                return "";
            }
        }
        public string rerun_bcas_journal(NetSuiteService service, sdeEntities entities, string refNo, string refRange, string refType)
        {
            DateTime rangeFrom = DateTime.Parse(refRange.Substring(0, 19));
            DateTime rangeTo = DateTime.Parse(refRange.Substring(20, 19));

            Boolean status = false;
            String errorMsg = "";

            ////
            Int32 jnCount = 0;
            Int32 rowCount = 0;

            try
            {
                var journalGroup = (from q1 in entities.wms_jobordscan
                                    join q2 in entities.map_country on q1.jos_country_tag equals q2.mc_countryCode
                                    join q3 in entities.map_currency on q1.jos_country_tag equals q3.mc_country
                                    where q1.jos_businessChannel_code == "BC" 
                                    && (q1.jos_rangeTo > rangeFrom && q1.jos_rangeTo <= rangeTo)
                                    && q1.jos_job_ID == refNo
                                    && q1.jos_moNo.Substring(0, 1) == refType
                                    //&& !q1.jos_moNo.Contains("E")
                                    select new
                                    {
                                        q1.jos_job_ID,
                                        q1.jos_businessChannel_code,
                                        q1.jos_country_tag,
                                        q2.mc_country_internalID,
                                        q3.mc_currency_internalID,
                                        q1.jos_rangeTo,
                                        mono = q1.jos_moNo.Substring(0, 1)
                                    })
                      .Distinct()
                      .OrderBy(x => x.jos_businessChannel_code)
                      .ThenBy(y => y.jos_country_tag)
                      .ToList();

                JournalEntry[] jeList = new JournalEntry[journalGroup.Count()];
                foreach (var j in journalGroup)
                {
                    try
                    {
                        JournalEntry je = new JournalEntry();
                        JournalEntryLineList jell = new JournalEntryLineList();

                        je.tranDate = DateTime.Now;
                        je.tranDateSpecified = true;

                        RecordRef refSub = new RecordRef();
                        refSub.internalId = @Resource.BCAS_DUMMYSALES_MY;//j.subsidiary;
                        je.subsidiary = refSub;

                        RecordRef refCurrency = new RecordRef();
                        refCurrency.internalId = j.mc_currency_internalID;
                        je.currency = refCurrency;

                        CustomFieldRef[] cfrList1 = new CustomFieldRef[1];
                        StringCustomFieldRef scfr1 = new StringCustomFieldRef();
                        scfr1.scriptId = @Resource.CUSTOMFIELD_REMARKS_SCRIPTID;
                        scfr1.internalId = @Resource.CUSTOMFIELD_REMARKS_INTERNALID;
                        if (j.mono.Equals("E"))
                        {
                            scfr1.value = "BCAS REPLACEMENT SALES";
                        }
                        else
                        {
                            scfr1.value = "BCAS SALES";
                        }
                        cfrList1[0] = scfr1;

                        #region ID Sales
                        /*
                                if (j.jos_country_tag.Equals("ID"))
                                {
                                    var query2 = (from jmp in entities.netsuite_jobmo_pack
                                                  where jmp.nsjmp_nsj_jobID == j.jos_job_ID && jmp.nsjmp_moNo.StartsWith(j.mono)
                                                  select new { jmp.nsjmp_nsj_jobID, jmp.nsjmp_amt }).ToList();

                                    var journalLine = from p in query2
                                                      let k = new
                                                      {
                                                          jobID = p.nsjmp_nsj_jobID
                                                      }
                                                      group p by k into g
                                                      select new
                                                      {
                                                          amount = g.Sum(p => p.nsjmp_amt)
                                                      };

                                    var journalAcc = (from q1 in entities.map_chartofaccount
                                                      where q1.coa_tranType == "BCAS JOURNAL"
                                                      select q1).ToList();

                                    if (journalLine.Count() > 0)
                                    {
                                        String amount = "";
                                        foreach (var jl in journalLine)
                                        {
                                            amount = jl.amount.ToString();
                                        }

                                        JournalEntryLine[] lines = new JournalEntryLine[2];

                                        for (int i = 0; i < lines.Count(); i++)
                                        {
                                            JournalEntryLine line = new JournalEntryLine();

                                            RecordRef refBusinessChannel = new RecordRef();
                                            refBusinessChannel.internalId = @Resource.BCAS_DUMMYSALES_BUSINESSCHANNEL;
                                            line.@class = refBusinessChannel;

                                            CustomFieldRef[] cfrList = new CustomFieldRef[1];
                                            StringCustomFieldRef scfr = new StringCustomFieldRef();
                                            scfr.scriptId = @Resource.CUSTOMFIELD_SCHO_SALESTERRITORY_SCRIPTID;
                                            scfr.internalId = @Resource.CUSTOMFIELD_SCHO_SALESTERRITORY_INTERNALID;
                                            scfr.value = j.mc_country_internalID;
                                            cfrList[0] = scfr;

                                            line.customFieldList = cfrList;

                                            if (j.mono.Equals("E"))
                                            {
                                                line.memo = "BCAS REPLACEMENT SALES";
                                            }
                                            else
                                            {
                                                line.memo = "BCAS SALES";
                                            }

                                            if (i == 0)
                                            {
                                                if (journalAcc[i].coa_glType.Equals("DEBIT"))
                                                {
                                                    RecordRef refDebit = new RecordRef();
                                                    refDebit.internalId = journalAcc[i].coa_account_internalID;
                                                    line.account = refDebit;

                                                    line.debit = Convert.ToDouble(amount);
                                                    line.debitSpecified = true;
                                                }
                                            }
                                            else if (i == 1)
                                            {
                                                if (journalAcc[i].coa_glType.Equals("CREDIT"))
                                                {
                                                    RecordRef refCredit = new RecordRef();
                                                    refCredit.internalId = journalAcc[i].coa_account_internalID;
                                                    line.account = refCredit;

                                                    line.credit = Convert.ToDouble(amount);
                                                    line.creditSpecified = true;
                                                }
                                            }
                                            lines[i] = line;
                                        }
                                        jell.line = lines;
                                        je.lineList = jell;
                                        jeList[jnCount] = je;

                                        rowCount = jnCount + 1;
                                        String refNo = "JOBORDSCAN.JOS_JOB_ID." + j.jos_job_ID + "." + convertDateToString(rangeFrom) + "-" + convertDateToString(rangeTo) + "." + j.mono;
                                        var insertTask = "insert into requestnetsuite_task (rnt_task, rnt_description, rnt_refNO, rnt_jobID, rnt_status, rnt_createdDate, " +
                                            "rnt_seqNO,rnt_createdFromInternalID) values ('ADD', 'BCAS-JOURNAL', '" + refNo + "', '" + gjob_id.ToString() + "'," +
                                            "'START', '" + convertDateToString(DateTime.Now) + "', '" + rowCount + "','')";
                                        this.DataFromNetsuiteLog.Debug("BCASJournal: " + insertTask);
                                        entities.Database.ExecuteSqlCommand(insertTask);

                                        jnCount++;
                                        status = true;
                                    }
                                }
                                 * */
                        #endregion
                        #region Other Sales
                        //else
                        //{
                        /*
                        var query2 = (from jomp in entities.netsuite_jobordmaster_pack
                                      join josp in entities.wms_jobordscan_pack on jomp.nsjomp_jobOrdMaster_pack_ID equals josp.josp_pack_ID
                                      join jmp in entities.netsuite_jobmo_pack on jomp.nsjomp_ordPack equals jmp.nsjmp_packID
                                      where jomp.nsjomp_job_ID == j.jos_job_ID && josp.josp_ordFulFill > 0 //&& jompd.nsjompd_item_internalID != null
                                      && jomp.nsjomp_moNo.StartsWith(j.mono)
                                      select new { jomp.nsjomp_job_ID, amt = (josp.josp_ordFulFill * jmp.nsjmp_packPrice) }).ToList();
                        */
                        var query2 = (from josp in entities.wms_jobordscan_pack
                                      join jomp in entities.netsuite_jobordmaster_pack on josp.josp_pack_ID equals jomp.nsjomp_jobOrdMaster_pack_ID
                                      join jmp in entities.netsuite_jobmo_pack on jomp.nsjomp_ordPack equals jmp.nsjmp_packID
                                      //join jompd in entities.netsuite_jobordmaster_packdetail on jomp.nsjomp_jobOrdMaster_pack_ID equals jompd.nsjompd_jobOrdMaster_pack_ID
                                      where josp.josp_jobID == j.jos_job_ID && josp.josp_ordFulFill > 0
                                      && josp.josp_moNo.StartsWith(j.mono) && josp.josp_rangeTo > rangeFrom && josp.josp_rangeTo <= rangeTo
                                      select new { jomp.nsjomp_job_ID, amt = (josp.josp_ordFulFill * jmp.nsjmp_packPrice) }).ToList();

                        var groupQ2 = from p in query2
                                      let k = new
                                      {
                                          jobID = p.nsjomp_job_ID
                                      }
                                      group p by k into g
                                      select new
                                      {
                                          amount = g.Sum(p => p.amt)
                                      };

                        var journalAcc = (from q1 in entities.map_chartofaccount
                                          where q1.coa_tranType == "BCAS JOURNAL"
                                          select q1).ToList();

                        if (groupQ2.Count() > 0)
                        {
                            String amount = "";
                            foreach (var jl in groupQ2)
                            {
                                amount = jl.amount.ToString();
                            }

                            JournalEntryLine[] lines = new JournalEntryLine[2];

                            for (int i = 0; i < lines.Count(); i++)
                            {
                                JournalEntryLine line = new JournalEntryLine();

                                RecordRef refBusinessChannel = new RecordRef();
                                refBusinessChannel.internalId = @Resource.BCAS_DUMMYSALES_BUSINESSCHANNEL;
                                line.@class = refBusinessChannel;

                                CustomFieldRef[] cfrList = new CustomFieldRef[1];
                                StringCustomFieldRef scfr = new StringCustomFieldRef();
                                scfr.scriptId = @Resource.CUSTOMFIELD_SCHO_SALESTERRITORY_SCRIPTID;
                                scfr.internalId = @Resource.CUSTOMFIELD_SCHO_SALESTERRITORY_INTERNALID;
                                scfr.value = j.mc_country_internalID;
                                cfrList[0] = scfr;

                                line.customFieldList = cfrList;

                                if (j.mono.Equals("E"))
                                {
                                    line.memo = "BCAS REPLACEMENT SALES";
                                }
                                else
                                {
                                    line.memo = "BCAS SALES";
                                }


                                if (i == 0)
                                {
                                    if (journalAcc[i].coa_glType.Equals("DEBIT"))
                                    {
                                        RecordRef refDebit = new RecordRef();
                                        refDebit.internalId = journalAcc[i].coa_account_internalID;
                                        line.account = refDebit;

                                        line.debit = Convert.ToDouble(amount);
                                        line.debitSpecified = true;
                                    }
                                }
                                else if (i == 1)
                                {
                                    if (journalAcc[i].coa_glType.Equals("CREDIT"))
                                    {
                                        RecordRef refCredit = new RecordRef();
                                        refCredit.internalId = journalAcc[i].coa_account_internalID;
                                        line.account = refCredit;

                                        line.credit = Convert.ToDouble(amount);
                                        line.creditSpecified = true;
                                    }
                                }
                                lines[i] = line;
                            }
                            jell.line = lines;
                            je.lineList = jell;
                            jeList[jnCount] = je;

                            rowCount = jnCount + 1;
                            jnCount++;
                        }
                        //}
                        #endregion

                        status = true;
                    }
                    catch (Exception ex)
                    {
                        status = false;
                        if (rowCount == 0)
                        {
                            rowCount++;
                        }
                        break;
                    }
                }//end of journal

                if (status == true)
                {
                    WriteResponseList resList = service.addList(jeList);
                    WriteResponse[] res = resList.writeResponse;
                    foreach (WriteResponse result in res)
                    {
                        if (result.status.isSuccess == false)
                        {
                            status = false;
                            if (result.status.statusDetail != null)
                            {
                                for (int j = 0; j < result.status.statusDetail.Count(); j++)
                                {
                                    errorMsg += "" + (j + 1) + ". " + result.status.statusDetail[j].code + ": " + result.status.statusDetail[j].message + "";
                                }
                            }
                        }
                    }
                }

            }
            catch (Exception ex)
            {
                errorMsg = ex.ToString();
                status = false;
            }
            ////

            if (status == false)
            {
                return errorMsg;
            }
            else
            {
                return "";
            }
        }
        public string rerun_bcas_sales_order(NetSuiteService service, sdeEntities entities, string refNo, string refRange, string refType)
        {
            DateTime rangeFrom = DateTime.Parse(refRange.Substring(0, 19));
            DateTime rangeTo = DateTime.Parse(refRange.Substring(20, 19));

            Boolean status = false;
            String errorMsg = "";

            ////
            Int32 soCount = 0;
            Int32 rowCount = 0;

            try
            {
                var IDquery = (from q1 in entities.wms_jobordscan
                               where q1.jos_businessChannel_code == "BC"
                               && (q1.jos_rangeTo > rangeFrom && q1.jos_rangeTo <= rangeTo)
                               && q1.jos_country_tag == "ID"
                               select new { q1.jos_job_ID, q1.jos_moNo })
                              .Distinct()
                              .ToList();

                List<string> _IDjob = new List<string>();
                foreach (var q1 in IDquery)
                {
                    _IDjob.Add(q1.jos_job_ID + "-" + q1.jos_moNo);
                }

                var query1 = (from q1 in entities.wms_jobordscan
                              where q1.jos_businessChannel_code == "BC"
                              && (q1.jos_rangeTo > rangeFrom && q1.jos_rangeTo <= rangeTo)
                              && q1.jos_job_ID == refNo
                              && q1.jos_moNo.Substring(0, 1) == refType
                              select new
                              {
                                  q1.jos_job_ID,
                                  q1.jos_businessChannel_code,
                                  q1.jos_country_tag,
                                  q1.jos_rangeTo,
                                  mono = q1.jos_moNo.Substring(0, 1)
                              })
                                    .Distinct()
                                    .OrderBy(x => x.jos_businessChannel_code)
                                    .ThenBy(y => y.jos_country_tag)
                                    .ToList();

                SalesOrder[] soList = new SalesOrder[query1.Count()];
                foreach (var q1 in query1)
                {
                    try
                    {
                        SalesOrder so = new SalesOrder();

                        RecordRef refForm = new RecordRef();
                        refForm.internalId = @Resource.BCAS_SALES_CUSTOMFORM_MY;
                        so.customForm = refForm;

                        so.tranDate = DateTime.Now;
                        so.tranDateSpecified = true;

                        so.orderStatus = SalesOrderOrderStatus._pendingFulfillment;
                        so.orderStatusSpecified = true;

                        RecordRef refClass = new RecordRef();
                        refClass.internalId = @Resource.BCAS_DUMMYSALES_BUSINESSCHANNEL;
                        so.@class = refClass;

                        RecordRef refLocation = new RecordRef();
                        refLocation.internalId = @Resource.BCAS_DUMMYSALES_LOCATION;
                        so.location = refLocation;

                        CustomFieldRef[] cfrList = new CustomFieldRef[1];
                        StringCustomFieldRef scfr = new StringCustomFieldRef();
                        scfr.scriptId = @Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                        scfr.internalId = @Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                        scfr.value = "2";
                        cfrList[0] = scfr;

                        so.customFieldList = cfrList;

                        if (q1.mono.Equals("E"))
                        {
                            so.memo = "BCAS REPLACEMENT SALES";
                        }
                        else
                        {
                            so.memo = "BCAS SALES";
                        }

                        RecordRef refEntity = new RecordRef();
                        if (q1.jos_country_tag.Equals("ID"))
                        {
                            refEntity.internalId = @Resource.BCAS_CUSTOMER_ID;
                            so.entity = refEntity;
                        }
                        else if (q1.jos_country_tag.Equals("MY"))
                        {
                            refEntity.internalId = @Resource.BCAS_CUSTOMER_MY;
                            so.entity = refEntity;
                        }
                        else if (q1.jos_country_tag.Equals("SG"))
                        {
                            refEntity.internalId = @Resource.BCAS_CUSTOMER_SG;
                            so.entity = refEntity;
                        }

                        //RecordRef refShipAddr = new RecordRef();
                        //refShipAddr.internalId = "1";
                        //so.shipAddressList = refShipAddr;

                        #region ID Sales
                        if (q1.jos_country_tag.Equals("ID"))
                        {
                            var query3 = (from ji in entities.netsuite_jobitem
                                          where ji.nsji_nsj_jobID == q1.jos_job_ID
                                          && _IDjob.Contains(ji.nsji_nsj_jobID + "-" + ji.nsji_moNo)
                                          select ji).ToList();

                            var groupQ3 = from p in query3
                                          let k = new
                                          {
                                              itemInternalID = p.nsji_item_internalID,
                                              fulFillQty = p.nsji_item_qty
                                          }
                                          group p by k into g
                                          select new
                                          {
                                              item = g.Key.itemInternalID,
                                              fulFillQty = g.Sum(p => p.nsji_item_qty)
                                          };

                            if (groupQ3.Count() > 0)
                            {
                                SalesOrderItem[] soii = new SalesOrderItem[groupQ3.Count()];
                                SalesOrderItemList soil = new SalesOrderItemList();
                                Int32 itemCount = 0;

                                foreach (var item in groupQ3)
                                {
                                    SalesOrderItem soi = new SalesOrderItem();

                                    RecordRef refItem = new RecordRef();
                                    refItem.type = RecordType.inventoryItem;
                                    refItem.typeSpecified = true;
                                    refItem.internalId = item.item;
                                    soi.item = refItem;

                                    soi.quantity = Convert.ToDouble(item.fulFillQty);
                                    soi.quantitySpecified = true;

                                    soi.amount = 0;
                                    soi.amountSpecified = true;

                                    soii[itemCount] = soi;
                                    itemCount++;
                                }
                                soil.item = soii;
                                so.itemList = soil;
                                soList[soCount] = so;

                                rowCount = soCount + 1;
                                soCount++;
                                status = true;
                            }
                        }
                        #endregion
                        #region Other Sales
                        else
                        {
                            var query2 = (from josp in entities.wms_jobordscan_pack
                                          join jomp in entities.netsuite_jobordmaster_pack on josp.josp_pack_ID equals jomp.nsjomp_jobOrdMaster_pack_ID
                                          join jompd in entities.netsuite_jobordmaster_packdetail on jomp.nsjomp_jobOrdMaster_pack_ID equals jompd.nsjompd_jobOrdMaster_pack_ID
                                          where josp.josp_jobID == q1.jos_job_ID && josp.josp_ordFulFill > 0
                                          && josp.josp_moNo.StartsWith(q1.mono)
                                          && josp.josp_rangeTo > rangeFrom
                                          && josp.josp_rangeTo <= rangeTo
                                          select new { jompd.nsjompd_item_internalID, qty = (josp.josp_ordFulFill * jompd.nsjompd_sku_qty) }).ToList();

                            var groupQ2 = from p in query2
                                          let k = new
                                          {
                                              itemInternalID = p.nsjompd_item_internalID,
                                              fulFillQty = p.qty
                                          }
                                          group p by k into g
                                          select new
                                          {
                                              item = g.Key.itemInternalID,
                                              fulFillQty = g.Sum(p => p.qty)
                                          };

                            if (groupQ2.Count() > 0)
                            {
                                SalesOrderItem[] soii = new SalesOrderItem[groupQ2.Count()];
                                SalesOrderItemList soil = new SalesOrderItemList();
                                Int32 itemCount = 0;

                                foreach (var item in groupQ2)
                                {
                                    SalesOrderItem soi = new SalesOrderItem();

                                    RecordRef refItem = new RecordRef();
                                    refItem.type = RecordType.inventoryItem;
                                    refItem.typeSpecified = true;
                                    refItem.internalId = item.item;
                                    soi.item = refItem;

                                    soi.quantity = Convert.ToDouble(item.fulFillQty);
                                    soi.quantitySpecified = true;

                                    soi.amount = 0;
                                    soi.amountSpecified = true;

                                    soii[itemCount] = soi;
                                    itemCount++;
                                }
                                soil.item = soii;
                                so.itemList = soil;
                                soList[soCount] = so;

                                rowCount = soCount + 1;
                                soCount++;
                                status = true;
                            }
                        }
                        #endregion
                    }
                    catch (Exception ex)
                    {
                        status = false;
                        if (rowCount == 0)
                        {
                            rowCount++;
                        }
                        break;
                    }
                }//end of bcas SO

                if (status == true)
                {
                    WriteResponseList resList = service.addList(soList);
                    WriteResponse[] res = resList.writeResponse; 
                    foreach (WriteResponse result in res)
                    {
                        if (result.status.isSuccess == false)
                        {
                            status = false;
                            if (result.status.statusDetail != null)
                            {
                                for (int j = 0; j < result.status.statusDetail.Count(); j++)
                                {
                                    errorMsg += "" + (j + 1) + ". " + result.status.statusDetail[j].code + ": " + result.status.statusDetail[j].message + "";
                                }
                            }
                        }
                    }
                }

            }
            catch (Exception ex)
            {
                errorMsg = ex.ToString();
                status = false;
            }
            ////

            if (status == false)
            {
                return errorMsg;
            }
            else
            {
                return "";
            }
        }
        public string rerun_bcas_sales_order_fulfill(NetSuiteService service, sdeEntities entities, string refNo)
        {
            //DateTime rangeFrom = DateTime.Parse(refRange.Substring(0, 19));
            //DateTime rangeTo = DateTime.Parse(refRange.Substring(20, 19));

            Boolean status = false;
            String errorMsg = "";

            ////
            try
            {
                var salesOrder = (from t in entities.requestnetsuite_task
                                  where t.rnt_id == int.Parse(refNo)
                                  //&& t.rnt_updatedDate > rangeFrom
                                  //&& t.rnt_updatedDate <= rangeTo
                                  //&& t.rnt_description == "BCAS-SALES ORDER"
                                  //&& t.rnt_status == "TRUE"
                                  select t).ToList();

                ItemFulfillment[] iffList = new ItemFulfillment[salesOrder.Count()];
                Int32 fulFillCount = 0;
                Int32 rowCount = 0;

                foreach (var so in salesOrder)
                {
                    try
                    {
                        InitializeRef refSO = new InitializeRef();
                        refSO.type = InitializeRefType.salesOrder;
                        refSO.internalId = so.rnt_nsInternalId;
                        refSO.typeSpecified = true;

                        InitializeRecord recSO = new InitializeRecord();
                        recSO.type = InitializeType.itemFulfillment;
                        recSO.reference = refSO;

                        ReadResponse rrSO = service.initialize(recSO);
                        Record rSO = rrSO.record;

                        ItemFulfillment iff1 = (ItemFulfillment)rSO;
                        ItemFulfillment iff2 = new ItemFulfillment();
                        ItemFulfillmentItemList ifitemlist = iff1.itemList;

                        RecordRef refCreatedFrom = new RecordRef();
                        refCreatedFrom.internalId = iff1.createdFrom.internalId;
                        iff2.createdFrom = refCreatedFrom;

                        ItemFulfillmentItem[] ifitems = new ItemFulfillmentItem[ifitemlist.item.Length];
                        int count1 = 0;

                        if (ifitemlist.item.Count() > 0)
                        {
                            for (int i = 0; i < ifitemlist.item.Length; i++)
                            {
                                ItemFulfillmentItem iffi = new ItemFulfillmentItem();
                                RecordRef refItem = new RecordRef();
                                iffi.item = ifitemlist.item[i].item;

                                iffi.orderLine = ifitemlist.item[i].orderLine;
                                iffi.orderLineSpecified = true;

                                RecordRef refLocation = new RecordRef();
                                iffi.location = ifitemlist.item[i].location;

                                iffi.quantity = ifitemlist.item[i].quantity;
                                iffi.quantitySpecified = true;

                                iffi.itemIsFulfilled = true;
                                iffi.itemIsFulfilledSpecified = true;

                                ifitems[count1] = iffi;
                                count1++;
                            }

                            ItemFulfillmentItemList ifil1 = new ItemFulfillmentItemList();
                            ifil1.item = ifitems;
                            iff2.itemList = ifil1;

                            iffList[fulFillCount] = iff2;
                            rowCount = fulFillCount + 1;

                            fulFillCount++;
                            status = true;
                        }
                    }
                    catch (Exception ex)
                    {
                        status = false;
                        if (rowCount == 0)
                        {
                            rowCount++;
                        }
                        break;
                    }
                }//end of ordMaster

                if (status == true)
                {
                    WriteResponseList resList = service.addList(iffList);
                    WriteResponse[] res = resList.writeResponse; 
                    foreach (WriteResponse result in res)
                    {
                        if (result.status.isSuccess == false)
                        {
                            status = false;
                            if (result.status.statusDetail != null)
                            {
                                for (int j = 0; j < result.status.statusDetail.Count(); j++)
                                {
                                    errorMsg += "" + (j + 1) + ". " + result.status.statusDetail[j].code + ": " + result.status.statusDetail[j].message + "";
                                }
                            }
                        }
                    }
                }

            }
            catch (Exception ex)
            {
                errorMsg = ex.ToString();
                status = false;
            }
            ////

            if (status == false)
            {
                return errorMsg;
            }
            else
            {
                return "";
            }
        }
        public string rerun_bcas_deduct_dummy_sales_order(NetSuiteService service, sdeEntities entities, string refNo, string refRange, string refType)
        {
            DateTime rangeFrom = DateTime.Parse(refRange.Substring(0, 19));
            DateTime rangeTo = DateTime.Parse(refRange.Substring(20, 19));

            Boolean status = false;
            String errorMsg = "";

            ////
            Int32 soCount = 0;
            Int32 rowCount = 0;

            try
            {
                #region Get ordered qty
                RecordRef refDummy = new RecordRef();
                refDummy.internalId = @Resource.BCAS_DUMMYSALES_INTERNALID;

                SearchPreferences sp = new SearchPreferences();
                sp.bodyFieldsOnly = false;
                service.searchPreferences = sp;

                TransactionSearchAdvanced sotsa = new TransactionSearchAdvanced();
                TransactionSearch sots = new TransactionSearch();
                TransactionSearchBasic sotsb = new TransactionSearchBasic();

                SearchMultiSelectField bcasDummySO = new SearchMultiSelectField();
                bcasDummySO.@operator = SearchMultiSelectFieldOperator.anyOf;
                bcasDummySO.operatorSpecified = true;
                bcasDummySO.searchValue = new RecordRef[] { refDummy };
                sotsb.internalId = bcasDummySO;

                sots.basic = sotsb;
                sotsa.criteria = sots;
                SearchResult sr = service.search(sotsa);
                Record[] srRecord = sr.recordList;

                SalesOrder decommitSO = new SalesOrder();
                List<String> deCommitItem = new List<String>();
                List<Int32> deCommitQty = new List<Int32>();

                for (int i = 0; i < srRecord.Count(); i++)
                {
                    SalesOrder so = (SalesOrder)srRecord[i];
                    decommitSO.itemList = so.itemList;

                    var query1 = (from q1 in entities.wms_jobordscan
                                  where q1.jos_businessChannel_code == "BC" 
                                  && (q1.jos_rangeTo > rangeFrom && q1.jos_rangeTo <= rangeTo)  
                                  select new { q1.jos_job_ID, q1.jos_businessChannel_code, q1.jos_country_tag, q1.jos_rangeTo })
                                  .Distinct()
                                  .OrderBy(x => x.jos_businessChannel_code)
                                  .ThenBy(y => y.jos_country_tag).ToList();

                    foreach (var q1 in query1)
                    {
                        var query2 = (from josp in entities.wms_jobordscan_pack
                                      join jomp in entities.netsuite_jobordmaster_pack 
                                      on josp.josp_pack_ID equals jomp.nsjomp_jobOrdMaster_pack_ID
                                      join jompd in entities.netsuite_jobordmaster_packdetail 
                                      on jomp.nsjomp_jobOrdMaster_pack_ID equals jompd.nsjompd_jobOrdMaster_pack_ID
                                      where josp.josp_jobID == q1.jos_job_ID
                                      && josp.josp_rangeTo > rangeFrom && josp.josp_rangeTo <= rangeTo
                                      select new { jompd.nsjompd_item_internalID, qty = (jomp.nsjomp_ordQty * jompd.nsjompd_sku_qty) }).ToList();

                        var groupQ2 = from p in query2
                                      let k = new
                                      {
                                          itemInternalID = p.nsjompd_item_internalID,
                                          ordQty = p.qty
                                      }
                                      group p by k into g
                                      select new
                                      {
                                          item = g.Key.itemInternalID,
                                          ordQty = g.Sum(p => p.qty)
                                      };

                        foreach (var q2 in groupQ2)
                        {
                            for (int j = 0; j < so.itemList.item.Count(); j++)
                            {
                                if (!String.IsNullOrEmpty(q2.item) && q2.item.Equals(so.itemList.item[j].item.internalId))
                                {
                                    deCommitItem.Add(so.itemList.item[j].item.internalId);
                                    deCommitQty.Add(Convert.ToInt32(so.itemList.item[j].quantity) - Convert.ToInt32(q2.ordQty));
                                    break;
                                }
                            }
                        }
                    }
                }
                #endregion
                #region Decommit item
                // only one dummy sales order
                SalesOrder[] soList = new SalesOrder[1];
                if (deCommitItem.Count() > 0)
                {
                    decommitSO.internalId = @Resource.BCAS_DUMMYSALES_INTERNALID;

                    RecordRef refSub = new RecordRef();
                    refSub.internalId = @Resource.BCAS_DUMMYSALES_MY;
                    decommitSO.subsidiary = refSub;

                    RecordRef refBusinessChannel = new RecordRef();
                    refBusinessChannel.internalId = @Resource.BCAS_DUMMYSALES_BUSINESSCHANNEL;
                    decommitSO.@class = refBusinessChannel;

                    decommitSO.orderStatus = SalesOrderOrderStatus._pendingFulfillment;
                    decommitSO.orderStatusSpecified = true;

                    SalesOrderItem[] soii = new SalesOrderItem[decommitSO.itemList.item.Count()];
                    SalesOrderItemList soil = new SalesOrderItemList();

                    for (int j = 0; j < decommitSO.itemList.item.Count(); j++)
                    {
                        SalesOrderItem soi = new SalesOrderItem();
                        RecordRef refItem = new RecordRef();
                        refItem.internalId = decommitSO.itemList.item[j].item.internalId;
                        soi.item = refItem;

                        soi.quantity = decommitSO.itemList.item[j].quantity;
                        soi.quantitySpecified = true;

                        soi.amount = 0;
                        soi.amountSpecified = true;

                        soi.createPoSpecified = false;

                        for (int i = 0; i < deCommitItem.Count(); i++)
                        {
                            if (deCommitItem[i].Equals(decommitSO.itemList.item[j].item.internalId))
                            {
                                soi.quantity = deCommitQty[i];
                                soi.quantitySpecified = true;
                                break;
                            }
                        }
                        soii[j] = soi;
                    }

                    soil.item = soii;
                    decommitSO.itemList = soil;
                    soList[soCount] = decommitSO;
                    rowCount = soCount + 1;
                    soCount++;
                    status = true;
                }
                #endregion

                if (status == true)
                {
                    WriteResponse[] res = service.addList(soList).writeResponse;
                    foreach (WriteResponse result in res)
                    {
                        if (result.status.isSuccess == false)
                        {
                            status = false;
                            if (result.status.statusDetail != null)
                            {
                                for (int j = 0; j < result.status.statusDetail.Count(); j++)
                                {
                                    errorMsg += "" + (j + 1) + ". " + result.status.statusDetail[j].code + ": " + result.status.statusDetail[j].message + "";
                                }
                            }
                        }
                    }
                }

            }
            catch (Exception ex)
            {
                errorMsg = ex.ToString();
                status = false;
            }
            ////

            if (status == false)
            {
                return errorMsg;
            }
            else
            {
                return "";
            }
        }
        #endregion
        #region CPAS-ReRun
        public string rerun_cpas_order_adjustment(NetSuiteService service, sdeEntities entities, string refNo, int taskId = 0)
        {
            Boolean status = false;
            String errorMsg = "";

            /////
            Int32 daCount = 0;
            Int32 rowCount = 0;
            Int32 itemCount = 0;

            try
            {

                var groupQ1 = (from q1 in entities.cpas_stockposting
                               where q1.spl_transactionType.Contains("ADJUSTMENT")
                               && q1.spl_sp_id == refNo
                               select new
                               {
                                   id = q1.spl_sp_id,
                                   tranType = q1.spl_transactionType,
                                   subsidiary = q1.spl_subsidiary_internalID,
                                   businessChannel = q1.spl_mb_businessChannel_internalID,
                                   memo = q1.spl_sDesc,
                                   postingDate = q1.spl_postingDate,
                               }).Distinct().ToList();

                InventoryAdjustment[] invAdjList = new InventoryAdjustment[groupQ1.Count()];
                InventoryAdjustmentInventoryList iail = new InventoryAdjustmentInventoryList();

                foreach (var q1 in groupQ1)
                {
                    InventoryAdjustment invAdj = new InventoryAdjustment();

                    RecordRef refAccount = new RecordRef();
                    refAccount.internalId = @Resource.ADJUSTMENT_ACCOUNT_REPLACEMENT;
                    invAdj.account = refAccount;

                    CustomFieldRef[] cfrList = new CustomFieldRef[1];
                    StringCustomFieldRef scfr = new StringCustomFieldRef();
                    scfr.scriptId = @Resource.CUSTOMFIELD_INVADJ_ADJTYPE_SCRIPTID;
                    scfr.internalId = @Resource.CUSTOMFIELD_INVADJ_ADJTYPE_INTERNALID;
                    scfr.value = @Resource.CUSTOMVALUE_INVADJ_ADJTYPE_REPLACEMENT;
                    cfrList[0] = scfr;
                    invAdj.customFieldList = cfrList;

                    RecordRef refSubsidiary = new RecordRef();
                    refSubsidiary.internalId = q1.subsidiary;
                    invAdj.subsidiary = refSubsidiary;

                    RecordRef refBusinessChannel = new RecordRef();
                    refBusinessChannel.internalId = q1.businessChannel;
                    invAdj.@class = refBusinessChannel;

                    invAdj.tranDate = Convert.ToDateTime(q1.postingDate);
                    invAdj.memo = q1.memo;

                    var ordAdj = (from o in entities.cpas_stockposting
                                  where o.spl_transactionType == q1.tranType && o.spl_sp_id == q1.id
                                  select o).ToList();

                    var ordAdjItem = from p in ordAdj
                                     let k = new
                                     {
                                         item = p.spl_mi_item_internalID,
                                         tranType = p.spl_transactionType,
                                         loc = p.spl_ml_location_internalID,
                                         inOut = p.spl_inout
                                     }
                                     group p by k into g
                                     select new
                                     {
                                         item = g.Key.item,
                                         tranType = g.Key.tranType,
                                         loc = g.Key.loc,
                                         inout = g.Key.inOut,
                                         qty = g.Sum(p => p.spl_dQty)
                                     };

                    if (ordAdjItem.Count() > 0)
                    {
                        InventoryAdjustmentInventory[] items = new InventoryAdjustmentInventory[ordAdjItem.Count()];

                        foreach (var i in ordAdjItem)
                        {
                            InventoryAdjustmentInventory item = new InventoryAdjustmentInventory();

                            RecordRef refItem = new RecordRef();
                            refItem.internalId = i.item;
                            item.item = refItem;

                            RecordRef refLocation = new RecordRef();
                            refLocation.internalId = i.loc;
                            item.location = refLocation;

                            if (i.inout.Equals("I"))
                            {
                                item.adjustQtyBy = Convert.ToDouble(i.qty);
                            }
                            else
                            {
                                item.adjustQtyBy = -(Convert.ToDouble(i.qty));
                            }
                            item.adjustQtyBySpecified = true;

                            items[itemCount] = item;
                            itemCount++;
                        }
                        iail.inventory = items;
                        invAdj.inventoryList = iail;
                        invAdjList[daCount] = invAdj;

                        rowCount = daCount + 1;
                        daCount++;
                        status = true;
                    }
                }

                if (status == true)
                {
                    if (invAdjList != null && invAdjList.Count() > 0)
                    {
                        WriteResponseList resList = service.addList(invAdjList);
                        WriteResponse[] res = resList.writeResponse; 
                        foreach (WriteResponse result in res)
                        {
                            if (result.status.isSuccess == false)
                            {
                                status = false;
                                if (result.status.statusDetail != null)
                                {
                                    for (int j = 0; j < result.status.statusDetail.Count(); j++)
                                    {
                                        errorMsg += "" + (j + 1) + ". " + result.status.statusDetail[j].code + ": " + result.status.statusDetail[j].message + "";
                                    }
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                errorMsg = ex.ToString();
                status = false;
            }
            /////

            if (status == false)
            {
                return errorMsg;
            }
            else
            {
                return "";
            }
        }
        public string rerun_cpas_order_cancellation(NetSuiteService service, sdeEntities entities, string refNo, int taskId = 0)
        {
            Boolean status = false;
            String errorMsg = "";

            /////
            Int32 daCount = 0;
            Int32 rowCount = 0;

            try
            {
                var groupQ1 = (from q1 in entities.cpas_stockposting
                               where (q1.spl_transactionType == "RNCO" || q1.spl_transactionType == "RETN")
                               && (q1.spl_sp_id == refNo)
                               select new
                               {
                                   id = q1.spl_sp_id,
                                   tranType = q1.spl_transactionType,
                                   subsidiary = q1.spl_subsidiary_internalID,
                                   businessChannel = q1.spl_mb_businessChannel_internalID,
                                   memo = q1.spl_sDesc,
                                   postingDate = q1.spl_postingDate
                               }).Distinct().ToList();

                InventoryAdjustment[] invAdjList = new InventoryAdjustment[groupQ1.Count()];
                InventoryAdjustmentInventoryList iail = new InventoryAdjustmentInventoryList();

                foreach (var q1 in groupQ1)
                {
                    InventoryAdjustment invAdj = new InventoryAdjustment();

                    CustomFieldRef[] cfrList = new CustomFieldRef[1];
                    StringCustomFieldRef scfr = new StringCustomFieldRef();
                    scfr.scriptId = @Resource.CUSTOMFIELD_INVADJ_ADJTYPE_SCRIPTID;
                    scfr.internalId = @Resource.CUSTOMFIELD_INVADJ_ADJTYPE_INTERNALID;

                    RecordRef refAccount = new RecordRef();
                    if (q1.tranType.Equals("RETN"))
                    {
                        refAccount.internalId = @Resource.ADJUSTMENT_ACCOUNT_RETURN;
                        scfr.value = @Resource.CUSTOMVALUE_INVADJ_ADJTYPE_RETN;
                        cfrList[0] = scfr;
                    }
                    else if (q1.tranType.Equals("RNCO"))
                    {
                        refAccount.internalId = @Resource.ADJUSTMENT_ACCOUNT_RETURN_CHARGEOFF;
                        scfr.value = @Resource.CUSTOMVALUE_INVADJ_ADJTYPE_RNCO;
                        cfrList[0] = scfr;
                    }
                    invAdj.account = refAccount;
                    invAdj.customFieldList = cfrList;

                    RecordRef refSubsidiary = new RecordRef();
                    refSubsidiary.internalId = q1.subsidiary;
                    invAdj.subsidiary = refSubsidiary;

                    RecordRef refBusinessChannel = new RecordRef();
                    refBusinessChannel.internalId = q1.businessChannel;
                    invAdj.@class = refBusinessChannel;

                    invAdj.tranDate = Convert.ToDateTime(q1.postingDate);
                    invAdj.memo = q1.memo;

                    var ordAdj = (from o in entities.cpas_stockposting
                                  where o.spl_transactionType == q1.tranType && o.spl_sp_id == q1.id
                                  select o).ToList();

                    var ordAdjItem = from p in ordAdj
                                     let k = new
                                     {
                                         item = p.spl_mi_item_internalID,
                                         tranType = p.spl_transactionType,
                                         loc = p.spl_ml_location_internalID,
                                         memo = p.spl_sDesc,
                                         postingDate = p.spl_postingDate
                                     }
                                     group p by k into g
                                     select new
                                     {
                                         item = g.Key.item,
                                         tranType = g.Key.tranType,
                                         loc = g.Key.loc,
                                         memo = g.Key.memo,
                                         postingDate = g.Key.postingDate,
                                         qty = g.Sum(p => p.spl_dQty)
                                     };

                    if (ordAdjItem.Count() > 0)
                    {
                        InventoryAdjustmentInventory[] items = new InventoryAdjustmentInventory[ordAdjItem.Count()];
                        Int32 itemCount = 0;

                        foreach (var i in ordAdjItem)
                        {
                            InventoryAdjustmentInventory item = new InventoryAdjustmentInventory();

                            RecordRef refItem = new RecordRef();
                            refItem.internalId = i.item;

                            RecordRef refLocation = new RecordRef();
                            refLocation.internalId = i.loc;

                            item.item = refItem;
                            item.location = refLocation;
                            item.adjustQtyBy = Convert.ToDouble(i.qty);
                            item.adjustQtyBySpecified = true;
                            items[itemCount] = item;
                            itemCount++;
                        }
                        iail.inventory = items;
                        invAdj.inventoryList = iail;
                        invAdjList[daCount] = invAdj;

                        rowCount = daCount + 1;
                        daCount++;
                        status = true;
                    }
                }

                if (status == true)
                {
                    if (invAdjList != null && invAdjList.Count() > 0)
                    {
                        WriteResponseList resList = service.addList(invAdjList);
                        WriteResponse[] res = resList.writeResponse;
                        foreach (WriteResponse result in res)
                        {
                            if (result.status.isSuccess == false)
                            {
                                status = false;
                                if (result.status.statusDetail != null)
                                {
                                    for (int j = 0; j < result.status.statusDetail.Count(); j++)
                                    {
                                        errorMsg += "" + (j + 1) + ". " + result.status.statusDetail[j].code + ": " + result.status.statusDetail[j].message + "";
                                    }
                                }
                            }
                        }
                    }
                }

            }
            catch (Exception ex)
            {
                errorMsg = ex.ToString();
                status = false;
            }

            ////

            if (status == false)
            {
                return errorMsg;
            }
            else
            {
                return "";
            }
        }
        public string rerun_cpas_journal(NetSuiteService service, sdeEntities entities, string refNo, int taskId = 0)
        {
            Boolean status = false;
            String errorMsg = "";

            ////
            Int32 jnCount = 0;
            Int32 rowCount = 0;

            try
            {
                var journal = (from jn in entities.cpas_journal
                               join q2 in entities.map_country on jn.jn_subsidiary equals q2.mc_countryCode
                               where (jn.jn_journalID == refNo)
                               && jn.jn_tranType != "PRESALES"
                               select new { jn.jn_tranType, jn.jn_desc, jn.jn_subsidiary_internalID, jn.jn_postingDate, q2.mc_country_internalID }).ToList();

                var journalItem = from j in journal
                                  let k = new
                                  {
                                      tranType = j.jn_tranType,
                                      desc = j.jn_desc,
                                      subsidiary = j.jn_subsidiary_internalID,
                                      postingDate = j.jn_postingDate,
                                      country = j.mc_country_internalID
                                  }
                                  group j by k into g
                                  select new
                                  {
                                      tranType = g.Key.tranType,
                                      desc = g.Key.desc,
                                      subsidiary = g.Key.subsidiary,
                                      postingDate = g.Key.postingDate,
                                      country = g.Key.country
                                  };

                JournalEntry[] jeList = new JournalEntry[journalItem.Count()];
                foreach (var j in journalItem)
                {
                    try
                    {
                        Int32 lineCount = 0;
                        JournalEntry je = new JournalEntry();
                        JournalEntryLineList jell = new JournalEntryLineList();

                        //je.tranDate = DateTime.Now;
                        je.tranDate = Convert.ToDateTime(j.postingDate);
                        je.tranDateSpecified = true;

                        RecordRef refSub = new RecordRef();
                        refSub.internalId = j.subsidiary;
                        je.subsidiary = refSub;

                        CustomFieldRef[] cfrList1 = new CustomFieldRef[1];
                        StringCustomFieldRef scfr1 = new StringCustomFieldRef();
                        scfr1.scriptId = @Resource.CUSTOMFIELD_REMARKS_SCRIPTID;
                        scfr1.internalId = @Resource.CUSTOMFIELD_REMARKS_INTERNALID;
                        scfr1.value = j.desc;
                        cfrList1[0] = scfr1;

                        var journalLine = new List<cpas_journal>();
                        if (j.tranType.Equals("TOTAL UNSHIP"))
                        {
                            journalLine = (from jn in entities.cpas_journal
                                           where (jn.jn_tranType == j.tranType || jn.jn_tranType == "PRESALES") && jn.jn_subsidiary_internalID == j.subsidiary && jn.jn_postingDate == j.postingDate
                                           select jn).ToList();
                        }
                        else
                        {
                            journalLine = (from jn in entities.cpas_journal
                                           where jn.jn_tranType == j.tranType && jn.jn_subsidiary_internalID == j.subsidiary && jn.jn_postingDate == j.postingDate
                                           select jn).ToList();
                        }

                        if (journalLine.Count() > 0)
                        {
                            JournalEntryLine[] lines = new JournalEntryLine[journalLine.Count()];

                            foreach (var i in journalLine)
                            {
                                JournalEntryLine line = new JournalEntryLine();

                                RecordRef refAccount = new RecordRef();
                                refAccount.internalId = i.jn_account_internalID;
                                line.account = refAccount;

                                RecordRef refBusinessChannel = new RecordRef();
                                refBusinessChannel.internalId = i.jn_businessChannel_internalID;
                                line.@class = refBusinessChannel;

                                if (j.tranType.Equals("RNCO"))
                                {
                                    RecordRef refDepartment = new RecordRef();
                                    refDepartment.internalId = @Resource.COMPANY_COSTCENTER_INTERNALID;
                                    line.department = refDepartment;
                                }

                                CustomFieldRef[] cfrList = new CustomFieldRef[1];
                                StringCustomFieldRef scfr = new StringCustomFieldRef();
                                scfr.scriptId = @Resource.CUSTOMFIELD_SCHO_SALESTERRITORY_SCRIPTID;
                                scfr.internalId = @Resource.CUSTOMFIELD_SCHO_SALESTERRITORY_INTERNALID;
                                scfr.value = j.country;
                                cfrList[0] = scfr;

                                if (i.jn_crAmount > 0)
                                {
                                    line.credit = Convert.ToDouble(i.jn_crAmount);
                                    line.creditSpecified = true;
                                }
                                else if (i.jn_drAmount > 0)
                                {
                                    line.debit = Convert.ToDouble(i.jn_drAmount);
                                    line.debitSpecified = true;
                                }
                                lines[lineCount] = line;
                                lineCount++;

                                rowCount = jnCount + 1;
                            }

                            jell.line = lines;
                            je.lineList = jell;
                            jeList[jnCount] = je;
                            jnCount++;
                            status = true;
                        }
                    }
                    catch (Exception ex)
                    {
                        status = false;
                    }
                }//end of journal

                if (status == true)
                {
                    WriteResponse[] res = service.addList(jeList).writeResponse;
                    foreach (WriteResponse result in res)
                    {
                        if (result.status.isSuccess == false)
                        {
                            status = false;
                            if (result.status.statusDetail != null)
                            {
                                for (int j = 0; j < result.status.statusDetail.Count(); j++)
                                {
                                    errorMsg += "" + (j + 1) + ". " + result.status.statusDetail[j].code + ": " + result.status.statusDetail[j].message + "";
                                }
                            }
                        }
                    }
                }

            }
            catch (Exception ex)
            {
                errorMsg = ex.ToString();
                status = false;
            }
            ////

            if (status == false)
            {
                return errorMsg;
            }
            else
            {
                return "";
            }
        }
        public string rerun_cpas_sales_order(NetSuiteService service, sdeEntities entities, string referenceNo, int taskId = 0)
        {
            Boolean status = false;
            String errorMsg = "";
            Int32 soCount = 0;
            Int32 rowCount = 0;

            try
            {
                var cpasSalesGroup = (from c in entities.cpas_stockposting
                                      where (c.spl_transactionType == "SALES" || c.spl_transactionType == "UNSHIP")
                                      && (c.spl_sp_id == referenceNo)
                                      select new
                                      {
                                          id = c.spl_sp_id,
                                          tranType = c.spl_transactionType,
                                          subsidiary = c.spl_subsidiary_internalID,
                                          businessChannel = c.spl_mb_businessChannel_internalID,
                                          postingDate = c.spl_postingDate,
                                          memo = c.spl_sDesc
                                      }).Distinct().ToList();

                SalesOrder[] soList = new SalesOrder[cpasSalesGroup.Count()];

                foreach (var con in cpasSalesGroup)
                {
                    try
                    {
                        String refNo = null;
                        SalesOrder so = new SalesOrder();

                        RecordRef refForm = new RecordRef();
                        RecordRef refEntity = new RecordRef();
                        switch (con.subsidiary)
                        {
                            case "3"://hard code
                                refForm.internalId = @Resource.CPAS_SALES_CUSTOMFORM_MY;
                                so.customForm = refForm;

                                refEntity.internalId = @Resource.CPAS_CUSTOMER_MY;
                                so.entity = refEntity;
                                break;
                            case "5"://hard code
                                refForm.internalId = @Resource.CPAS_SALES_CUSTOMFORM_SG;
                                so.customForm = refForm;

                                refEntity.internalId = @Resource.CPAS_CUSTOMER_SG;
                                so.entity = refEntity;
                                break;
                        }

                        RecordRef refTerm = new RecordRef();
                        refTerm.internalId = @Resource.ACCOUNTINGLIST_TERMS_INTERNALID;//default 60 days
                        so.terms = refTerm;

                        //so.tranDate = DateTime.Now;
                        so.tranDate = Convert.ToDateTime(con.postingDate);
                        so.tranDateSpecified = true;

                        so.orderStatus = SalesOrderOrderStatus._pendingFulfillment;
                        so.orderStatusSpecified = true;

                        RecordRef refClass = new RecordRef();
                        refClass.internalId = con.businessChannel;
                        so.@class = refClass;

                        so.memo = con.memo;

                        CustomFieldRef[] cfrList = new CustomFieldRef[1];
                        StringCustomFieldRef scfr = new StringCustomFieldRef();
                        scfr.scriptId = @Resource.CUSTOMFIELD_SYNCTOWMS_SCRIPTID;
                        scfr.internalId = @Resource.CUSTOMFIELD_SYNCTOWMS_INTERNALID;
                        scfr.value = "2";
                        cfrList[0] = scfr;

                        so.customFieldList = cfrList;

                        /*
                        RecordRef refLocation = new RecordRef();
                        //refLocation.internalId = con.so_ml_location_internalID;
                        refLocation.internalId = "6";
                        so.location = refLocation;
                        */
                        refNo = con.id;

                        var conItem = (from i in entities.cpas_stockposting
                                       where i.spl_sp_id == refNo
                                       select i).ToList();

                        var conItemGroup = from p in conItem
                                           let k = new
                                           {
                                               item = p.spl_mi_item_internalID,
                                               location = p.spl_ml_location_internalID,
                                           }
                                           group p by k into g
                                           select new
                                           {
                                               item = g.Key.item,
                                               location = g.Key.location,
                                               qty = g.Sum(p => p.spl_dQty)
                                           };

                        SalesOrderItem[] soii = new SalesOrderItem[conItemGroup.Count()];
                        SalesOrderItemList soil = new SalesOrderItemList();

                        if (conItemGroup.Count() > 0)
                        {
                            int itemCount = 0;
                            foreach (var item in conItemGroup)
                            {
                                SalesOrderItem soi = new SalesOrderItem();

                                RecordRef refItem = new RecordRef();
                                refItem.type = RecordType.inventoryItem;
                                refItem.typeSpecified = true;
                                refItem.internalId = item.item;
                                soi.item = refItem;
                                /*
                                soi.quantityFulfilled = Convert.ToDouble(item.spl_dQty);
                                soi.quantityFulfilledSpecified = true;
                                */
                                soi.quantity = Convert.ToDouble(item.qty);
                                soi.quantitySpecified = true;

                                //ANET-28 Sales Order able to apply commit tag at item line.
                                soi.commitInventory = SalesOrderItemCommitInventory._completeQty;
                                //soi.commitInventory = SalesOrderItemCommitInventory._availableQty;
                                soi.commitInventorySpecified = true;

                                soi.amount = 0;
                                soi.amountSpecified = true;

                                RecordRef refLocation = new RecordRef();
                                refLocation.internalId = item.location;
                                soi.location = refLocation;

                                soii[itemCount] = soi;
                                itemCount++;
                            }
                            soil.item = soii;
                            so.itemList = soil;
                            soList[soCount] = so;

                            rowCount = soCount + 1;
                            soCount++;
                            status = true;
                        }
                    }
                    catch (Exception ex)
                    {
                        status = false;
                    }
                }//end of cpascontract

                if (status == true)
                {
                    WriteResponse[] res = service.addList(soList).writeResponse;
                    foreach (WriteResponse result in res)
                    {
                        RecordRef rec = (RecordRef)result.baseRef;
                        String recInternalId = rec.internalId;

                        if (result.status.isSuccess == false)
                        {
                            status = false;
                            if (result.status.statusDetail != null)
                            {
                                for (int j = 0; j < result.status.statusDetail.Count(); j++)
                                {
                                    errorMsg += "" + (j + 1) + ". " + result.status.statusDetail[j].code + ": " + result.status.statusDetail[j].message + "";
                                }
                            }
                        }

                        if (result.status.isSuccess == true)
                        {
                            var updateTask = "update requestnetsuite_task set rnt_nsInternalId ='" + recInternalId + "' where rnt_id = " + taskId;
                            entities.Database.ExecuteSqlCommand(updateTask);
                        }                        
                    }
                }
            }
            catch (Exception ex)
            {
                errorMsg = ex.ToString();
                status = false;
            }
            ////

            if (status == false)
            {
                return errorMsg;
            }
            else
            {
                return "";
            }
        }
        public string rerun_cpas_sales_order_fulfill(NetSuiteService service, sdeEntities entities, string referenceNo, int taskId = 0)
        {
            Boolean status = false;
            String errorMsg = "";
            Int32 rowCount = 0;

            ////
            try
            {
                var salesOrder = (from t in entities.requestnetsuite_task
                                  where t.rnt_id == taskId
                                  && t.rnt_status == "FALSE"
                                  select t).ToList();

                foreach (var so in salesOrder)
                {
                    try
                    {
                        String[] tempRefNo = so.rnt_refNO.Split('.');
                        String refNo = tempRefNo[2];
                        String strLocation = tempRefNo[4];

                        var location = (from q1 in entities.cpas_stockposting
                                        where q1.spl_sp_id == refNo
                                        && q1.spl_ml_location_internalID == strLocation
                                        select q1).ToList();

                        var locationGroup1 = from p in location
                                             let k = new
                                             {
                                                 id = p.spl_sp_id,
                                                 location = p.spl_ml_location_internalID
                                             }
                                             group p by k into g
                                             select new
                                             {
                                                 id = g.Key.id,
                                                 location = g.Key.location,
                                             };

                        ItemFulfillment[] iffList = new ItemFulfillment[locationGroup1.Count()];
                        Int32 ordCount = 0;

                        foreach (var l in locationGroup1)
                        {
                            InitializeRef refSO = new InitializeRef();
                            refSO.type = InitializeRefType.salesOrder;
                            refSO.internalId = so.rnt_nsInternalId;
                            refSO.typeSpecified = true;

                            InitializeRecord recSO = new InitializeRecord();
                            recSO.type = InitializeType.itemFulfillment;
                            recSO.reference = refSO;

                            ReadResponse rrSO = service.initialize(recSO);
                            Record rSO = rrSO.record;

                            ItemFulfillment iff1 = (ItemFulfillment)rSO;
                            ItemFulfillment iff2 = new ItemFulfillment();
                            ItemFulfillmentItemList ifitemlist = iff1.itemList;

                            RecordRef refCreatedFrom = new RecordRef();
                            refCreatedFrom.internalId = iff1.createdFrom.internalId;
                            iff2.createdFrom = refCreatedFrom;

                            ItemFulfillmentItem[] ifitems = new ItemFulfillmentItem[ifitemlist.item.Length]; //new ItemFulfillmentItem[itemList.Count()];
                            int count1 = 0;
                            for (int i = 0; i < ifitemlist.item.Length; i++)
                            //foreach (var item in itemList)
                            {
                                ItemFulfillmentItem iffi = new ItemFulfillmentItem();

                                RecordRef refItem = new RecordRef();
                                //refItem.internalId = item.spl_mi_item_internalID;
                                iffi.item = ifitemlist.item[i].item;// refItem;

                                iffi.orderLine = ifitemlist.item[i].orderLine;
                                iffi.orderLineSpecified = true;
                                /*
                                iffi.quantity = ifitemlist.item[i].quantityRemaining;// Convert.ToInt32(item.spl_dQty);
                                iffi.quantitySpecified = true;

                                iffi.itemIsFulfilled = true;
                                iffi.itemIsFulfilledSpecified = true;

                                ifitems[count1] = iffi;
                                count1++;
                                */

                                RecordRef refLocation = new RecordRef();

                                if (l.location.Equals(ifitemlist.item[i].location.internalId))
                                {
                                    iffi.location = ifitemlist.item[i].location;//refLocation;

                                    iffi.quantity = ifitemlist.item[i].quantityRemaining;// Convert.ToInt32(item.spl_dQty);
                                    iffi.quantitySpecified = true;

                                    iffi.itemIsFulfilled = true;
                                    iffi.itemIsFulfilledSpecified = true;

                                    ifitems[count1] = iffi;
                                    count1++;
                                }
                                else
                                {
                                    //refLocation.internalId = item.spl_ml_location_internalID;
                                    iffi.location = ifitemlist.item[i].location;//refLocation;

                                    iffi.quantity = 0;
                                    iffi.quantitySpecified = true;

                                    iffi.itemIsFulfilled = false;
                                    iffi.itemIsFulfilledSpecified = true;

                                    ifitems[count1] = iffi;
                                    count1++;
                                }
                            }

                            ItemFulfillmentItemList ifil1 = new ItemFulfillmentItemList();
                            ifil1.item = ifitems;
                            iff2.itemList = ifil1;

                            iffList[ordCount] = iff2;
                            rowCount = ordCount + 1;

                            WriteResponse res = service.add(iffList[ordCount]);
                            RecordRef rec = (RecordRef)res.baseRef;
                            String recInternalId = rec.internalId;
                            if (res.status.isSuccess == false)
                            {
                                status = false;
                                if (res.status.statusDetail != null)
                                {
                                    errorMsg = "1. " + res.status.statusDetail;
                                }
                            }

                            if (res.status.isSuccess == true)
                            {
                                status = true;
                                var updateTask = "update requestnetsuite_task set rnt_nsInternalId ='" + recInternalId + "' where rnt_id = " + taskId;
                                entities.Database.ExecuteSqlCommand(updateTask);
                            }

                            ordCount++;
                        }
                    }
                    catch (Exception ex)
                    {
                        status = false;
                    }
                } 
            }
            catch (Exception ex)
            {
                errorMsg = ex.ToString();
                status = false;
            }
            ////

            if (status == false)
            {
                return errorMsg;
            }
            else
            {
                return "";
            }
        }
        #endregion

        public String convertDateToString(DateTime date)
        {
            String convertedDate = null;
            convertedDate = date.ToString("yyyy-MM-dd HH:mm:ss");
            return convertedDate;
        }

    }
}