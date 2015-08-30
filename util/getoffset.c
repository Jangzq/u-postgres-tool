#include <c.h>
#include "catalog/pg_control.h"
#include "access/xlog_internal.h"
#include "storage/bufpage.h"
#include "postgres.h"
#include "access/htup_details.h"
#include "access/multixact.h"

#include <stdio.h>

#define MAX_MAPPINGS            62

typedef struct RelMapping {
	Oid mapoid; /* OID of a catalog */
	Oid mapfilenode; /* its filenode number */
} RelMapping;

typedef struct RelMapFile {
	int32 magic; /* always RELMAPPER_FILEMAGIC */
	int32 num_mappings; /* number of valid RelMapping entries */
	RelMapping mappings[MAX_MAPPINGS];
	pg_crc32c crc; /* CRC of all above */
	int32 pad; /* to make the struct size be 512 exactly */
} RelMapFile;

static void append(char * key, int size, FILE * fp) {
	char buffer[500];
	sprintf(buffer, "%s=%d\n", key, size);
	printf("%s\n", buffer);
	fputs(buffer, fp);
}

static void typeappend(char * key, int size, FILE * fp) {
	char buffer[500];
	sprintf(buffer, "%s=%u\n", key, size);
	printf("%s\n", buffer);
	fputs(buffer, fp);
}

int main() {
	FILE *fp;
	fp = fopen("offset.dat", "wt");

	append("ControlFileData.system_identifier",
			offsetof(ControlFileData, system_identifier), fp);
	append("ControlFileData.state", offsetof(ControlFileData, state), fp);
	append("ControlFileData.catalog_version_no",
			offsetof(ControlFileData, catalog_version_no), fp);
	append("ControlFileData.checkPoint", offsetof(ControlFileData, checkPoint),
			fp);
	append("ControlFileData.prevCheckPoint",
			offsetof(ControlFileData, prevCheckPoint), fp);
	append("ControlFileData.checkPointCopy",
			offsetof(ControlFileData, checkPointCopy), fp);
	append("ControlFileData.minRecoveryPoint",
			offsetof(ControlFileData, minRecoveryPoint), fp);
	append("ControlFileData.minRecoveryPointTLI",
			offsetof(ControlFileData, minRecoveryPointTLI), fp);

	append("ControlFileData.backupStartPoint",
			offsetof(ControlFileData, backupStartPoint), fp);
	append("ControlFileData.backupEndPoint",
			offsetof(ControlFileData, backupEndPoint), fp);
	append("ControlFileData.backupEndRequired",
			offsetof(ControlFileData, backupEndRequired), fp);
	append("ControlFileData.wal_level", offsetof(ControlFileData, wal_level),
			fp);
	append("ControlFileData.maxAlign", offsetof(ControlFileData, maxAlign), fp);

	append("ControlFileData.blcksz", offsetof(ControlFileData, blcksz), fp);
	append("ControlFileData.relseg_size",
			offsetof(ControlFileData, relseg_size), fp);

	append("ControlFileData.xlog_blcksz",
			offsetof(ControlFileData, xlog_blcksz), fp);
	append("ControlFileData.xlog_seg_size",
			offsetof(ControlFileData, xlog_seg_size), fp);
	append("ControlFileData.nameDataLen",
			offsetof(ControlFileData, nameDataLen), fp);

	append("ControlFileData.float4ByVal",
			offsetof(ControlFileData, float4ByVal), fp);
	append("ControlFileData.float8ByVal",
			offsetof(ControlFileData, float8ByVal), fp);

	append("ControlFileData.crc", offsetof(ControlFileData, crc), fp);

	append("CheckPoint.redo", offsetof(CheckPoint, redo), fp);
	append("CheckPoint.ThisTimeLineID", offsetof(CheckPoint, ThisTimeLineID),
			fp);
	append("CheckPoint.PrevTimeLineID", offsetof(CheckPoint, PrevTimeLineID),
			fp);
	append("CheckPoint.fullPageWrites", offsetof(CheckPoint, fullPageWrites),
			fp);
	append("CheckPoint.nextXidEpoch", offsetof(CheckPoint, nextXidEpoch), fp);
	append("CheckPoint.nextXid", offsetof(CheckPoint, nextXid), fp);
	append("CheckPoint.nextOid", offsetof(CheckPoint, nextOid), fp);
	append("CheckPoint.nextMulti", offsetof(CheckPoint, nextMulti), fp);
	append("CheckPoint.nextMultiOffset", offsetof(CheckPoint, nextMultiOffset),
			fp);
	append("CheckPoint.oldestXid", offsetof(CheckPoint, oldestXid), fp);
	append("CheckPoint.oldestXidDB", offsetof(CheckPoint, oldestXidDB), fp);
	append("CheckPoint.oldestMulti", offsetof(CheckPoint, oldestMulti), fp);
	append("CheckPoint.oldestMultiDB", offsetof(CheckPoint, oldestMultiDB), fp);
	append("CheckPoint.time", offsetof(CheckPoint, time), fp);
	append("CheckPoint.oldestCommitTs", offsetof(CheckPoint, oldestCommitTs),
			fp);
	append("CheckPoint.newestCommitTs", offsetof(CheckPoint, newestCommitTs),
			fp);
	append("CheckPoint.oldestActiveXid", offsetof(CheckPoint, oldestActiveXid),
			fp);

	append("XLogPageHeaderData.xlp_magic",
			offsetof(XLogPageHeaderData, xlp_magic), fp);
	append("XLogPageHeaderData.xlp_info",
			offsetof(XLogPageHeaderData, xlp_info), fp);
	append("XLogPageHeaderData.xlp_tli", offsetof(XLogPageHeaderData, xlp_tli),
			fp);
	append("XLogPageHeaderData.xlp_pageaddr",
			offsetof(XLogPageHeaderData, xlp_pageaddr), fp);
	append("XLogPageHeaderData.xlp_rem_len",
			offsetof(XLogPageHeaderData, xlp_rem_len), fp);

	append("XLogLongPageHeaderData.xlp_sysid",
			offsetof(XLogLongPageHeaderData, xlp_sysid), fp);
	append("XLogLongPageHeaderData.xlp_seg_size",
			offsetof(XLogLongPageHeaderData, xlp_seg_size), fp);
	append("XLogLongPageHeaderData.xlp_xlog_blcksz",
			offsetof(XLogLongPageHeaderData, xlp_xlog_blcksz), fp);

	append("XLogRecord.xl_tot_len", offsetof(XLogRecord, xl_tot_len), fp);
	append("XLogRecord.xl_xid", offsetof(XLogRecord, xl_xid), fp);
	append("XLogRecord.xl_prev", offsetof(XLogRecord, xl_prev), fp);
	append("XLogRecord.xl_info", offsetof(XLogRecord, xl_info), fp);
	append("XLogRecord.xl_rmid", offsetof(XLogRecord, xl_rmid), fp);
	append("XLogRecord.xl_crc", offsetof(XLogRecord, xl_crc), fp);

	append("RelFileNode.spcNode", offsetof(RelFileNode, spcNode), fp);
	append("RelFileNode.dbNode", offsetof(RelFileNode, dbNode), fp);
	append("RelFileNode.relNode", offsetof(RelFileNode, relNode), fp);

	append("PageHeaderData.pd_lsn", offsetof(PageHeaderData, pd_lsn), fp);
	append("PageHeaderData.pd_checksum", offsetof(PageHeaderData, pd_checksum),
			fp);
	append("PageHeaderData.pd_flags", offsetof(PageHeaderData, pd_flags), fp);
	append("PageHeaderData.pd_lower", offsetof(PageHeaderData, pd_lower), fp);
	append("PageHeaderData.pd_upper", offsetof(PageHeaderData, pd_upper), fp);
	append("PageHeaderData.pd_special", offsetof(PageHeaderData, pd_special),
			fp);
	append("PageHeaderData.pd_pagesize_version",
			offsetof(PageHeaderData, pd_pagesize_version), fp);
	append("PageHeaderData.pd_prune_xid",
			offsetof(PageHeaderData, pd_prune_xid), fp);
	append("PageHeaderData.pd_linp", offsetof(PageHeaderData, pd_linp), fp);

	append("HeapTupleHeaderData.t_heap",
			offsetof(struct HeapTupleHeaderData, t_choice.t_heap), fp);
	append("HeapTupleHeaderData.t_ctid",
			offsetof(struct HeapTupleHeaderData, t_ctid), fp);
	append("HeapTupleHeaderData.t_infomask2",
			offsetof(struct HeapTupleHeaderData, t_infomask2), fp);
	append("HeapTupleHeaderData.t_infomask",
			offsetof(struct HeapTupleHeaderData, t_infomask), fp);
	append("HeapTupleHeaderData.t_hoff",
			offsetof(struct HeapTupleHeaderData, t_hoff), fp);
	append("HeapTupleHeaderData.t_bits",
			offsetof(struct HeapTupleHeaderData, t_bits), fp);

	append("HeapTupleFields.t_xmin", offsetof(struct HeapTupleFields, t_xmin),
			fp);
	append("HeapTupleFields.t_xmax", offsetof(struct HeapTupleFields, t_xmax),
			fp);
	append("HeapTupleFields.t_field3",
			offsetof(struct HeapTupleFields, t_field3), fp);

	append("DatumTupleFields.datum_len_",
			offsetof(struct DatumTupleFields, datum_len_), fp);
	append("DatumTupleFields.datum_typmod",
			offsetof(struct DatumTupleFields, datum_typmod), fp);
	append("DatumTupleFields.datum_typeid",
			offsetof(struct DatumTupleFields, datum_typeid), fp);

	append("ItemPointerData.ip_blkid",
			offsetof(struct ItemPointerData, ip_blkid), fp);
	append("ItemPointerData.ip_posid",
			offsetof(struct ItemPointerData, ip_posid), fp);

	append("BlockIdData.bi_hi", offsetof(struct BlockIdData, bi_hi), fp);
	append("BlockIdData.bi_lo", offsetof(struct BlockIdData, bi_lo), fp);

	append("RelMapFile.magic", offsetof(struct RelMapFile, magic), fp);
	append("RelMapFile.num_mappings", offsetof(struct RelMapFile, num_mappings),
			fp);
	append("RelMapFile.mappings", offsetof(struct RelMapFile, mappings), fp);
	append("RelMapFile.crc", offsetof(struct RelMapFile, crc), fp);

	append("RelMapping.mapoid", offsetof(struct RelMapping, mapoid), fp);
	append("RelMapping.mapfilenode", offsetof(struct RelMapping, mapfilenode),
			fp);

	append("varattrib_1b_e.va_header", offsetof(varattrib_1b_e, va_header), fp);
	append("varattrib_1b_e.va_tag", offsetof(varattrib_1b_e, va_tag), fp);
	append("varattrib_1b_e.va_data", offsetof(varattrib_1b_e, va_data), fp);

	append("varattrib_1b.va_header", offsetof(varattrib_1b, va_header), fp);

	append("varattrib_4b.va_4byte.va_header",
			offsetof(varattrib_4b, va_4byte.va_header), fp);

	append("MultiXactMember.xid", offsetof(MultiXactMember, xid), fp);
	append("MultiXactMember.status", offsetof(MultiXactMember, status), fp);

	fclose(fp);
	fp = fopen("type.dat", "wt");
	typeappend("bool", sizeof(bool), fp);
	typeappend("DBState", sizeof(DBState), fp);
	typeappend("SizeOfXLogLongPHD", SizeOfXLogLongPHD, fp);
	typeappend("SizeOfXLogShortPHD", SizeOfXLogShortPHD, fp);
	typeappend("SizeOfXLogRecord", SizeOfXLogRecord, fp);

	typeappend("MAXIMUM_ALIGNOF", MAXIMUM_ALIGNOF, fp);
	typeappend("ALIGNOF_SHORT", ALIGNOF_SHORT, fp);
	typeappend("ALIGNOF_INT", ALIGNOF_INT, fp);
	typeappend("ALIGNOF_LONG", ALIGNOF_LONG, fp);
	typeappend("ALIGNOF_DOUBLE", ALIGNOF_DOUBLE, fp);

	typeappend("RmgrId", sizeof(RmgrId), fp);
	typeappend("SizeOfXLogRecordDataHeaderShort",
			SizeOfXLogRecordDataHeaderShort, fp);
	typeappend("SizeOfXLogRecordDataHeaderLong", SizeOfXLogRecordDataHeaderLong,
			fp);
	typeappend("CheckPoint", sizeof(CheckPoint), fp);
	typeappend("RelFileNode", sizeof(RelFileNode), fp);
	typeappend("ItemIdData", sizeof(ItemIdData), fp);
	typeappend("RelMapFile", sizeof(RelMapFile), fp);
	typeappend("RelMapping", sizeof(RelMapping), fp);

	typeappend("varatt_indirect", sizeof(varatt_indirect), fp);
	typeappend("varatt_external", sizeof(varatt_external), fp);

	typeappend("TransactionId", sizeof(TransactionId), fp);

	typeappend("MultiXactMember", sizeof(MultiXactMember), fp);
	typeappend("MultiXactStatus", sizeof(MultiXactStatus), fp);
	typeappend("MultiXactOffset", sizeof(MultiXactOffset), fp);

	fclose(fp);

	return 0;
}
