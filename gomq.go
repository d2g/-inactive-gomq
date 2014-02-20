package gomq

// #cgo CFLAGS: -I.
// #cgo LDFLAGS: -L. -lmqm
// #include "cmqc.h"
// #include <stdlib.h>
import "C"
import (
	"errors"
	"strconv"
	"strings"
	"unsafe"
)

type MQ struct {
	connection C.MQHCONN
}

func (t *MQ) Connect() error {
	var reason C.MQLONG
	var completion_code C.MQLONG

	C.MQCONN((*C.MQCHAR)(unsafe.Pointer(C.CString(""))), &t.connection, &completion_code, &reason)

	if completion_code == C.MQCC_FAILED {
		return errors.New("Connect Error:" + strconv.Itoa(int(completion_code)) + " With Reason:" + strconv.Itoa(int(reason)))
	}

	return nil
}

func (t *MQ) ConnectX() error {
	var reason C.MQLONG
	var completion_code C.MQLONG
	var connection_options C.MQCNO
	var eMQBYTE128 C.MQBYTE128
	var eMQBYTE24 C.MQBYTE24

	connection_options.StrucId = *(*C.MQCHAR4)(unsafe.Pointer(C.CString("CNO ")))
	connection_options.Version = C.MQCNO_VERSION_1
	connection_options.Options = C.MQCNO_HANDLE_SHARE_BLOCK
	connection_options.ClientConnOffset = 0
	connection_options.ClientConnPtr = nil
	connection_options.ConnTag = eMQBYTE128
	connection_options.SSLConfigPtr = nil
	connection_options.SSLConfigOffset = 0
	connection_options.ConnectionId = eMQBYTE24
	connection_options.SecurityParmsOffset = 0
	connection_options.SecurityParmsPtr = nil

	C.MQCONNX((*C.MQCHAR)(unsafe.Pointer(C.CString(""))), &connection_options, &t.connection, &completion_code, &reason)

	if completion_code == C.MQCC_FAILED {
		return errors.New("Connect Error:" + strconv.Itoa(int(completion_code)) + " With Reason:" + strconv.Itoa(int(reason)))
	}

	return nil
}

func (t *MQ) Disconnect() error {
	var reason C.MQLONG
	var completion_code C.MQLONG

	C.MQDISC(&t.connection, &completion_code, &reason)

	if completion_code == C.MQCC_FAILED {
		return errors.New("Disconnect Error:" + strconv.Itoa(int(completion_code)) + " With Reason:" + strconv.Itoa(int(reason)))
	}

	return nil
}

func (t *MQ) Open(queueName string) (MQQueue, error) {
	var reason C.MQLONG
	var completion_code C.MQLONG
	var options C.MQLONG
	var queuedescriptor C.MQOD
	var eMQBYTE40 C.MQBYTE40
	var eMQCHARV C.MQCHARV

	queue := MQQueue{
		parent: t,
	}

	queuedescriptor.StrucId = *(*C.MQCHAR4)(unsafe.Pointer(C.CString("OD  ")))
	queuedescriptor.Version = C.MQOD_VERSION_1
	queuedescriptor.ObjectType = C.MQOT_Q
	queuedescriptor.ObjectName = *(*C.MQCHAR48)(unsafe.Pointer(C.CString(queueName)))
	queuedescriptor.ObjectQMgrName = *(*C.MQCHAR48)(unsafe.Pointer(C.CString("")))
	queuedescriptor.DynamicQName = *(*C.MQCHAR48)(unsafe.Pointer(C.CString("AMQ.*")))
	queuedescriptor.AlternateUserId = *(*C.MQCHAR12)(unsafe.Pointer(C.CString("")))
	queuedescriptor.RecsPresent = 0
	queuedescriptor.KnownDestCount = 0
	queuedescriptor.UnknownDestCount = 0
	queuedescriptor.InvalidDestCount = 0
	queuedescriptor.ObjectRecOffset = 0
	queuedescriptor.ResponseRecOffset = 0
	queuedescriptor.ObjectRecPtr = nil
	queuedescriptor.ResponseRecPtr = nil
	queuedescriptor.AlternateSecurityId = eMQBYTE40
	queuedescriptor.ResolvedQName = *(*C.MQCHAR48)(unsafe.Pointer(C.CString("")))
	queuedescriptor.ResolvedQMgrName = *(*C.MQCHAR48)(unsafe.Pointer(C.CString("")))
	queuedescriptor.ObjectString = eMQCHARV
	queuedescriptor.SelectionString = eMQCHARV
	queuedescriptor.ResObjectString = eMQCHARV
	queuedescriptor.ResolvedType = C.MQOT_NONE

	options = C.MQOO_INPUT_AS_Q_DEF | C.MQOO_OUTPUT | C.MQOO_FAIL_IF_QUIESCING

	C.MQOPEN(t.connection, (C.PMQVOID)(unsafe.Pointer(&queuedescriptor)), options, &queue.queue, &completion_code, &reason)

	if completion_code == C.MQCC_FAILED {
		return queue, errors.New("Open Queue \"" + queueName + "\" Error:" + strconv.Itoa(int(completion_code)) + " With Reason:" + strconv.Itoa(int(reason)))
	}

	return queue, nil
}

type MQQueue struct {
	parent *MQ
	queue  C.MQHOBJ
}

func (t *MQQueue) Put(message string) error {

	var messageDescriptor C.MQMD
	var eMQBYTE24 C.MQBYTE24
	var eMQBYTE32 C.MQBYTE32

	messageDescriptor.StrucId = *(*C.MQCHAR4)(unsafe.Pointer(C.CString("MD  ")))
	messageDescriptor.Version = C.MQMD_VERSION_1
	messageDescriptor.Report = C.MQRO_NONE
	messageDescriptor.MsgType = C.MQMT_DATAGRAM
	messageDescriptor.Expiry = C.MQEI_UNLIMITED
	messageDescriptor.Feedback = C.MQFB_NONE
	messageDescriptor.Encoding = C.MQENC_NATIVE
	messageDescriptor.CodedCharSetId = C.MQCCSI_Q_MGR
	messageDescriptor.Format = *(*C.MQCHAR8)(unsafe.Pointer(C.CString("MQSTR   ")))
	messageDescriptor.Priority = C.MQPRI_PRIORITY_AS_Q_DEF
	messageDescriptor.Persistence = C.MQPER_PERSISTENCE_AS_Q_DEF
	messageDescriptor.MsgId = eMQBYTE24
	messageDescriptor.CorrelId = eMQBYTE24
	messageDescriptor.BackoutCount = 0
	messageDescriptor.ReplyToQ = *(*C.MQCHAR48)(unsafe.Pointer(C.CString("")))
	messageDescriptor.ReplyToQMgr = *(*C.MQCHAR48)(unsafe.Pointer(C.CString("")))
	messageDescriptor.UserIdentifier = *(*C.MQCHAR12)(unsafe.Pointer(C.CString("")))
	messageDescriptor.AccountingToken = eMQBYTE32
	messageDescriptor.ApplIdentityData = *(*C.MQCHAR32)(unsafe.Pointer(C.CString("")))
	messageDescriptor.PutApplType = C.MQAT_NO_CONTEXT
	messageDescriptor.PutApplName = *(*C.MQCHAR28)(unsafe.Pointer(C.CString("")))
	messageDescriptor.PutDate = *(*C.MQCHAR8)(unsafe.Pointer(C.CString("")))
	messageDescriptor.PutTime = *(*C.MQCHAR8)(unsafe.Pointer(C.CString("")))
	messageDescriptor.ApplOriginData = *(*C.MQCHAR4)(unsafe.Pointer(C.CString("")))
	messageDescriptor.GroupId = eMQBYTE24
	messageDescriptor.MsgSeqNumber = 1
	messageDescriptor.Offset = 0
	messageDescriptor.MsgFlags = C.MQMF_NONE
	messageDescriptor.OriginalLength = C.MQOL_UNDEFINED

	var putMessageOptions C.MQPMO

	putMessageOptions.StrucId = *(*C.MQCHAR4)(unsafe.Pointer(C.CString("PMO ")))
	putMessageOptions.Version = C.MQPMO_VERSION_1
	putMessageOptions.Options = C.MQPMO_NO_SYNCPOINT | C.MQPMO_FAIL_IF_QUIESCING
	putMessageOptions.Timeout = -1
	putMessageOptions.Context = 0
	putMessageOptions.KnownDestCount = 0
	putMessageOptions.UnknownDestCount = 0
	putMessageOptions.InvalidDestCount = 0
	putMessageOptions.ResolvedQName = *(*C.MQCHAR48)(unsafe.Pointer(C.CString("")))
	putMessageOptions.ResolvedQMgrName = *(*C.MQCHAR48)(unsafe.Pointer(C.CString("")))
	putMessageOptions.RecsPresent = 0
	putMessageOptions.PutMsgRecFields = C.MQPMRF_NONE
	putMessageOptions.PutMsgRecOffset = 0
	putMessageOptions.ResponseRecOffset = 0
	putMessageOptions.PutMsgRecPtr = nil
	putMessageOptions.ResponseRecPtr = nil
	putMessageOptions.OriginalMsgHandle = C.MQHM_NONE
	putMessageOptions.NewMsgHandle = C.MQHM_NONE
	putMessageOptions.Action = C.MQACTP_NEW
	putMessageOptions.PubLevel = 9

	var reason C.MQLONG
	var completion_code C.MQLONG

	C.MQPUT(t.parent.connection, t.queue, (C.PMQVOID)(unsafe.Pointer(&messageDescriptor)), (C.PMQVOID)(unsafe.Pointer(&putMessageOptions)), (C.MQLONG)(C.long(len(message))), (C.PMQVOID)(unsafe.Pointer(C.CString(message))), &completion_code, &reason)

	if completion_code == C.MQCC_FAILED {
		return errors.New("Put Message Error:" + strconv.Itoa(int(completion_code)) + " With Reason:" + strconv.Itoa(int(reason)))
	}

	return nil
}

/*
 * Returns:
 * bool (False = No Message)
 * String (Message)
 * Error
 */
func (t *MQQueue) Get() (bool, string, error) {

	var messageDescriptor C.MQMD
	var eMQBYTE16 C.MQBYTE16
	var eMQBYTE24 C.MQBYTE24
	var eMQBYTE32 C.MQBYTE32

	messageDescriptor.StrucId = *(*C.MQCHAR4)(unsafe.Pointer(C.CString("MD  ")))
	messageDescriptor.Version = C.MQMD_VERSION_1
	messageDescriptor.Report = C.MQRO_NONE
	messageDescriptor.MsgType = C.MQMT_DATAGRAM
	messageDescriptor.Expiry = C.MQEI_UNLIMITED
	messageDescriptor.Feedback = C.MQFB_NONE
	messageDescriptor.Encoding = C.MQENC_NATIVE
	messageDescriptor.CodedCharSetId = C.MQCCSI_Q_MGR
	messageDescriptor.Format = *(*C.MQCHAR8)(unsafe.Pointer(C.CString("        ")))
	messageDescriptor.Priority = C.MQPRI_PRIORITY_AS_Q_DEF
	messageDescriptor.Persistence = C.MQPER_PERSISTENCE_AS_Q_DEF
	messageDescriptor.MsgId = eMQBYTE24
	messageDescriptor.CorrelId = eMQBYTE24
	messageDescriptor.BackoutCount = 0
	messageDescriptor.ReplyToQ = *(*C.MQCHAR48)(unsafe.Pointer(C.CString("")))
	messageDescriptor.ReplyToQMgr = *(*C.MQCHAR48)(unsafe.Pointer(C.CString("")))
	messageDescriptor.UserIdentifier = *(*C.MQCHAR12)(unsafe.Pointer(C.CString("")))
	messageDescriptor.AccountingToken = eMQBYTE32
	messageDescriptor.ApplIdentityData = *(*C.MQCHAR32)(unsafe.Pointer(C.CString("")))
	messageDescriptor.PutApplType = C.MQAT_NO_CONTEXT
	messageDescriptor.PutApplName = *(*C.MQCHAR28)(unsafe.Pointer(C.CString("")))
	messageDescriptor.PutDate = *(*C.MQCHAR8)(unsafe.Pointer(C.CString("")))
	messageDescriptor.PutTime = *(*C.MQCHAR8)(unsafe.Pointer(C.CString("")))
	messageDescriptor.ApplOriginData = *(*C.MQCHAR4)(unsafe.Pointer(C.CString("")))
	messageDescriptor.GroupId = eMQBYTE24
	messageDescriptor.MsgSeqNumber = 1
	messageDescriptor.Offset = 0
	messageDescriptor.MsgFlags = C.MQMF_NONE
	messageDescriptor.OriginalLength = C.MQOL_UNDEFINED

	var getMessageOptions C.MQGMO

	getMessageOptions.StrucId = *(*C.MQCHAR4)(unsafe.Pointer(C.CString("GMO ")))
	getMessageOptions.Version = C.MQGMO_VERSION_1
	getMessageOptions.Options = (C.MQGMO_NO_WAIT + C.MQGMO_CONVERT)
	getMessageOptions.WaitInterval = 0
	getMessageOptions.Signal1 = 0
	getMessageOptions.Signal2 = 0
	getMessageOptions.ResolvedQName = *(*C.MQCHAR48)(unsafe.Pointer(C.CString("")))
	getMessageOptions.MatchOptions = (C.MQMO_MATCH_MSG_ID + C.MQMO_MATCH_CORREL_ID)
	getMessageOptions.GroupStatus = C.MQGS_NOT_IN_GROUP
	getMessageOptions.SegmentStatus = C.MQSS_NOT_A_SEGMENT
	getMessageOptions.Segmentation = C.MQSEG_INHIBITED
	getMessageOptions.Reserved1 = *(*C.MQCHAR)(unsafe.Pointer(C.CString(" ")))
	getMessageOptions.MsgToken = eMQBYTE16
	getMessageOptions.ReturnedLength = C.MQRL_UNDEFINED
	getMessageOptions.Reserved2 = 0
	getMessageOptions.MsgHandle = C.MQHM_NONE

	var reason C.MQLONG
	var completion_code C.MQLONG
	var message_length C.MQLONG

	buffer := make([]byte, 4194304)

	C.MQGET(t.parent.connection,
		t.queue,
		(C.PMQVOID)(unsafe.Pointer(&messageDescriptor)),
		(C.PMQVOID)(unsafe.Pointer(&getMessageOptions)),
		(C.MQLONG)(C.long(len(buffer))),
		(C.PMQVOID)(unsafe.Pointer(&buffer[0])),
		&message_length,
		&completion_code,
		&reason)

	goResult := strings.TrimSpace(string(buffer[:int(message_length)]))

	if reason == C.MQRC_NO_MSG_AVAILABLE {
		return false, "", nil
	}

	if completion_code == C.MQCC_FAILED {
		return false, "", errors.New("Get Message Error:" + strconv.Itoa(int(completion_code)) + " With Reason:" + strconv.Itoa(int(reason)))
	}

	return true, goResult, nil
}

func (t *MQQueue) Close() error {
	var reason C.MQLONG
	var completion_code C.MQLONG

	C.MQCLOSE(t.parent.connection, &t.queue, C.MQCO_NONE, &completion_code, &reason)

	if completion_code == C.MQCC_FAILED {
		return errors.New("Put Message Error:" + strconv.Itoa(int(completion_code)) + " With Reason:" + strconv.Itoa(int(reason)))
	}
	return nil
}
