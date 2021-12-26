package main

//FilesTaskType .
type FilesTaskType string

//FilesTaskType Enum
const (
	FilesTaskTypeCopy   = "copy"
	FilesTaskTypeMove   = "move"
	FilesTaskTypeEncode = "encode"
	FilesTaskTypeDecode = "decode"
	FilesTaskTypeDate   = "date"
)

//FilesTaskPriority .
type FilesTaskPriority byte

//FilesTaskPriority Enum
const (
	FilesTaskPrioritySkip   = byte(0)
	FilesTaskPriorityMin    = byte(1)
	FilesTaskPriorityNormal = byte(0x0F)
	FilesTaskPriorityMax    = byte(0xFF)
)

//FilesTaskDenc .
type FilesTaskDenc string

//FilesTaskDenc Enum
const (
	FilesTaskDencAuto    = "auto"
	FilesTaskDencEncrypt = "encrypt"
	FilesTaskDencDecrypt = "decrypt"
)

//FilesTaskExist .
type FilesTaskExist string

//FilesTaskExist Enum
const (
	FilesTaskExistResume    = "resume"
	FilesTaskExistOverwrite = "overwrite"
	FilesTaskExistSkip      = "skip"
)

//FilesTask .
type FilesTask struct {
	Source   string          `json:"source"`
	Target   string          `json:"target"`
	Priority byte            `json:"priority,omitempty"`
	Type     FilesTaskType   `json:"type"`
	Denc     *FilesTaskDenc  `json:"denc,omitempty"`
	Exist    *FilesTaskExist `json:"exist,omitempty"`
	Initial  string          `json:"initial,omitempty"`
	File     string
}

//ControlTaskType .
type ControlTaskType string

//ControlTaskType Enum
const (
	ControlTaskTypeReload  = "reload"
	ControlTaskTypeRestart = "restart"
	ControlTaskTypeStop    = "stop"
	ControlTaskTypeKill    = "kill"
	ControlTaskTypeUpdate  = "update"
)

//ControlTask .
type ControlTask struct {
	Type ControlTaskType `json:"type"`
	Data *string         `json:"data"`
}
