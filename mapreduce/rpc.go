package mapreduce

type TaskType int

const (
	Map TaskType = iota
	Reduce
	Idle
)

type Task struct {
	Id int
	Type TaskType
	Filename string
}

type AssignTaskArgs struct {
	WorkerId int
}

type AssignTaskReply struct {
	Task Task
}