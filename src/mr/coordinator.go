package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var mu = sync.Mutex{}

type Coordinator struct {
	// Your definitions here.
	ReducerNum        int
	TaskId            int
	DistPhase         Phase
	TaskChannelMap    chan *Task
	TaskChannelReduce chan *Task
	taskMetaHolder    TaskMetaHolder // 存放所有的task
	files             []string
}

type TaskMetaHolder struct {
	MetaMap map[int]*TaskMetaInfo
}

type TaskMetaInfo struct {
	state     State
	StartTime time.Time
	TaskAdr   *Task
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) CrashDetector() {
	for {
		time.Sleep(2 * time.Second)
		mu.Lock()
		if c.DistPhase == AllDone {
			mu.Unlock()
			break
		}
		for _, v := range c.taskMetaHolder.MetaMap {
			if v.state == Working && time.Since(v.StartTime) > 9*time.Second {
				fmt.Printf("The task[ %d ] is crash, take [%d] s\n", v.TaskAdr.TaskId, time.Since(v.StartTime))
				switch v.TaskAdr.TaskType {
				case MapTask:
					c.TaskChannelMap <- v.TaskAdr
					v.state = Waiting
				case ReduceTask:
					c.TaskChannelReduce <- v.TaskAdr
					v.state = Waiting
				}
			}
		}
		mu.Unlock()
	}

}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	//ret := false

	// Your code here.
	mu.Lock()
	defer mu.Unlock()
	if c.DistPhase == AllDone {
		fmt.Println("All tasks are finished, the coordinator will be exit! !")
		return true
	} else {
		return false
	}

	//return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		ReducerNum:        nReduce,
		DistPhase:         MapPhase,
		TaskChannelMap:    make(chan *Task, len(files)),
		TaskChannelReduce: make(chan *Task, nReduce),
		taskMetaHolder: TaskMetaHolder{
			MetaMap: make(map[int]*TaskMetaInfo, len(files)+nReduce),
		},
		files: files,
	}
	// Your code here.
	c.makeMapTasks(files)

	c.server()
	go c.CrashDetector()
	return &c
}

func (c *Coordinator) makeMapTasks(files []string) {
	mu.Lock()
	defer mu.Unlock()
	for _, v := range files {
		id := c.generateTaskId()
		task := Task{
			TaskType:   MapTask,
			TaskId:     id,
			ReducerNum: c.ReducerNum,
			Filename:   v,
		}
		taskMetaInfo := TaskMetaInfo{
			state:   Waiting,
			TaskAdr: &task,
		}
		c.taskMetaHolder.acceptMeta(&taskMetaInfo)
		fmt.Println("Make a map task:", task)
		c.TaskChannelMap <- &task
	}
}

func (c *Coordinator) makeReduceTasks() {
	for i := 0; i < c.ReducerNum; i++ {
		id := c.generateTaskId()
		task := Task{
			TaskType:   ReduceTask,
			TaskId:     id,
			ReducerNum: i,
		}
		taskMetaInfo := TaskMetaInfo{
			state:   Waiting,
			TaskAdr: &task,
		}
		c.taskMetaHolder.acceptMeta(&taskMetaInfo)
		fmt.Println("Make a reduce task:", task)
		c.TaskChannelReduce <- &task
	}
}

func (c *Coordinator) generateTaskId() int {
	res := c.TaskId
	c.TaskId++
	return res
}

func (t *TaskMetaHolder) acceptMeta(TaskInfo *TaskMetaInfo) bool {
	taskId := TaskInfo.TaskAdr.TaskId
	meta, _ := t.MetaMap[taskId]
	if meta != nil {
		fmt.Println("Meta contains task which id = ", taskId)
		return false
	} else {
		t.MetaMap[taskId] = TaskInfo
	}
	return true
}

// rpc 分发任务
func (c *Coordinator) PollTask(args *TaskArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	switch c.DistPhase {
	case MapPhase:
		{
			fmt.Printf("Map phase..., task remains[%d] \t", len(c.TaskChannelMap))
			if len(c.TaskChannelMap) > 0 {
				*reply = *(<-c.TaskChannelMap)
				fmt.Println("Give map task: ", *reply)
				taskInfo := c.taskMetaHolder.MetaMap[reply.TaskId]
				taskInfo.state = Working
				taskInfo.StartTime = time.Now()
				//if !c.taskMetaHolder.judgeState(reply.TaskId) {
				//	fmt.Println("Map taskid[ %d ] is running\n", reply.TaskId)
				//}
			} else {
				reply.TaskType = WaittingTask
				fmt.Println("No map task, give waitting task...")
				if c.taskMetaHolder.checkTaskDone() {
					c.toNextPhase()
				}
			}
			return nil
		}
	case ReducePhase:
		{
			fmt.Printf("Reduce phase..., task remains[%d] \t", len(c.TaskChannelReduce))
			if len(c.TaskChannelReduce) > 0 {
				*reply = *(<-c.TaskChannelReduce)
				fmt.Println("Give reduce task: ", *reply)
				taskInfo := c.taskMetaHolder.MetaMap[reply.TaskId]
				taskInfo.state = Working
				taskInfo.StartTime = time.Now()
				//if !c.taskMetaHolder.judgeState(reply.TaskId) {
				//	fmt.Println("Reduce taskid[ %d ] is running\n", reply.TaskId)
				//}
			} else {
				reply.TaskType = WaittingTask
				fmt.Println("No reduce task, give waitting task...")
				if c.taskMetaHolder.checkTaskDone() {
					c.toNextPhase()
				}
			}
			return nil
		}
	case AllDone:
		{
			reply.TaskType = ExitTask
			fmt.Println("No task, give exit task...")
		}
	default:
		{
			panic("The phase undefined !!")
		}
	}
	return nil
}

func (c *Coordinator) toNextPhase() {
	if c.DistPhase == MapPhase {
		c.makeReduceTasks()
		c.DistPhase = ReducePhase
		// todo
		//c.DistPhase = AllDone
	} else if c.DistPhase == ReducePhase {
		c.DistPhase = AllDone
	}
}

func (t *TaskMetaHolder) checkTaskDone() bool {
	var (
		mapDoneNum      = 0
		mapUnDoneNum    = 0
		reduceDoneNum   = 0
		reduceUnDoneNum = 0
	)
	for _, v := range t.MetaMap {
		if v.TaskAdr.TaskType == MapTask {
			if v.state == Done {
				mapDoneNum++
			} else {
				mapUnDoneNum++
			}
		} else if v.TaskAdr.TaskType == ReduceTask {
			if v.state == Done {
				reduceDoneNum++
			} else {
				reduceUnDoneNum++
			}
		}

	}
	fmt.Printf("Map tasks are finished %d/%d, reduce task are finished %d/%d \n",
		mapDoneNum, mapDoneNum+mapUnDoneNum, reduceDoneNum, reduceDoneNum+reduceUnDoneNum)
	if (mapDoneNum > 0 && mapUnDoneNum == 0) && (reduceDoneNum == 0 && reduceUnDoneNum == 0) {
		// map phase finished
		return true
	}
	if (mapDoneNum > 0 && mapUnDoneNum == 0) && (reduceDoneNum > 0 && reduceUnDoneNum == 0) {
		// reduce phase finished
		return true
	}
	return false
}

func (t *TaskMetaHolder) judgeState(taskId int) bool {
	taskInfo, ok := t.MetaMap[taskId]
	if !ok || taskInfo.state != Waiting {
		return false
	}
	taskInfo.state = Working
	taskInfo.StartTime = time.Now()
	return true
}

func (c *Coordinator) MarkFinished(args *Task, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	switch args.TaskType {
	case MapTask:
		meta, _ := c.taskMetaHolder.MetaMap[args.TaskId]
		meta.state = Done
		//prevent a duplicated work which returned from another worker
		//meta, ok := c.taskMetaHolder.MetaMap[args.TaskId]
		//if ok && meta.state == Working {
		//	meta.state = Done
		//	fmt.Printf("Map task Id[%d] is finished.\n", args.TaskId)
		//} else {
		//	fmt.Printf("Map task Id[%d] is finished,already ! ! !\n", args.TaskId)
		//}
	case ReduceTask:
		meta, _ := c.taskMetaHolder.MetaMap[args.TaskId]
		meta.state = Done
		//prevent a duplicated work which returned from another worker
		//meta, ok := c.taskMetaHolder.MetaMap[args.TaskId]
		//if ok && meta.state == Working {
		//	meta.state = Done
		//	fmt.Printf("Reduce task Id[%d] is finished.\n", args.TaskId)
		//} else {
		//	fmt.Printf("Reduce task Id[%d] is finished,already ! ! !\n", args.TaskId)
		//}
	default:
		panic("The task type undefined ! ! !")
	}
	return nil
}
