package mapreduce

import "fmt"

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	taskChan := make(chan int, ntasks)

	sendTask := func(worker string, ind int) bool {
		jobArgs := &DoTaskArgs{}

		jobArgs.JobName = mr.jobName
		jobArgs.File =mr.files[ind]
		jobArgs.Phase = phase
		jobArgs.TaskNumber = ind
		jobArgs.NumOtherPhase = nios
		return call(worker, "Worker.DoTask", jobArgs, new(struct{}))
	}

	for i := 0; i < ntasks; i++ {
		go func(ind int) {
			var worker string
			var ok bool

			for {
				select {
				case worker = <- mr.registerChannel:
					mr.idleChannel <- worker
				case worker = <- mr.idleChannel:
					ok = sendTask(worker, ind)
				}

				if ok {
					taskChan <- ind
					mr.idleChannel <- worker
					return
				}
			}
		}(i)
	}

	for i := 0; i < ntasks; i++ {
		<-taskChan
		fmt.Println("Finish")
	}

	fmt.Printf("Schedule: %v phase done\n", phase)
}
