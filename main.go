package main

import (
	"context"
	"fmt"
)

func main() {
	ctx := context.Background()

	runtime := NewRuntime()
	runtime.scheduler.Start(ctx)

	run, err := runtime.CreateRun("Why is this project delayed?")
	if err != nil {
		panic(err)
	}

	fmt.Println("created run:", run.ID)

	if err := runtime.Run(ctx, run.ID); err != nil {
		fmt.Println("run error:", err)
	}

	finalRun, _ := runtime.store.GetRun(run.ID)

	fmt.Println()
	fmt.Println("========== RESULT ==========")
	fmt.Println("Run ID:", finalRun.ID)
	fmt.Println("Status:", finalRun.Status)
	fmt.Println("Steps:", finalRun.Steps)
	fmt.Println("Output:", finalRun.Output)

	fmt.Println()
	fmt.Println("========== EVENTS ==========")

	for _, event := range runtime.store.GetEvents(run.ID) {
		fmt.Printf(
			"%s step=%s type=%s message=%s\n",
			event.Timestamp.Format("15:04:05.000"),
			event.StepID,
			event.Type,
			event.Message,
		)
	}
}
