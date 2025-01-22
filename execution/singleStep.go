package execution

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/getgauge/common"
	"github.com/getgauge/gauge-proto/go/gauge_messages"
	"github.com/getgauge/gauge/api"
	"github.com/getgauge/gauge/config"
	"github.com/getgauge/gauge/execution/event"
	"github.com/getgauge/gauge/execution/result"
	"github.com/getgauge/gauge/gauge"
	"github.com/getgauge/gauge/logger"
	"github.com/getgauge/gauge/manifest"
	"github.com/getgauge/gauge/parser"
	"github.com/getgauge/gauge/plugin"
	"github.com/getgauge/gauge/plugin/install"
	"github.com/getgauge/gauge/runner"
	"github.com/getgauge/gauge/skel"
	"github.com/getgauge/gauge/validation"
)

type ExecutionStatus struct {
	Args        []string `json:"Args"`
	FailedItems []string `json:"FailedItems"`
}

type StepLocation struct {
	SpecFile   string
	LineNumber int
}

type singleStepExecutor interface {
	run() *result.StepResult
}

const (
	failedFile = "failures.json"
)

func readFailedSpecLocations() ([]string, error) {
	projectRoot, _ := common.GetProjectRoot()
	failedStatusFile := filepath.Join(projectRoot, common.DotGauge, failedFile)

	content, err := os.ReadFile(failedStatusFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("failed tests file not found at %s", err)
		}
		return nil, fmt.Errorf("failed to read %s: %w", failedStatusFile, err)
	}

	var status ExecutionStatus
	if err := json.Unmarshal(content, &status); err != nil {
		return nil, fmt.Errorf("failed to parse failed status file: %w", err)
	}

	if len(status.FailedItems) == 0 {
		return nil, fmt.Errorf("no failed items found in failures.json")
	}

	return status.FailedItems, nil
}

func parseSpecLocation(location string) (string, int, error) {
	parts := strings.Split(location, ":")
	if len(parts) != 2 {
		return "", 0, fmt.Errorf("invalid format. Expected format: filepath:line")
	}

	specFile := parts[0]
	lineNum, err := strconv.Atoi(parts[1])
	if err != nil {
		return "", 0, fmt.Errorf("invalid line number: %s", parts[1])
	}

	return specFile, lineNum, nil
}

func GetFailedStepLocations() ([]StepLocation, error) {
	failedItems, err := readFailedSpecLocations()
	if err != nil {
		return nil, fmt.Errorf("failed to read failed items: %w", err)
	}

	var locations []StepLocation
	for _, item := range failedItems {
		specFile, lineNum, err := parseSpecLocation(item)
		if err != nil {
			return nil, fmt.Errorf("failed to parse location %s: %w", item, err)
		}
		locations = append(locations, StepLocation{
			SpecFile:   specFile,
			LineNumber: lineNum,
		})
	}

	return locations, nil
}

func startAPI(debug bool) runner.Runner {
	sc := api.StartAPI(debug)
	select {
	case runner := <-sc.RunnerChan:
		return runner
	case err := <-sc.ErrorChan:
		logger.Fatalf(true, "Failed to start gauge API: %s", err.Error())
	}
	return nil
}

var ExecuteStep = func(step *gauge.Step, specDir []string) int {
	if config.CheckUpdates() {
		i := &install.UpdateFacade{}
		i.BufferUpdateDetails()
		defer i.PrintUpdateBuffer()
	}

	skel.SetupPlugins(MachineReadable)
	if err := os.Setenv(gaugeParallelStreamCountEnv, strconv.Itoa(NumberOfExecutionStreams)); err != nil {
		logger.Fatalf(true, "failed to set env %s. %s", gaugeParallelStreamCountEnv, err.Error())
	}

	spec, scenario, err := findSpecAndScenario(step, specDir)
	if err != nil {
		logger.Errorf(true, "Failed to find spec and scenario: %v", err)
		return ExecutionFailed
	}

	r := startAPI(false)
	if r == nil {
		return ExecutionFailed
	}
	defer func() {
		err := r.Kill()
		if err != nil {
			logger.Errorf(false, "unable to kill runner: %s", err.Error())
		}
	}()

	executionInfo := &gauge_messages.ExecutionInfo{
		CurrentSpec: &gauge_messages.SpecInfo{
			Name:     spec.Heading.Value,
			FileName: spec.FileName,
		},
		CurrentScenario: &gauge_messages.ScenarioInfo{
			Name: scenario.Heading.Value,
		},
	}

	// Initialize plugin handler
	m, err := manifest.ProjectManifest()
	if err != nil {
		logger.Errorf(true, "Failed to get project manifest: %v", err)
		return ExecutionFailed
	}
	handler := plugin.StartPlugins(m)

	executor := &stepExecutor{
		runner:               r,
		pluginHandler:        handler,
		currentExecutionInfo: executionInfo,
		stream:               0,
	}

	protoStep := gauge.ConvertToProtoItem(step).GetStep()
	stepResult := executor.executeStep(step, protoStep)

	if stepResult.GetFailed() {
		return ExecutionFailed
	}
	return Success
}

func findSpecsContainingStep(step *gauge.Step, specDirs []string) (*gauge.SpecCollection, error) {
	conceptDict, res, err := parser.ParseConcepts()
	if err != nil {
		return nil, fmt.Errorf("failed to parse concepts: %w", err)
	}
	if !res.Ok {
		return nil, fmt.Errorf("failed to parse concepts")
	}

	errMap := gauge.NewBuildErrors()
	specs, specsFailed := parser.ParseSpecs(specDirs, conceptDict, errMap)
	if specsFailed {
		return nil, fmt.Errorf("failed to parse specs")
	}

	var matchingSpecs []*gauge.Specification
	for _, spec := range specs {
		if containsStep(spec, step) {
			matchingSpecs = append(matchingSpecs, spec)
		}
	}

	if len(matchingSpecs) == 0 {
		return nil, fmt.Errorf("no specs found containing step: %s", step.Value)
	}

	return gauge.NewSpecCollection(matchingSpecs, false), nil
}

func containsStep(spec *gauge.Specification, targetStep *gauge.Step) bool {
	for _, step := range spec.Contexts {
		if stepsEqual(step, targetStep) {
			return true
		}
	}

	for _, scenario := range spec.Scenarios {
		for _, step := range scenario.Steps {
			if stepsEqual(step, targetStep) {
				return true
			}
		}
	}

	for _, step := range spec.TearDownSteps {
		if stepsEqual(step, targetStep) {
			return true
		}
	}

	return false
}

func stepsEqual(step1, step2 *gauge.Step) bool {
	return step1.Value == step2.Value && step1.LineNo == step2.LineNo
}

func validateSpecs(specs *gauge.SpecCollection, errMap *gauge.BuildErrors, r runner.Runner) *ValidationStatus {
	conceptDict, res, err := parser.ParseConcepts()
	if err != nil {
		return &ValidationStatus{Ok: false, ParseErrors: true}
	}
	if !res.Ok {
		return &ValidationStatus{Ok: false, ParseErrors: true}
	}

	validator := validation.NewValidator(specs.Specs(), r, conceptDict)
	validationErrors := validator.Validate()
	if len(validationErrors) > 0 {
		return &ValidationStatus{Ok: false}
	}

	return &ValidationStatus{Ok: true}
}

type ValidationStatus struct {
	Ok          bool
	ParseErrors bool
}

func ExecuteSingleStep(e *stepExecutor, step *gauge.Step, protoStep *gauge_messages.ProtoStep) *result.StepResult {
	stepRequest := e.createStepRequest(protoStep)
	e.currentExecutionInfo.CurrentStep = &gauge_messages.StepInfo{Step: stepRequest, IsFailed: false}
	stepResult := result.NewStepResult(protoStep)
	for i := range step.GetFragments() {
		stepFragmet := step.GetFragments()[i]
		protoStepFragmet := protoStep.GetFragments()[i]
		if stepFragmet.FragmentType == gauge_messages.Fragment_Parameter && stepFragmet.Parameter.ParameterType == gauge_messages.Parameter_Dynamic {
			stepFragmet.GetParameter().Value = protoStepFragmet.GetParameter().Value
		}
	}
	event.Notify(event.NewExecutionEvent(event.StepStart, step, nil, e.stream, e.currentExecutionInfo))

	e.notifyBeforeStepHook(stepResult)
	if !stepResult.GetFailed() {
		executeStepMessage := &gauge_messages.Message{MessageType: gauge_messages.Message_ExecuteStep, ExecuteStepRequest: stepRequest}
		stepExecutionStatus := e.runner.ExecuteAndGetStatus(executeStepMessage)
		stepExecutionStatus.Message = append(stepResult.ProtoStepExecResult().GetExecutionResult().Message, stepExecutionStatus.Message...)
		if stepExecutionStatus.GetFailed() {
			e.currentExecutionInfo.CurrentStep.ErrorMessage = stepExecutionStatus.GetErrorMessage()
			e.currentExecutionInfo.CurrentStep.StackTrace = stepExecutionStatus.GetStackTrace()
			setStepFailure(e.currentExecutionInfo)
			stepResult.SetStepFailure()
		} else if stepResult.GetSkippedScenario() {
			e.currentExecutionInfo.CurrentStep.ErrorMessage = stepExecutionStatus.GetErrorMessage()
			e.currentExecutionInfo.CurrentStep.StackTrace = stepExecutionStatus.GetStackTrace()
		}
		stepResult.SetProtoExecResult(stepExecutionStatus)
	}
	e.notifyAfterStepHook(stepResult)

	event.Notify(event.NewExecutionEvent(event.StepEnd, *step, stepResult, e.stream, e.currentExecutionInfo))
	defer e.currentExecutionInfo.CurrentStep.Reset()
	return stepResult
}

func findSpecAndScenario(step *gauge.Step, specDirs []string) (*gauge.Specification, *gauge.Scenario, error) {
	conceptDict, res, err := parser.ParseConcepts()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse concepts: %w", err)
	}
	if !res.Ok {
		return nil, nil, fmt.Errorf("failed to parse concepts")
	}

	errMap := gauge.NewBuildErrors()
	specs, specsFailed := parser.ParseSpecs(specDirs, conceptDict, errMap)
	if specsFailed {
		return nil, nil, fmt.Errorf("failed to parse specs")
	}

	for _, spec := range specs {
		for _, scenario := range spec.Scenarios {
			if step.LineNo >= scenario.Span.Start && step.LineNo <= scenario.Span.End {
				return spec, scenario, nil
			}
		}
	}

	return nil, nil, fmt.Errorf("no scenario found containing step at line %d", step.LineNo)
}
